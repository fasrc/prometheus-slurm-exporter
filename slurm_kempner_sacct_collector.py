#!/usr/bin/python3

import os
import sys
import re
import csv
import time
import subprocess
from typing import List, Tuple, Dict
from datetime import datetime, timedelta
from os import path
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client import start_http_server

# Constants
PREFIX = os.path.normpath(os.path.join(os.path.abspath(os.path.dirname(__file__))))
EXTERNAL = os.path.join(PREFIX, 'external')
sys.path = [PREFIX, EXTERNAL] + sys.path
WGPU = {'a100': 209.1, 'h100': 546.9}

# Utility Functions
def extract_gres_gpu(string: str) -> int:
    match = re.search(r'gres/gpu=(\d+)', string)
    return int(match.group(1)) if match else 0

def extract_gpu_factor(input_string: str) -> float:
    attributes = input_string.split(',')
    for attribute in attributes:
        if attribute.startswith('gres/gpu:'):
            gpu_info = attribute.split('=')[0].replace('gres/gpu:', '')
            if 'h100' in gpu_info.lower():
                return WGPU['h100']
            elif 'a100' in gpu_info.lower():
                return WGPU['a100']
    return 0.0


def convert_to_hours(time_str):
    # Split the time string into components based on the presence of '-'
    if '-' in time_str:
        days_part, time_part = time_str.split('-')
        days = int(days_part)  # Convert days to integer
    else:
        time_part = time_str
        days = 0  # No days present in the format

    # Split the time part (hours:minutes:seconds)
    hours, minutes, seconds = map(int, time_part.split(':'))

    # Calculate total hours
    total_hours = days * 24 + hours + minutes / 60 + seconds / 3600

    return float(total_hours)



def update_dictionary(data_dict: dict, name: str, t_time: float, g_time: float, g_tr_time: float, c_time: float):
    if name in data_dict:
        data_dict[name]['total_hours'] += t_time
        data_dict[name]['gpu_hours'] += g_time
        data_dict[name]['gpu_tres_hours'] += g_tr_time
        data_dict[name]['cpu_hours'] += c_time
    else:
        data_dict[name] = {'total_hours': t_time, 'gpu_hours': g_time, 'gpu_tres_hours': g_tr_time, 'cpu_hours': c_time}

def get_node_list() -> str:
    try:
        command = 'sinfo -p kempner_dev -h -o "%N" | paste -sd ","'
        result = subprocess.check_output(command, shell=True, universal_newlines=True)
        return result.strip()
    except subprocess.CalledProcessError:
        return ""

def get_node_names():
    try:
        command = "sinfo -p kempner_requeue -N 1 | grep kempner | awk '{print $1}'"
        result = subprocess.check_output(command, shell=True, universal_newlines=True)
        node_names = result.strip().split('\n')
        return node_names
    except subprocess.CalledProcessError as e:
        return []


def check_kempner_node(n_name, n_list, p_key, w_factor):
    """
    Check if any node name matches a line and if the line does not contain 'kempner'.
    """
    for n in n_list:
        if n_name in n_list and "kempner" not in p_key:
            if w_factor == wgpu['h100']:
                return "fasrc_h100"
            if w_factor == wgpu['a100']:
                return "fasrc_a100"
            if w_factor == 0 :
                return "fasrc_cpu"
        else:
            return p_key


def process_cpu_gpu_usage(input_file_name):
    partition_dict = {}
    user_dict = {}
    group_dict = {}
    node_list = get_node_list()
    global wgpu
    wgpu = {'a100': 209.1, 'h100': 546.9}
    with open(input_file_name, 'r') as file:
        for line in file:
            # Filter lines containing the finished jobs
            #if "gpu" in line and "RUNNING" not in line and "PENDING" not in line:
            if ("gpu" in line and "RUNNING" not in line) or ("gpu" in line and "PENDING" not in line):
                # Split the line using the '|' separator
                fields = line.strip().split('|')
                gpu_count = 0
                gpu_tres_hours = 0
                # Ensure there are enough fields to avoid index errors
                if len(fields) >= 8:
                    user_key = fields[2]
                    group_key = fields[3].split(',')[0]
                    partition_key = fields[4].split(',')[0]
                    gpu_tfield = fields[5]
                    gpu_thours = convert_to_hours(gpu_tfield)
                    gpu_count = extract_gres_gpu(fields[6])

                    #cpu_count = extract_cpu_count(fields[6])
                    node_name = fields[7]
                    cpu_count = int(fields[11])
                    cpu_hours = gpu_thours*cpu_count
                    gpu_hours = gpu_count*gpu_thours
                    tres_factor = extract_gpu_factor(fields[6])
                    if tres_factor > 0:
                        gpu_tres_hours = gpu_hours*tres_factor
                    update_dictionary(user_dict, user_key, gpu_thours, cpu_hours, gpu_hours, gpu_tres_hours)

                    partition_name = check_kempner_node(node_name, node_list, partition_key, tres_factor)
                    update_dictionary(partition_dict, partition_name, gpu_thours, cpu_hours, gpu_hours, gpu_tres_hours)
                    update_dictionary(user_dict, user_key, gpu_thours, cpu_hours, gpu_hours, gpu_tres_hours)
                    if "kempner" in group_key:
                        update_dictionary(group_dict, group_key, gpu_thours, cpu_hours, gpu_hours, gpu_tres_hours)




def write_dict_to_file(data_dict: dict, file_name: str):
    with open(file_name, 'w') as file:
        sorted_data_dict = dict(sorted(data_dict.items(), key=lambda x: x[1]['gpu_hours'], reverse=True))
        for k, v in sorted_data_dict.items():
            file.write(f"name= {k} , cpu_hours= {v['cpu_hours']:.1f}, gpu_hours= {v['gpu_hours']:.1f}, gpu_tres_hours= {v['gpu_tres_hours']:.1f} \n")

def parse_line(line: str) -> dict:
    pattern = r"name=\s*(\S+)\s*,\s*cpu_hours=\s*([\d.]+)\s*,\s*gpu_hours=\s*([\d.]+)\s*,\s*gpu_tres_hours=\s*([\d.]+)"
    match = re.match(pattern, line)
    if match:
        return {
            'name_id': match.group(1),
            'cpu_hours': float(match.group(2)),
            'gpu_hours': float(match.group(3)),
            'gpu_tres_hours': float(match.group(4))
        }
    else:
        raise ValueError(f"Line format is incorrect: {line}")

def read_custom_csv(file_name: str) -> dict:
    data = {}
    with open(file_name, mode='r') as file:
        for line in file:
            entry = parse_line(line.strip())
            data[entry['name_id']] = {
                'cpu_hours': entry['cpu_hours'],
                'gpu_hours': entry['gpu_hours'],
                'gpu_tres_hours': entry['gpu_tres_hours']
            }
    return data

def merge_dictionaries(dict1: dict, dict2: dict) -> dict:
    merged_data = dict1.copy()
    for name_id, values in dict2.items():
        if name_id in merged_data:
            merged_data[name_id]['cpu_hours'] += values['cpu_hours']
            merged_data[name_id]['gpu_hours'] += values['gpu_hours']
            merged_data[name_id]['gpu_tres_hours'] += values['gpu_tres_hours']
        else:
            merged_data[name_id] = values
    return merged_data

def process_each_pair(file1: str, file2: str) -> dict:
    data1 = read_custom_csv(file1)
    data2 = read_custom_csv(file2)
    return merge_dictionaries(data1, data2)

def merge_files(file_pairs: List[Tuple[str, str]]):
    for file1, file2 in file_pairs:
        result_dict = process_each_pair(file1, file2)
        write_dict_to_file(result_dict, file1)

def run_command(s_date: str, e_date: str, output_file_path: str):
    node_list = get_node_list()
    command = [
        "sacct",
        "-N", node_list,
        "-S", s_date,
        "-E", e_date,
        "--allusers",
        "-X",
        "-p",
        "--format=JobID,State,user%-24,Account%-24,partition%-24,Elapsed,AllocTRES%-160,NodeList%-160,ReqMem,MaxRSS,ExitCode,NCPUs,TotalCPU,CPUTime,ReqTRES,start,end%-120"
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, universal_newlines=True)
    
    seen_lines = set()
    filtered_lines = []

    for line in result.stdout.splitlines():
        if line not in seen_lines:
            seen_lines.add(line)
            fields = line.split('|')
            if len(fields) > 1:
                last_field = fields[-2]
                if last_field.split('T')[0] == e_date:
                    filtered_lines.append(line)

    with open(output_file_path, "w") as output_file:
        for line in filtered_lines:
            output_file.write(line + '\n')

def find_missing_dates(file_path: str) -> List[Tuple[datetime, datetime]]:
    date_format = "%Y-%m-%d"
    with open(file_path, 'r') as file:
        lines = file.readlines()
    entry_dates = {datetime.strptime(line.strip().split(',')[0], date_format).date() for line in lines}
    oldest_date = min(entry_dates)
    today = datetime.now().date()
    end_date = today - timedelta(days=1)
    missing_dates = []
    current_date = oldest_date
    while current_date <= end_date:
        if current_date not in entry_dates:
            previous_date = current_date - timedelta(days=1)
            missing_dates.append((previous_date, current_date))
        current_date += timedelta(days=1)
    return missing_dates

def getdata_current_or_missing_dates(time_stamp_entry_file):
    missing_dates = find_missing_dates(time_stamp_entry_file)
    if (len(missing_dates)>0):
        with open(time_stamp_file, 'a') as file:
            for p_end_date, end_date in missing_dates:
                e_date = str(end_date)
                s_date = str((datetime.strptime(e_date, '%Y-%m-%d') - timedelta(days=10)).date())
                today_sacct_data_file_path = "/tmp/kempner_sacct_collect_tmp_files/today_sacct.data"
                run_command(s_date, e_date, today_sacct_data_file_path)
                process_cpu_gpu_usage(today_sacct_data_file_path)
                merge_files(file_pairs)
                file.write(f"{e_date}\n")
        return "non-empty"
                
    else: 
        return "empty"

def read_file_to_dict(file_path: str, include_index=False, start_index=1) -> Tuple[Dict[str, Dict[str, float]], int]:
    data = {}
    current_index = start_index
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) >= 4:
                name_id = row[0].split('=')[1].strip()
                cpu_hours = float(row[1].split('=')[1].strip())
                gpu_hours = float(row[2].split('=')[1].strip())
                gpu_tres_hours = float(row[3].split('=')[1].strip())
                if include_index:
                    index_label = f"A{current_index}"
                    data[name_id] = {'index': index_label, 'cpu_hours': cpu_hours, 'gpu_hours': gpu_hours, 'gpu_tres_hours': gpu_tres_hours}
                    current_index += 1
                else:
                    data[name_id] = {'cpu_hours': cpu_hours, 'gpu_hours': gpu_hours, 'gpu_tres_hours': gpu_tres_hours}
    return data, current_index

def read_file_pairs(file_pairs: List[Tuple[str, str]]):
    """
    Reads file pairs one at a time and stores data in six dictionaries.

    :param file_pairs: List of tuples containing file paths for each category (partition, group, user).
    :return: Six dictionaries for each data type and sum data.
    """
    # Initialize separate dictionaries for each type and sum type
    partition_dict = {}
    partition_dict_sum = {}
    group_dict = {}
    group_dict_sum = {}
    user_dict = {}
    user_dict_sum = {}

    # Initialize starting index for 'A' labels for each dictionary
    partition_index = 1
    partition_sum_index = 1
    group_index = 1
    group_sum_index = 1
    user_index = 1
    user_sum_index = 1

    for file_sum, file_regular in file_pairs:
        # Determine which dictionary to update based on the file path
        if 'partition' in file_sum:
            # Update partition sum and regular dictionaries with unique indices
            partition_dict_sum_data, partition_sum_index = read_file_to_dict(file_sum, include_index=True, start_index=partition_sum_index)
            partition_dict_data, partition_index = read_file_to_dict(file_regular, include_index=True, start_index=partition_index)
            partition_dict_sum.update(partition_dict_sum_data)
            partition_dict.update(partition_dict_data)
        elif 'group' in file_sum:
            # Update group sum and regular dictionaries with unique indices
            group_dict_sum_data, group_sum_index = read_file_to_dict(file_sum, include_index=True, start_index=group_sum_index)
            group_dict_data, group_index = read_file_to_dict(file_regular, include_index=True, start_index=group_index)
            group_dict_sum.update(group_dict_sum_data)
            group_dict.update(group_dict_data)
        elif 'user' in file_sum:
            # Update user sum and regular dictionaries with unique indices
            user_dict_sum_data, user_sum_index = read_file_to_dict(file_sum, include_index=True, start_index=user_sum_index)
            user_dict_data, user_index = read_file_to_dict(file_regular, include_index=True, start_index=user_index)
            user_dict_sum.update(user_dict_sum_data)
            user_dict.update(user_dict_data)

    return partition_dict, partition_dict_sum, group_dict, group_dict_sum, user_dict, user_dict_sum



class SlurmKempnerSacctsCollector:

    def collect(self):
        # Create GaugeMetricFamily for cpu_hours, gpu_hours, and gpu_tres_hours with name_id and index labels
        day_cpu_hours_part_metric = GaugeMetricFamily(
            'day_cpu_part_hours',
            'Total CPU hours for partition',
            labels=['name_id', 'index']
        )
        day_gpu_hours_part_metric = GaugeMetricFamily(
            'day_gpu_part_hours',
            'Total GPU hours for partition',
            labels=['name_id', 'index']
        )
        day_gpu_tres_hours_part_metric = GaugeMetricFamily(
            'day_gpu_tres_part_hours',
            'Total GPU hours for partition',
            labels=['name_id', 'index']
        )

        day_cpu_hours_group_metric = GaugeMetricFamily(
            'day_cpu_group_hours',
            'Total CPU hours for group',
            labels=['name_id', 'index']
        )
        day_gpu_hours_group_metric = GaugeMetricFamily(
            'day_gpu_group_hours',
            'Total GPU hours for group',
            labels=['name_id', 'index']
        )
        day_gpu_tres_hours_group_metric = GaugeMetricFamily(
            'day_gpu_tres_group_hours',
            'Total GPU hours for group',
            labels=['name_id', 'index']
        )

        day_cpu_hours_user_metric = GaugeMetricFamily(
            'day_cpu_user_hours',
            'Total CPU hours for user',
            labels=['name_id', 'index']
        )
        day_gpu_hours_user_metric = GaugeMetricFamily(
            'day_gpu_user_hours',
            'Total GPU hours for user',
            labels=['name_id', 'index']
        )
        day_gpu_tres_hours_user_metric = GaugeMetricFamily(
            'day_gpu_tres_user_hours',
            'Total GPU hours for user',
            labels=['name_id', 'index']
        )

        tot_cpu_hours_part_metric = GaugeMetricFamily(
            'tot_cpu_part_hours',
            'Cumulative Total CPU hours for partition',
            labels=['name_id', 'index']
        )
        tot_gpu_hours_part_metric = GaugeMetricFamily(
            'tot_gpu_part_hours',
            'Cumulative Total GPU hours for partition',
            labels=['name_id', 'index']
        )
        tot_gpu_tres_hours_part_metric = GaugeMetricFamily(
            'tot_gpu_tres_part_hours',
            'Cumulative Total GPU hours for partition',
            labels=['name_id', 'index']
        )

        tot_cpu_hours_group_metric = GaugeMetricFamily(
            'tot_cpu_group_hours',
            'Cumulative Total CPU hours for group',
            labels=['name_id', 'index']
        )
        tot_gpu_hours_group_metric = GaugeMetricFamily(
            'tot_gpu_group_hours',
            'Cumulative Total GPU hours for group',
            labels=['name_id', 'index']
        )
        tot_gpu_tres_hours_group_metric = GaugeMetricFamily(
            'tot_gpu_tres_group_hours',
            'Cumulative Total GPU hours for group',
            labels=['name_id', 'index']
        )

        tot_cpu_hours_user_metric = GaugeMetricFamily(
            'tot_cpu_user_hours',
            'Cumulative Total CPU hours for user',
            labels=['name_id', 'index']
        )
        tot_gpu_hours_user_metric = GaugeMetricFamily(
            'tot_gpu_user_hours',
            'Cumulative Total GPU hours for user',
            labels=['name_id', 'index']
        )
        tot_gpu_tres_hours_user_metric = GaugeMetricFamily(
            'tot_gpu_tres_user_hours',
            'Cumulative Total GPU hours for user',
            labels=['name_id', 'index']
        )

        # Add metrics from partition_dict, group_dict, user_dict
        for name_id, metrics in partition_dict.items():
            index = metrics['index']
            day_cpu_hours_part_metric.add_metric([name_id, index], metrics['cpu_hours'])
            day_gpu_hours_part_metric.add_metric([name_id, index], metrics['gpu_hours'])
            day_gpu_tres_hours_part_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])
        for name_id, metrics in group_dict.items():
            index = metrics['index']
            day_cpu_hours_group_metric.add_metric([name_id, index], metrics['cpu_hours'])
            day_gpu_hours_group_metric.add_metric([name_id, index], metrics['gpu_hours'])
            day_gpu_tres_hours_group_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])
        for name_id, metrics in user_dict.items():
            index = metrics['index']
            day_cpu_hours_user_metric.add_metric([name_id, index], metrics['cpu_hours'])
            day_gpu_hours_user_metric.add_metric([name_id, index], metrics['gpu_hours'])
            day_gpu_tres_hours_user_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])

        # Add metrics from partition_dict_sum, group_dict_sum, user_dict_sum
        for name_id, metrics in partition_dict_sum.items():
            index = metrics['index']
            tot_cpu_hours_part_metric.add_metric([name_id, index], metrics['cpu_hours'])
            tot_gpu_hours_part_metric.add_metric([name_id, index], metrics['gpu_hours'])
            tot_gpu_tres_hours_part_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])
        for name_id, metrics in group_dict_sum.items():
            index = metrics['index']
            tot_cpu_hours_group_metric.add_metric([name_id, index], metrics['cpu_hours'])
            tot_gpu_hours_group_metric.add_metric([name_id, index], metrics['gpu_hours'])
            tot_gpu_tres_hours_group_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])
        for name_id, metrics in user_dict_sum.items():
            index = metrics['index']
            tot_cpu_hours_user_metric.add_metric([name_id, index], metrics['cpu_hours'])
            tot_gpu_hours_user_metric.add_metric([name_id, index], metrics['gpu_hours'])
            tot_gpu_tres_hours_user_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])

        # Yield metrics to Prometheus
        yield day_cpu_hours_part_metric
        yield day_gpu_hours_part_metric
        yield day_gpu_tres_hours_part_metric
        yield day_cpu_hours_group_metric
        yield day_gpu_hours_group_metric
        yield day_gpu_tres_hours_group_metric
        yield day_cpu_hours_user_metric
        yield day_gpu_hours_user_metric
        yield day_gpu_tres_hours_user_metric
        yield tot_cpu_hours_part_metric
        yield tot_gpu_hours_part_metric
        yield tot_gpu_tres_hours_part_metric
        yield tot_cpu_hours_group_metric
        yield tot_gpu_hours_group_metric
        yield tot_gpu_tres_hours_group_metric
        yield tot_cpu_hours_user_metric
        yield tot_gpu_hours_user_metric
        yield tot_gpu_tres_hours_user_metric

file_pairs = [
    ('/tmp/kempner_sacct_collect_tmp_files/partition_dictionary_sum.csv', '/tmp/kempner_sacct_collect_tmp_files/partition_dictionary.csv'),
    ('/tmp/kempner_sacct_collect_tmp_files/group_dictionary_sum.csv', '/tmp/kempner_sacct_collect_tmp_files/group_dictionary.csv'),
    ('/tmp/kempner_sacct_collect_tmp_files/user_dictionary_sum.csv', '/tmp/kempner_sacct_collect_tmp_files/user_dictionary.csv')
]
time_stamp_file = "/tmp/kempner_sacct_collect_tmp_files/sacct_collect_timestamp.data"

if __name__ == "__main__":
    update_status = getdata_current_or_missing_dates(time_stamp_file)
    if ("non-empty" in update_status):
        partition_dict, partition_dict_sum, group_dict, group_dict_sum, user_dict, user_dict_sum = read_file_pairs(file_pairs)
        start_http_server(9007)
        REGISTRY.register(SlurmKempnerSacctsCollector())
    while True:
        time.sleep(86400)


