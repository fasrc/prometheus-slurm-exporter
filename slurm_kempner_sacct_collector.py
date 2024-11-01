#!/usr/bin/python3

import os, sys, re, csv, time, subprocess

from typing import List, Tuple, Dict
from datetime import datetime, timedelta

from os import path

prefix = os.path.normpath(
  os.path.join(os.path.abspath(os.path.dirname(__file__)))
)
external = os.path.join(prefix, 'external')
sys.path = [prefix, external] + sys.path

from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client import start_http_server


def extract_gres_gpu(string):
    match = re.search(r'gres/gpu=(\d+)', string)
    if match:
        return int(match.group(1))
    else:
        return int(0)

def extract_gpu_factor(input_string):
    attributes = input_string.split(',')
    factor = 0.0
    for attribute in attributes:
        if attribute.startswith('gres/gpu:'):
            gpu_info = attribute.split('=')[0].replace('gres/gpu:', '')
            if 'h100' in gpu_info.lower():
                factor = wgpu['h100']
            elif 'a100' in gpu_info.lower():
                factor = wgpu['a100']
    return factor

def convert_to_hours(time_str):
    if '-' in time_str:
        days_part, time_part = time_str.split('-')
        days = int(days_part)
    else:
        time_part = time_str
        days = 0
    hours, minutes, seconds = map(int, time_part.split(':'))
    total_hours = days * 24 + hours + minutes / 60 + seconds / 3600
    return float(total_hours)

def update_dictionary(data_dict, name, t_time, g_time, g_tr_time):
    if name in data_dict:
        data_dict[name]['total_hours'] += t_time
        data_dict[name]['gpu_hours'] += g_time
        data_dict[name]['gpu_tres_hours'] += g_tr_time
    else:
        data_dict[name] = {'total_hours': t_time, 'gpu_hours': g_time, 'gpu_tres_hours': g_tr_time}

def get_node_names():
    try:
        command = "sinfo -p kempner_requeue -N 1 | grep kempner | awk '{print $1}'"
        result = subprocess.check_output(command, shell=True, universal_newlines=True)
        node_names = result.strip().split('\n')
        return node_names
    except subprocess.CalledProcessError as e:
        #print(f"Error occurred while running the command: {e}")
        return []

def check_kempner_node(n_name, n_list, p_key):
    for n in n_list:
        if n_name in n_list and "kempner" not in p_key:
            return "non-kempner"
        else:
            return p_key

def write_dict_to_file(data_dict, file_name):
    with open(file_name, 'w') as file:
        sorted_data_dict = dict(sorted(data_dict.items(), key=lambda x: x[1]['gpu_tres_hours'], reverse=True))
        for k, v in sorted_data_dict.items():
            file.write("name= {} , gpu_hours= {:0.1f}, gpu_tres_hours= {:0.1f} \n".format(k, v['gpu_hours'], v['gpu_tres_hours']))

def process_gpu_usage(input_file_name):
    partition_dict = {}
    user_dict = {}
    group_dict = {}
    node_list = get_node_names()
    global wgpu
    wgpu = {'a100': 209.1, 'h100': 546.9}

    with open(input_file_name, 'r') as file:
        for line in file:
            if "gpu" in line and "RUNNING" not in line and "PENDING" not in line:
                fields = line.strip().split('|')
                if len(fields) >= 8:
                    user_key = fields[2]
                    group_key = fields[3].split(',')[0]
                    partition_key = fields[4].split(',')[0]
                    gpu_tfield = fields[5]
                    gpu_count = extract_gres_gpu(fields[6])
                    node_name = fields[7]
                    cpu_count = fields[11]
                    if gpu_count > 0:
                        gpu_thours = convert_to_hours(gpu_tfield)
                        tres_factor = extract_gpu_factor(fields[6])
                        if tres_factor > 0:
                            gpu_hours = gpu_thours * gpu_count
                            gpu_tres_hours = gpu_hours * tres_factor
                            update_dictionary(user_dict, user_key, gpu_thours, gpu_hours, gpu_tres_hours)
                            if "kempner" in group_key:
                                update_dictionary(group_dict, group_key, gpu_thours, gpu_hours, gpu_tres_hours)
                            partition_name = check_kempner_node(node_name, node_list, partition_key)
                            if "kempner" in partition_name:
                                update_dictionary(partition_dict, partition_name, gpu_thours, gpu_hours, gpu_tres_hours)

    write_dict_to_file(user_dict, "/tmp/kempner_sacct_collect_tmp_files/user_dictionary.csv")
    write_dict_to_file(group_dict, "/tmp/kempner_sacct_collect_tmp_files/group_dictionary.csv")
    write_dict_to_file(partition_dict, "/tmp/kempner_sacct_collect_tmp_files/partition_dictionary.csv")

def parse_line(line: str) -> dict:
    pattern = r"name=\s*(\S+)\s*,\s*gpu_hours=\s*([\d.]+)\s*,\s*gpu_tres_hours=\s*([\d.]+)"
    match = re.match(pattern, line)
    if match:
        name_id = match.group(1)
        gpu_hours = float(match.group(2))
        gpu_tres_hours = float(match.group(3))
        return {
            'name_id': name_id,
            'gpu_hours': gpu_hours,
            'gpu_tres_hours': gpu_tres_hours
        }
    else:
        raise ValueError(f"Line format is incorrect: {line}")

def read_custom_csv(file_name: str) -> dict:
    data = {}
    with open(file_name, mode='r') as file:
        for line in file:
            entry = parse_line(line.strip())
            name_id = entry['name_id']
            data[name_id] = {
                'gpu_hours': entry['gpu_hours'],
                'gpu_tres_hours': entry['gpu_tres_hours']
            }
    return data

def merge_dictionaries(dict1: dict, dict2: dict) -> dict:
    merged_data = dict1.copy()
    for name_id, values in dict2.items():
        if name_id in merged_data:
            merged_data[name_id]['gpu_hours'] += values['gpu_hours']
            merged_data[name_id]['gpu_tres_hours'] += values['gpu_tres_hours']
        else:
            merged_data[name_id] = values
    return merged_data

def process_each_pair(file1, file2):
    data1 = read_custom_csv(file1)
    data2 = read_custom_csv(file2)
    merged_data = merge_dictionaries(data1, data2)
    return merged_data

def write_dict_to_file(data_dict, file_name):
    with open(file_name, 'w') as file:
        sorted_data_dict = dict(sorted(data_dict.items(), key=lambda x: x[1]['gpu_tres_hours'], reverse=True))
        for k, v in sorted_data_dict.items():
            file.write("name= {} , gpu_hours= {:0.1f}, gpu_tres_hours= {:0.1f} \n".format(k, v['gpu_hours'], v['gpu_tres_hours']))

def merge_files(file_pairs):
    for file1, file2 in file_pairs:
        result_dict = process_each_pair(file1, file2)
        write_dict_to_file(result_dict, file1)

def run_command(s_date, e_date):
    """
    Executes the sacct command with specified start and end dates.
    
    :param s_date: Start date for the command.
    :param e_date: End date for the command.
    """
    command = [
        "sacct",
        "-S", s_date,
        "-E", e_date,
        "--allusers",
        "-X",
        "-D",
        "-p",
        "--format=JobID,State,user%-24,Account%-24,partition%-24,Elapsed,AllocTRES%-160,NodeList%-160,ReqMem,MaxRSS,ExitCode,NCPUs,TotalCPU,CPUTime,ReqTRES,start,end%-120"
    ]
    # Run the command and redirect output to today_sacct.data
    with open("/tmp/kempner_sacct_collect_tmp_files/today_sacct.data", "w") as output_file:
        subprocess.run(command, stdout=output_file, universal_newlines=True)


def find_missing_dates(file_path):
    # Define the date format used in the file
    date_format = "%Y-%m-%d"

    # Read all dates from input.csv and store them in a set for quick lookup
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Parse dates from each line and find the oldest entry date
    entry_dates = set()
    for line in lines:
        parts = line.strip().split(',')
        entry_date = datetime.strptime(parts[0], date_format).date()
        entry_dates.add(entry_date)

    # Find the oldest entry date in the file
    oldest_date = min(entry_dates)

    # Calculate the target end date as (today - 7 days)
    today = datetime.now().date()
    end_date = today - timedelta(days=1)

    # Generate all dates from oldest_date to end_date and find missing dates
    
    missing_dates = [] 
    current_date = oldest_date
    while current_date <= end_date:
        if current_date not in entry_dates:
            previous_date = current_date - timedelta(days=1)
            missing_dates.append((previous_date, current_date))
        current_date += timedelta(days=1)
    return missing_dates

def getdata_current_or_missing_dates():
    time_stamp_file = "/tmp/kempner_sacct_collect_tmp_files/sacct_collect_timestamp.data"
    missing_dates = find_missing_dates(time_stamp_file)

    with open(time_stamp_file, 'a') as file:
        for start_date, end_date in missing_dates:
            s_date = str(start_date)
            e_date = str(end_date)
            run_command(s_date, e_date)
            process_gpu_usage("/tmp/kempner_sacct_collect_tmp_files/today_sacct.data")
            file_pairs = [
                ('/tmp/kempner_sacct_collect_tmp_files/partition_dictionary_sum.csv', '/tmp/kempner_sacct_collect_tmp_files/partition_dictionary.csv'),
                ('/tmp/kempner_sacct_collect_tmp_files/group_dictionary_sum.csv', '/tmp/kempner_sacct_collect_tmp_files/group_dictionary.csv'),
                ('/tmp/kempner_sacct_collect_tmp_files/user_dictionary_sum.csv', '/tmp/kempner_sacct_collect_tmp_files/user_dictionary.csv')
            ]
            merge_files(file_pairs)
            #file.write(f"{s_date},{e_date}\n")
            file.write(f"{e_date}\n")

def read_file_to_dict(file_path: str, include_index=False, start_index=1) -> Tuple[Dict[str, Dict[str, float]], int]:
    """
    Reads data from a single file and stores it in a dictionary.

    :param file_path: Path to the CSV file.
    :param include_index: Whether to include an incrementing index (e.g., A1, A2, ...) in the dictionary.
    :param start_index: The starting index for the 'A' labels.
    :return: A tuple of (dictionary with data, next available index).
    """
    data = {}
    current_index = start_index

    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) >= 3:  # Ensure there are enough columns
                name_id = row[0].split('=')[1].strip()
                gpu_hours = float(row[1].split('=')[1].strip())
                gpu_tres_hours = float(row[2].split('=')[1].strip())
                
                if include_index:
                    # Create an index label like A1, A2, ...
                    index_label = f"A{current_index}"
                    data[name_id] = {'index': index_label, 'gpu_hours': gpu_hours, 'gpu_tres_hours': gpu_tres_hours}
                    current_index += 1
                else:
                    # Store without index
                    data[name_id] = {'gpu_hours': gpu_hours, 'gpu_tres_hours': gpu_tres_hours}
                    
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

# Example usage
file_pairs = [
    ('/tmp/kempner_sacct_collect_tmp_files/partition_dictionary_sum.csv', '/tmp/kempner_sacct_collect_tmp_files/partition_dictionary.csv'),
    ('/tmp/kempner_sacct_collect_tmp_files/group_dictionary_sum.csv', '/tmp/kempner_sacct_collect_tmp_files/group_dictionary.csv'),
    ('/tmp/kempner_sacct_collect_tmp_files/user_dictionary_sum.csv', '/tmp/kempner_sacct_collect_tmp_files/user_dictionary.csv')
]

# Call the function and store the results in separate dictionaries
partition_dict, partition_dict_sum, group_dict, group_dict_sum, user_dict, user_dict_sum = read_file_pairs(file_pairs)

class SlurmKempnerSacctsCollector:

    def collect(self):
        # Create GaugeMetricFamily for gpu_hours and gpu_tres_hours with name_id and index labels
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

        tot_gpu_hours_part_metric = GaugeMetricFamily(
            'tot_gpu_part_hours',
            'Total GPU hours for partition',
            labels=['name_id', 'index']
        )
        tot_gpu_tres_hours_part_metric = GaugeMetricFamily(
            'tot_gpu_tres_part_hours',
            'Cumulative Total GPU hours for partition',
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
            day_gpu_hours_part_metric.add_metric([name_id, index], metrics['gpu_hours'])
            day_gpu_tres_hours_part_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])
        for name_id, metrics in group_dict.items():
            index = metrics['index']
            day_gpu_hours_group_metric.add_metric([name_id, index], metrics['gpu_hours'])
            day_gpu_tres_hours_group_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])
        for name_id, metrics in user_dict.items():
            index = metrics['index']
            day_gpu_hours_user_metric.add_metric([name_id, index], metrics['gpu_hours'])
            day_gpu_tres_hours_user_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])


        # Add metrics from partition_dict_sum, group_dict_sum, user_dict_sum
        for name_id, metrics in partition_dict_sum.items():
            index = metrics['index']
            tot_gpu_hours_part_metric.add_metric([name_id, index], metrics['gpu_hours'])
            tot_gpu_tres_hours_part_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])
        for name_id, metrics in group_dict_sum.items():
            index = metrics['index']
            tot_gpu_hours_group_metric.add_metric([name_id, index], metrics['gpu_hours'])
            tot_gpu_tres_hours_group_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])
        for name_id, metrics in user_dict_sum.items():
            index = metrics['index']
            tot_gpu_hours_user_metric.add_metric([name_id, index], metrics['gpu_hours'])
            tot_gpu_tres_hours_user_metric.add_metric([name_id, index], metrics['gpu_tres_hours'])

        # Yield metrics to Prometheus
        yield day_gpu_hours_part_metric
        yield day_gpu_tres_hours_part_metric
        yield day_gpu_hours_group_metric
        yield day_gpu_tres_hours_group_metric
        yield day_gpu_hours_user_metric
        yield day_gpu_tres_hours_user_metric
        yield tot_gpu_hours_part_metric
        yield tot_gpu_tres_hours_part_metric
        yield tot_gpu_hours_group_metric
        yield tot_gpu_tres_hours_group_metric
        yield tot_gpu_hours_user_metric
        yield tot_gpu_tres_hours_user_metric

if __name__ == "__main__":
    getdata_current_or_missing_dates()
    start_http_server(9007)
    REGISTRY.register(SlurmKempnerSacctsCollector())
    while True:
# We need to run this script once in a day
        time.sleep(86400)
