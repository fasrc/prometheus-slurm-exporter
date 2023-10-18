#!/bin/bash

PIP_INSTALL=$(pwd)/external

python3 -m ensurepip --root $PIP_INSTALL --default-pip

export PATH=$PIP_INSTALL/usr/bin:$PATH

export PYTHONPATH=$PIP_INSTALL/external:$PIP_INSTALL/usr/lib/python$pyver/site-packages:$PYTHONPATH

python3 -m pip install --no-cache-dir -q --upgrade pip --target=$PIP_INSTALL

python3 -m pip install --no-cache-dir -q -r requirements.txt --target=$PIP_INSTALL --upgrade
