#!/bin/bash

options=("-v")
options+=($1)

echo "Launch pytest ${options[@]}"
#echo "Set cloud to Google"
cms set cloud=google

pytest ${options[@]} ../cloudmesh-cloud/tests/cloud/test_00_sys.py
pytest ${options[@]} ../cloudmesh-cloud/tests/cloud/test_01_clean_local_remote.py
pytest ${options[@]} ../cloudmesh-cloud/tests/cloud/test_02_key.py
pytest ${options[@]} ../cloudmesh-cloud/tests/cloud/test_04_flavor.py
pytest ${options[@]} ../cloudmesh-cloud/tests/cloud/test_05_image.py
pytest ${options[@]} ../cloudmesh-cloud/tests/cloud/test_07_secgroup_provider.py
pytest ${options[@]} ../cloudmesh-cloud/tests/cloud/test_08_vm_provider.py
pytest ${options[@]} ../cloudmesh-cloud/tests/cloud/test_09_cm_names_find.py
