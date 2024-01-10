#!/bin/bash
LOCAL_SETUP_DIR="/home/entallaris/redis_bin/bin"
cd ${LOCAL_SETUP_DIR}

for i in {1..50000}
do
	   key=$(cat /dev/urandom | tr -dc '[:alpha:]' | fold -w ${1:-20} | head -n 1)
	   value=$(cat /dev/urandom | tr -dc '[:alpha:]' | fold -w ${1:-20} | head -n 1)
	   command=$(./redis-cli -c -p 8000 set ${key} ${value})
	   echo ${command}

	   sleep 5 
done
