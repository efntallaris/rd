#!/bin/bash -e

REDIS_LOCAL_BIN_DIR=$1
CONFIG_FILE=$2


if [[ -e ${REDIS_LOCAL_BIN_DIR} ]] ; then
	sudo rm -rf ${REDIS_LOCAL_BIN_DIR}
fi
if [[ -e ${CONFIG_FILE} ]] ; then
	sudo rm -rf ${CONFIG_FILE}
fi
