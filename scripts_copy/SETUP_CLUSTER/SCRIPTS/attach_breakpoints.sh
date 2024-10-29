#!/bin/bash

# Find the PID of the running redis-server process
#REDIS_PID=$(ps aux | grep '[r]edis-server' | awk '{print $2}' | head -n 1)
REDIS_PID=$(sudo ps aux | grep "redis-server 0.0.0.0" | awk '{print $2}' | head -n 1)

# Check if redis-server is running
if [ -z "$REDIS_PID" ]; then
    echo "redis-server process not found."
    exit 1
fi

#echo "Attaching to redis-server process with PID: $REDIS_PID"
#
## Start gdb, attach to the process, and set breakpoints
#sudo gdb -p "$REDIS_PID" --quiet -ex "
#    break cluster.c:573
#    break t_string.c:115
#    echo 'Breakpoint set at cluster.c:573\n'
#    echo 'Breakpoint set at t_string.c:115\n'
#    continue
#"
#
#echo "Attached to redis-server with breakpoints, enter interactive mode."
#sudo gdb -p "$REDIS_PID"
#

# Check if redis-server is running
if [ -z "$REDIS_PID" ]; then
    echo "redis-server process not found."
    exit 1
fi

echo "Attaching to redis-server process with PID: $REDIS_PID"

# Start gdb, attach to the process, and set breakpoints
sudo gdb -p "$REDIS_PID" --batch --quiet -ex "
    break cluster.c:573
    break t_string.c.c:115
    echo 'Breakpoint set at cluster.c:573\n'
    echo 'Breakpoint set at t_string.c:115\n'
    continue
" --ex "shell echo 'Attached to redis-server with breakpoints, enter interactive mode.'" \
   --ex "interact"

echo "sudo gdb -p $REDIS_PID"
