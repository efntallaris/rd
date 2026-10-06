#!/bin/bash
# Sync redis/src from the controller to the other redis hosts, rebuild everywhere, verify.
set -uo pipefail
R=/users/entall/rd
for h in redis1 redis2 redis3 redis4 redis5; do
  sudo rsync -a --exclude '*.o' --exclude '*.d' --exclude redis-server --exclude redis-cli --exclude '*.a' --exclude '*.bak*' \
    $R/redis/src/ root@$h:$R/redis/src/ || echo "rsync $h FAILED"
done
cd $R/ansible
sudo ansible-playbook -i inventory_scale3to6.ini tasks/teardown/kill_processes.yml > /dev/null 2>&1
sudo ansible-playbook -i inventory_scale3to6.ini tasks/build/build_redis_custom.yml -e redis_variant=custom 2>&1 | grep -aE 'error:|fatal|FAILED|ok=' | cut -c1-200
for h in redis0 redis1 redis2 redis3 redis4 redis5; do echo "$h $(sudo ssh $h 'md5sum /users/entall/rd/redis_bin/bin/redis-server | cut -c1-12')"; done
