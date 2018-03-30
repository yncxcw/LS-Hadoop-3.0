#!/bin/bash
cd /sys/fs/cgroup/memory/
#mkdir foo
#cd foo
~/tool/memory_test/cgroup_event_listener memory.pressure_level critical &
#echo 8000000000 > memory.limit_in_bytes
#echo 8000000000 > memory.memsw.limit_in_bytes
echo $$
echo $$ > tasks
##dd if=/dev/zero | read x
#~/tool/memory_test/a.out &

while true
do
    used=`sed -n '3,3p' /proc/meminfo`
    echo $used
    sleep 1
done

