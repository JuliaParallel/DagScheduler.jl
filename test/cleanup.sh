rm -rf /dev/shm/sem.*
rm -rf /dev/shm/executor*
rm -rf /dev/shm/broker
rm -rf /dev/shm/scheduler
rm -rf /dev/shm/jsch*
rm -rf .mempool
for x in `ipcs -m | grep $USER | grep -v dest | cut -d" " -f2`; do echo $x; ipcrm -m $x; done
