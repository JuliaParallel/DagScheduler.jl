rm -rf /dev/shm/sem.*
rm -rf /dev/shm/executor*
rm -rf /dev/shm/broker
rm -rf /dev/shm/dagscheduler
rm -rf /dev/shm/jsch*
rm -rf .mempool
for id in `ipcs -s | grep tan | cut -d" " -f2`; do echo $x; ipcrm -s $id; done
for x in `ipcs -m | grep $USER | grep -v dest | grep -e 819200 -e 32 -e 3764414 -e 120327162 -e 19215726 | cut -d" " -f2`; do echo $x; ipcrm -m $x; done
