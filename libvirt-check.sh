
# recover vm to original clean point
sudo virsh snapshot-revert --domain $1 --snapshotname $1-orig

# make sure vm isn't running
sudo virsh domstate $1|grep running && sudo virsh destroy $1

# start vm
sudo virsh start $1
sleep 5

STATE=$(sudo virsh list --all | egrep "$1\s" | awk '{ print $3 }')
if [ $STATE == 'running' ]
then
    echo 'All fine'
    exit 0
else
    echo "Domain is still in state $STATE - Aborting"
    exit 1
fi
