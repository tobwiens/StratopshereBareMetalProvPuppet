#!/bin/bash -x
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
apt-get update
echo "::MASTER.IP:: puppet" >> /etc/hosts
apt-get install puppet -y
sleep 60 #wait a minute. The master has to be fully online
puppet agent --enable
puppet agent --verbose
puppet agent --test
