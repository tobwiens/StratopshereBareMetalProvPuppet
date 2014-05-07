#!/bin/bash -x
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
#variables
sitePPPath=/etc/puppet/manifests/site.pp
sshCertPath=/etc/puppet/modules/hadoop/files/ssh/
apt-get update

#update puppet in /etc/hosts
wget -qO- http://instance-data/latest/meta-data/local-ipv4 >> /etc/hosts
echo " puppet" >> etc/hosts

#install puppet
apt-get install puppetmaster -y
echo "*" >> /etc/puppet/autosign.conf
puppet module install tobw-hadoop
echo -e "node default { \n include java \n include hadoop::cluster::slave \n } \n \n node '\c " >> $sitePPPath
wget -qO- http://instance-data/latest/meta-data/local-hostname >> $sitePPPath
echo -e "' { \n include java \n include hadoop::cluster::master \n } \n \n " >> $sitePPPath

#start auto update script
#The parameters have to be updated as soon as possible otherwise
#the datanodes on the slaves missbehave
chmod +x /etc/puppet/modules/hadoop/files/certListHostnames.sh 
nohup /etc/puppet/modules/hadoop/files/certListHostnames.sh &

#Download java
sed -i 's/1.7.0_03/1.7.0_51/g' /etc/puppet/modules/java/manifests/params.pp
curl  http://download.oracle.com/otn-pub/java/jdk/7u51-b13/jdk-7u51-linux-x64.tar.gz -L -b "oraclelicense=accept-securebackup-cookie" --silent --show-error --fail --connect-timeout 60 --max-time 600 --retry 5 -o '/etc/puppet/modules/java/files/jdk1.7.0_51.tar.gz'
#cp '/etc/puppet/modules/hadoop/files/jdk1.7.0_51.tar.gz' '/etc/puppet/modules/java/files/jdk1.7.0_51.tar.gz'


#download hadoop and change version in config
sed -i 's/2.0.3-alpha/2.3.0/g' /etc/puppet/modules/hadoop/manifests/params.pp
curl "http://ftp.halifax.rwth-aachen.de/apache/hadoop/common/hadoop-2.3.0/hadoop-2.3.0.tar.gz" -L --silent --show-error --fail --connect-timeout 60 --max-time 600 --retry 5 -o "/etc/puppet/modules/hadoop/files/hadoop-2.3.0.tar.gz"


#start puppet agent
puppet agent --enable
puppet agent

#wait so that yarn is started, hopefully
sleep 420

wget http://stratosphere-bin.s3-website-us-east-1.amazonaws.com/stratosphere-dist-0.5-SNAPSHOT-yarn.tar.gz

homePath=/home/ubuntu

 #untar stratopshere yarn
tar xvzf stratosphere-dist-0.5-SNAPSHOT-yarn.tar.gz -C $homePath/

cd $homePath/stratosphere-yarn-0.5-SNAPSHOT

export HADOOP_HOME=/opt/hadoop/hadoop/
export HADOOP_CONF_DIR=/opt/hadoop/hadoop/conf/
export JAVA_HOME=/opt/java/jdk1.7.0_51/
echo 'Start Stratosphere'
#start yarn session
#nohup ./bin/yarn-session.sh -n 1 -jm 1024 -tm 1024  >> stratosphere_yarn_session.log 2>&1 &
##### yarn session start code will be added by script to allow making the count and memory dynamic....


