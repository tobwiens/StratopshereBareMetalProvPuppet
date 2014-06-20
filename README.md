stratDeployPuppetScriptEc2
==============


Running the script - Installs and configures Stratosphere,HDFS and Yarn 
-------------------
1. Create Key Pairs for accessing your instances in the correct region, default is: eu-west-1 (Ireland).
2. Enter Key Pair names into conf/instances.cfg at 'key-name'.
3. Enter your Secret Key and Key ID into /conf/instances.cfg (key-ID,aws-secret-key).
4. Run {python StartInstances.py}.
5. Wait for cluster to be running and access the YARN interface via the URL given from the script.
6. Press any key to terminate your cluster. Or do it manually via the Amazon|OpenStack web interface.


Configuration file
-------------------
- When working with Amazon instances only key-name and Amazon credentials (key-ID,aws-secret-key) must be changed in order to run a cluster.
- Adjust configuration file towards your needs.

Configuration file details
-------------------
- ip-access: The IP which is given SSH access. Default is every IP address. Because the instances are protected via the Key Pairs there is no need to change that.
- spot-price: The script acquires spot instances, which are placed in the same availability zone so that fast and free transfer between nodes is ensured.
- user-data-file: The script which is given for execution at start-up. 
- cluster-name: Instances are put into the same availability zone. When starting different clusters use different names otherwise all instances will be put into the same availability zone. That might lead to restrictions with spot instances.
- region: Amazon region in which to start the instances. Has to be changed for OpenStack.
- image-ID: Defines which image is used as a base for provisioning.