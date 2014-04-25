stratDeployPuppetScriptEc2
==============

Running the script - Installs and configures Stratosphere,HDFS and Yarn 
-------------------
1. Create certificates for ssh access
2. Create two security groups {master group and slaves group} which allow all TCP+UDP traffic to each other and themselves
3. Write configuration file
4. run {python StartInstances.py}


Configuration file
-------------------
- adjust configuration file towards your needs
- enter credentials, certificate and security groups

