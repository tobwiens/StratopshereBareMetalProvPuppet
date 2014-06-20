#!/bin/python 
'''
Created on 15/04/2014

@author: Tobias Wiens
'''

MAXIMUM_TRIES = 100

from PuppetConfigFileManager import ConfigFileManager
from BotoConnectionManager import BotoConnectionManager
import sys, time, boto

def setupSecurityGroups(amazonConnection, configFile):
    '''
    Sets up security groups. Creates them + sets up access.
    '''
    #Automatically configure security groups
    print 'create security groups'
    createSecurityGroup(ec2RegionConnection=amazonConnection.ec2RegionConnection,
                        securityGroupName=configFile.getMasterSecurityGroup())
    createSecurityGroup(ec2RegionConnection=amazonConnection.ec2RegionConnection,
                        securityGroupName=configFile.getSlavesSecurityGroup())
    
    #Access from Master to Slaves and backwards and slaves to slaves
    print 'configure security group access'
    authorizeInboundSecGroup(securityGroupName= configFile.getMasterSecurityGroup(),
                             ec2RegionConnection=amazonConnection.ec2RegionConnection,
                             toAuthSecGroupName = configFile.getSlavesSecurityGroup(),
                             ipProtocol='tcp',
                             fromPort= 0,
                             toPort=65535)
    authorizeInboundSecGroup(securityGroupName=configFile.getSlavesSecurityGroup(),
                             ec2RegionConnection=amazonConnection.ec2RegionConnection,
                             toAuthSecGroupName = configFile.getMasterSecurityGroup(),
                             ipProtocol='tcp',
                             fromPort= 0,
                             toPort=65535)
    authorizeInboundSecGroup(securityGroupName=configFile.getSlavesSecurityGroup(),
                             ec2RegionConnection=amazonConnection,
                             toAuthSecGroupName = configFile.getSlavesSecurityGroup(),
                             ipProtocol='tcp',
                             fromPort= 0,
                             toPort=65535)
    authorizeInboundSecGroup(securityGroupName= configFile.getMasterSecurityGroup(),
                             ec2RegionConnection=amazonConnection.ec2RegionConnection,
                             toAuthSecGroupName = configFile.getSlavesSecurityGroup(),
                             ipProtocol='udp',
                             fromPort= 0,
                             toPort=65535)
    authorizeInboundSecGroup(securityGroupName=configFile.getSlavesSecurityGroup(),
                             ec2RegionConnection=amazonConnection.ec2RegionConnection,
                             toAuthSecGroupName = configFile.getMasterSecurityGroup(),
                             ipProtocol='udp',
                             fromPort= 0,
                             toPort=65535)
    authorizeInboundSecGroup(securityGroupName=configFile.getSlavesSecurityGroup(),
                             ec2RegionConnection=amazonConnection,
                             toAuthSecGroupName = configFile.getSlavesSecurityGroup(),
                             ipProtocol='udp',
                             fromPort= 0,
                             toPort=65535)
    
    #Allow SSH access
    print 'Authorize ssh at master and slaves'
    authorizeInboundIP(securityGroupName=configFile.getMasterSecurityGroup(),
                       ec2RegionConnection=amazonConnection.ec2RegionConnection,
                       ipAddress = '0.0.0.0/0',
                       ipProtocol='tcp',
                       fromPort=22,
                       toPort=22)
    authorizeInboundIP(securityGroupName=configFile.getSlavesSecurityGroup(),
                       ec2RegionConnection=amazonConnection.ec2RegionConnection,
                       ipAddress = '0.0.0.0/0',
                       ipProtocol='tcp',
                       fromPort=22,
                       toPort=22)
    print 'Allow access to yarn web-interface'
    authorizeInboundIP(securityGroupName=configFile.getMasterSecurityGroup(),
                       ec2RegionConnection=amazonConnection.ec2RegionConnection,
                       ipAddress = '0.0.0.0/0',
                       ipProtocol='tcp',
                       fromPort=9026,
                       toPort=9026)
    

def createSecurityGroup(ec2RegionConnection=None, securityGroupName=None):
    '''
    Creates a security group.
    @param ec2RegionConnection: connection to region where to create the security group
    @type ec2RegionConnection: boto.ec2.connection
    @param securityGroupName: Name of security group to create
    @type securityGroupName: String
    '''
    
    try:
        ec2RegionConnection.get_all_security_groups(groupnames=[securityGroupName])[0]
        print 'Security group: '+str(securityGroupName)+' does exist. It will not be created.'
        return True
    except:
        print 'Security group: '+str(securityGroupName)+' will be created'
     
    try:
        ec2RegionConnection.create_security_group(name=securityGroupName, description=securityGroupName)
    except boto.exception.EC2ResponseError as e:
        print e
    
    return True

def authorizeInboundIP(securityGroupName, ec2RegionConnection, ipAddress = None, ipProtocol='tcp', fromPort= None, toPort=None):
    '''
    Authorizes IP access for everyone to a specific security group. When ipAddress is give SSH access will be authorized for 
    that specific IP address
    @param securityGroupName: Name of the security group in that ec2 region
    @type securityGroupName: String
    @param amazonConnection: Boto connection an amazon region
    @type amazonConnection: Boto connection to a specific amazon region
    @param ipAddress: IP address which is allowed to access that security group with SSH
    @type ipAddress: String in form of '127.0.0.1/32'
    @param ipProtocol: Protocol type which is allowed to access that security group with SSH
    @type ipAddress: String in form of 'tcp' or 'udp'
    @param fromPort: Port number to start allowance from
    @type fromPort: Integer
    @param toPort: Port number to end allowance at
    @type toPort: Integer
    '''
    #Get security Group
    try:
        jobManagerSecGroup = ec2RegionConnection.get_all_security_groups(groupnames=[securityGroupName])[0]
    except:
        return False   
    
    if ipAddress is None :
        ipAddress = '0.0.0.0/0'
    
    #allow access
    try:
        jobManagerSecGroup.authorize(ip_protocol=ipProtocol, from_port=fromPort, to_port=toPort, cidr_ip= ipAddress)
    except boto.exception.EC2ResponseError as e:
        print e
        
def authorizeInboundSecGroup(securityGroupName, ec2RegionConnection, toAuthSecGroupName = None, ipProtocol='tcp', fromPort= None, toPort=None):
    '''
    Authorizes IP access for everyone to a specific security group. When ipAddress is give SSH access will be authorized for 
    that specific IP address
    @param securityGroupName: Name of the security group in that ec2 region
    @type securityGroupName: String
    @param amazonConnection: Boto connection an amazon region
    @type amazonConnection: Boto connection to a specific amazon region
    @param secGroupName: Security group which granted access.
    @type secGroupName: String: name of security group which want to grant access to.
    '''
    #Get security Group
    try:
        secGroup = ec2RegionConnection.get_all_security_groups(groupnames=[securityGroupName])[0]
    except:
        return False   
    
    if toAuthSecGroupName is None :
        print 'security group which is granted has to be specified'
        return False
    
    #allow access
    try:
        toAuthGroup = ec2RegionConnection.get_all_security_groups(groupnames=[toAuthSecGroupName])[0]
        secGroup.authorize(ip_protocol=ipProtocol, from_port=fromPort, to_port=toPort, src_group=toAuthGroup)
    except boto.exception.EC2ResponseError as e:
        print e
    

def waitUntilInstanceIsRunning(instance):
        '''
        @param instance: Instance type from boto 
        '''
        #check if instance is running
        while instance.state != 'running':
            instance.update()
            time.sleep(2)
            
        return True
    
def waitUntilInstanceHasIP(instance):
        '''
        @param instance: Instance type from boto 
        '''
        #check if instance is running
        while instance.state != 'running' and instance.state != 'pending':
            instance.update()
            time.sleep(2)
            
        return True

def getFileContent(fileWithPath):
    '''
    Tries to get the key material from hard disk or creates another keyPair and saves it on hard disk
    @param keyName: Name of the key in amazon ec2
    @type keyName: String
    @param keyPath: Local path and file name of the key /path/to/key/file.pem for example
    @type keyPath: String
    '''
    print fileWithPath
    openKeyFile = file(fileWithPath, 'r')
    #create empty string and concatinate lines
    result = ''
    for line in openKeyFile:
        result += line
        
    openKeyFile.close()
    return result 

def startAmazonInstance(ec2RegionConnection=None, instanceCount=None, spotPrice=None, keyName=None, availabilityZone=None, securityGroupsList=None, imageId=None, instanceType=None, userData=None, placement=None):
    if(spotPrice != None):
        spotInstanceRequestList = ec2RegionConnection.request_spot_instances(count=instanceCount,
                                                                                         image_id=imageId,
                                                                                         instance_type=instanceType,
                                                                                         key_name=keyName,
                                                                                         security_groups=securityGroupsList,
                                                                                         user_data=userData,
                                                                                         availability_zone_group = availabilityZone,
                                                                                         price=spotPrice,
                                                                                          dry_run=False)
        #wait until request is fulfilled
        requestIdList = [ request.id for request in spotInstanceRequestList]
        
        #Amazon does not list request directly, so wait until request is accessible
        while True:
            try:
                amazonConnection.ec2RegionConnection.get_all_spot_instance_requests(request_ids=requestIdList)
                break # NO exception was thrown
            except:
                time.sleep(2) #Wait a little bit
        
        while not all(request.state == 'active' for request in amazonConnection.ec2RegionConnection.get_all_spot_instance_requests(request_ids=requestIdList) ) :
            time.sleep(2)
        
        #request fulfilled 
        instanceIdList = [request.instance_id for request in amazonConnection.ec2RegionConnection.get_all_spot_instance_requests(request_ids=requestIdList) ]
        print instanceIdList
        instanceReservation = amazonConnection.ec2RegionConnection.get_all_reservations(instance_ids=instanceIdList)
        
        #create a list of instances
        instanceList = []
        for reservation in instanceReservation:
            instanceList.extend( [instance for instance in reservation.instances] )
        
        print instanceList
        #Get master instance object
        if len(instanceList) > 0:
            print str(len(instanceList))+' instance[s] succesfully started'
        else:
            sys.exit('ERROR - Check amazon web interface for running instances and terminate them')
    
        return instanceList
    else:
        instanceReservation = ec2RegionConnection.run_instances(min_count=1,
                                                                                         max_count=instanceCount,
                                                                                         image_id=imageId,
                                                                                         instance_type=instanceType,
                                                                                         key_name=keyName,
                                                                                         placement = placement,
                                                                                         security_groups=securityGroupsList,
                                                                                         user_data=userData
                                                                                         ) 
        #return instances
        return instanceReservation.instances
    
    

if __name__ == '__main__':
    print 'Stratosphere EC2 Deployment'
    
    #To connect to amazon we need to read the region which to connect to, out of the config file
    configFile = ConfigFileManager()
    print 'Config file opened'
    
    amazonConnection = None #Initialize amazon connection
    
    if configFile.isOpenStackUrlSet() is False:
    #Connect to amazon region
        amazonConnection = BotoConnectionManager(aws_secret_key=configFile.getAWSAccesKey(), aws_key_id=configFile.getAWSKeyID(), region=configFile.getRegion())
        print "Successfully connected to region "+configFile.getRegion()
    
    else: #Connect to OpenStack
        amazonConnection = BotoConnectionManager(aws_secret_key=configFile.getAWSAccesKey(), aws_key_id=configFile.getAWSKeyID(), openStack=True, openStackAdress=configFile.getOpenStackUrl())
    
    #authorizeSSH access 
    #if AmazonSSHUtils.authorizeSSH(securityGroupName = configFile.getSecurityGroup(), amazonConnection = amazonConnection, ipAddress=configFile.getIPAccess()) is False:
    #    print 'Security group '+configFile.getSecurityGroup()+' not found'
    #    sys.exit('Update config file')
    
    #Get secret SSH key
    #keyMaterial = getFileContent(fileWithPath=configFile.getKeyPath())
    
    #Get user data
    #userData = "#!/bin/bash -x \n exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1 \n"
    #add user name
    
    setupSecurityGroups(amazonConnection=amazonConnection, configFile=configFile)
    
    
    userDataMaster = getFileContent(fileWithPath=configFile.getMasterUserDataFile())
    
    print 'Customize master start script'
    #Wait until all expected nodes are started
    
    #Determine running nodes via Yarn rest API
    determineRunningNodes = ''
    determineRunningNodes += "\n runningNodes=$(curl --silent http://localhost:9026/ws/v1/cluster/metrics | grep -Po '"
    determineRunningNodes += '\"totalNodes\"'
    determineRunningNodes += ":.,' | sed 's/"
    determineRunningNodes += '\"totalNodes\"'
    determineRunningNodes += ":/ /' | sed 's/,//') \n"
    
    userDataMaster += determineRunningNodes
    userDataMaster += 'until [ $runningNodes  -ge '+str(configFile.getSlavesInstanceCount())+' ] \n do \n sleep 5 '+determineRunningNodes+'\n done \n'
    #Start the Stratosphere application
    userDataMaster += '\n nohup ./bin/yarn-session.sh -n '
    userDataMaster += str(int(configFile.getSlavesInstanceCount()) - 1)
    userDataMaster += ' -jm '
    userDataMaster += str(configFile.getStratosphereJobmanagerMemory())
    userDataMaster += ' -tm '
    userDataMaster += str(configFile.getStratosphereTaskmanagerMemory())
    userDataMaster += ' >> stratosphere_yarn_session.log 2>&1 &'
    
    print userDataMaster

    
    print 'Start one instance of type '+configFile.getMasterInstanceType()+' to run the puppet master instance with image id: '+configFile.getMasterImageId()
    #create variable to access master instance request
    puppetMasterInstanceList = None
    if configFile.isMasterSpotPriceSet():
        print 'Request Master as a spot instance with price '+configFile.getMasterSpotPrice()
        puppetMasterInstanceList = startAmazonInstance(ec2RegionConnection=amazonConnection.ec2RegionConnection,
                                               instanceCount=1,
                                               spotPrice=configFile.getMasterSpotPrice(),
                                               keyName=configFile.getKeyName(),
                                               availabilityZone=configFile.getClusterName(),
                                               securityGroupsList=[configFile.getMasterSecurityGroup()],
                                               imageId=configFile.getMasterImageId(),
                                               instanceType=configFile.getMasterInstanceType(),
                                               userData=userDataMaster)
    else:
        puppetMasterInstanceList = startAmazonInstance(ec2RegionConnection=amazonConnection.ec2RegionConnection,
                                               instanceCount=1,
                                               keyName=configFile.getKeyName(),
                                               availabilityZone=configFile.getClusterName(),
                                               securityGroupsList=[configFile.getMasterSecurityGroup()],
                                               imageId=configFile.getMasterImageId(),
                                               instanceType=configFile.getMasterInstanceType(),
                                               userData=userDataMaster)
    
        
    #Wait for master to be running
    puppetMasterInstance = puppetMasterInstanceList[0]
    waitUntilInstanceIsRunning(puppetMasterInstance)
    print 'Puppet master is running '+puppetMasterInstance.public_dns_name+'/'+puppetMasterInstance.private_dns_name
    
    #Prepare salves user data
    userDataSlave = getFileContent(fileWithPath=configFile.getSlavesUserDataFile())
    
    #replace ::MASTER.IP:: with the real master ip address
    userDataSlave = userDataSlave.replace("::MASTER.IP::", puppetMasterInstance.private_ip_address)
    print userDataSlave
    
    #Start puppet slaves
    puppetSlavesInstanceList = None
    print str(configFile.getSlavesInstanceCount())+' Puppet slaves will be started'
    
    print 'Start '+str(configFile.getSlavesInstanceCount())+' instance[s] of type '+str(configFile.getSlavesInstanceType())+' to run the puppet slave instances with image id: '+configFile.getSlavesImageId()
    if configFile.isSlavesSpotPriceSet():
        print 'Request Slaves as a spot instance with price '+configFile.getSlavesSpotPrice()
        puppetSlavesInstanceList = startAmazonInstance(ec2RegionConnection=amazonConnection.ec2RegionConnection,
                                               instanceCount=configFile.getSlavesInstanceCount(),
                                               spotPrice=configFile.getSlavesSpotPrice(),
                                               keyName=configFile.getKeyName(),
                                               availabilityZone=configFile.getClusterName(),
                                               securityGroupsList=[configFile.getSlavesSecurityGroup()],
                                               imageId=configFile.getSlavesImageId(),
                                               instanceType=configFile.getSlavesInstanceType(),
                                               userData=userDataSlave)
    else:
        puppetSlavesInstanceList = startAmazonInstance(ec2RegionConnection=amazonConnection.ec2RegionConnection,
                                               instanceCount=configFile.getSlavesInstanceCount(),
                                               keyName=configFile.getKeyName(),
                                               availabilityZone=configFile.getClusterName(),
                                               securityGroupsList=[configFile.getSlavesSecurityGroup()],
                                               imageId=configFile.getSlavesImageId(),
                                               instanceType=configFile.getSlavesInstanceType(),
                                               userData=userDataSlave)
    
    print 'Cluster '+configFile.getClusterName()+' successfully set up. Have fun with your cluster ;)'
    print "Master address: "+puppetMasterInstance.public_dns_name
    
    print 'Open the web page '+puppetMasterInstance.public_dns_name+':9026 '
    print 'When Stratosphere is started it will appear  '
    
    
    raw_input("Press enter to shutdown cluster...")
    
    print 'shutting down cluster'
    #shutdown slaves
    slavesInstanceIdList = [ instance.id for instance in puppetSlavesInstanceList]
    
    amazonConnection.ec2RegionConnection.terminate_instances(instance_ids=slavesInstanceIdList)
    
    puppetMasterInstance.terminate()
    
    print 'Cluster offline!!!'