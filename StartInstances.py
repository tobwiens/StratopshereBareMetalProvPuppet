'''
Created on 15/04/2014

@author: Tobias Wiens
'''

MAXIMUM_TRIES = 100

from PuppetConfigFileManager import ConfigFileManager
from BotoConnectionManager import BotoConnectionManager
import sys, time


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
    keyMaterial = getFileContent(fileWithPath=configFile.getKeyPath())
    
    #Get user data
    #userData = "#!/bin/bash -x \n exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1 \n"
    #add user name
    userDataMaster = getFileContent(fileWithPath=configFile.getMasterUserDataFile())
    print userDataMaster
    
    '''print 'Customize master start script'
    userDataMaster += 'nohup ./bin/yarn-session.sh -n '
    userDataMaster += str(configFile.getSlavesInstanceCount() - 1)
    userDataMaster += ' -jm '
    userDataMaster +=
    userDataMaster += ' -tm '
    userDataMaster += 
    userDataMaster '>> stratosphere_yarn_session.log 2>&1 &'
    '''

    
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
    
    
    raw_input("Press enter to shutdown cluster...")
    
    print 'shutting down cluster'
    #shutdown slaves
    slavesInstanceIdList = [ instance.id for instance in puppetSlavesInstanceList]
    
    amazonConnection.ec2RegionConnection.terminate_instances(instance_ids=slavesInstanceIdList)
    
    puppetMasterInstance.terminate()
    
    print 'Cluster offline!!!'