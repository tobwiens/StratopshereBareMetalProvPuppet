'''
Created on 15/04/2014

@author: Tobias Wiens
'''
import ConfigParser

class ConfigFileManager(object):
    '''
    This file is managing the config file which inherits all important configuration variables.
    The config file contains an overall section which contains:
    aws_key_id
    aws_acces_key
    region
    image id
    setup commands
    create setup image id with name
    '''

    #Global variables
    configPath = 'conf/instances.cfg'
    config = None
    
    #Section names
    OVERALL_SECTION_NAME = 'Basic'
    MASTER_SECTION_NAME = 'Master'
    SLAVES_SECTION_NAME = 'Slaves'
    JAVA_SECTION_NAME = 'Java'
    STRATOSPHERE_SECTION_NAME = 'Stratosphere'
    
    #Overall section OPTIONS
    OVERALL_AWS_SECRET_KEY = 'aws-secret-key'
    OVERALL_AWS_KEY_ID = 'key-ID'
    OVERALL_KEY_PATH = 'key-path'
    OVERALL_KEY_NAME = 'key-name'
    OVERALL_REGION = 'region'
    OVERALL_IPACCES = 'IP-access'
    OVERALL_CLUSTER_NAME = 'cluster-name'
    OVERALL_OPENSTACK_URL = 'openstack-url'
    
    #SLAVES SECTION
    SLAVES_IMAGE_ID = 'image-ID'
    SLAVES_USERNAME = 'username'
    SLAVES_INSTANCE_TYPE = 'instance-type'
    SLAVES_SECURITY_GROUP = 'security-group'
    SLAVES_INSTANCE_COUNT = 'instance-count'
    SLAVES_USER_DATA_FILE = 'user-data-file' 
    SLAVES_SPOT_PRICE = 'spot-price' 
    
    #MASTER SECTION
    MASTER_IMAGE_ID = 'image-ID'
    MASTER_USERNAME = 'username'
    MASTER_INSTANCE_TYPE = 'instance-type'
    MASTER_SECURITY_GROUP = 'security-group'
    MASTER_INSTANCE_COUNT = 'instance-count'
    MASTER_USER_DATA_FILE = 'user-data-file'  
    MASTER_SPOT_PRICE = 'spot-price'
    
    #Stratosphere
    STRATOSPHERE_TASKMANAGER_MEMORY = 'taskmanager-memory'
    STRATOSPHERE_JOBMANAGER_MEMORY = 'jobmanager-memory'
    
    
    
    
    
    def __init__(self, configPath=None):
        '''
        Constructor:
        Create config file. If file does not exist a new empty configuration file will be created
        @param configPath: /path/to/file/file.config
        @type configPath: String
        '''
        if configPath is not None:
            self.configPath = configPath
        
        #create config object
        self.config = ConfigParser.ConfigParser()
        try:
            openConfigFile = file(self.configPath, "r")
            self.config.readfp(fp=openConfigFile)
        except IOError as e:
            print e
            print 'No config file found '+configPath  
         
       
    
        
    def getAWSAccesKey(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_AWS_SECRET_KEY)
    
    def getAWSKeyID(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_AWS_KEY_ID)
    
    def getRegion(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_REGION)
    
    def getIPAccess(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_IPACCES)
    
    def getClusterName(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_CLUSTER_NAME)
    
    def getOpenStackUrl(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_OPENSTACK_URL)
    
    def isOpenStackUrlSet(self):
        '''
        returns True if a spot price is set for the master instance. False otherwise.
        '''
        try:
            self.config.get(self.OVERALL_SECTION_NAME, self.OVERALL_OPENSTACK_URL)
            return True
        except:
            return False
    
    def getMasterUsername(self):
        return self.config.get(section=self.MASTER_SECTION_NAME, option=self.MASTER_USERNAME)
    
    def getMasterImageId(self):
        return self.config.get(section=self.MASTER_SECTION_NAME, option=self.MASTER_IMAGE_ID)
    
    def getSlavesUsername(self):
        return self.config.get(section=self.SLAVES_SECTION_NAME, option=self.SLAVES_USERNAME)
    
    def getKeyPath(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_KEY_PATH)
    
    def getKeyName(self):
        return self.config.get(section=self.OVERALL_SECTION_NAME, option=self.OVERALL_KEY_NAME)
    
    def getMasterInstanceType(self):
        return self.config.get(section=self.MASTER_SECTION_NAME, option=self.MASTER_INSTANCE_TYPE)
   
    def getMasterSecurityGroup(self):
        return self.config.get(section=self.MASTER_SECTION_NAME, option=self.MASTER_SECURITY_GROUP)
    
    def getMasterUserDataFile(self):
        return self.config.get(section=self.MASTER_SECTION_NAME, option=self.MASTER_USER_DATA_FILE)
    
    def getSlavesImageId(self):
        return self.config.get(section=self.SLAVES_SECTION_NAME, option=self.SLAVES_IMAGE_ID)
    
    def getSlavesInstanceType(self):
        return self.config.get(section=self.SLAVES_SECTION_NAME, option=self.SLAVES_INSTANCE_TYPE)
    
    def getSlavesSecurityGroup(self):
        return self.config.get(section=self.SLAVES_SECTION_NAME, option=self.SLAVES_SECURITY_GROUP)
    
    def getSlavesInstanceCount(self):
        return self.config.get(section=self.SLAVES_SECTION_NAME, option=self.SLAVES_INSTANCE_COUNT)
    
    def getSlavesUserDataFile(self):
        return self.config.get(section=self.SLAVES_SECTION_NAME, option=self.SLAVES_USER_DATA_FILE)
    
    def getStratosphereTaskmanagerMemory(self):
        return self.config.get(section=self.STRATOSPHERE_SECTION_NAME, option=self.STRATOSPHERE_TASKMANAGER_MEMORY)
    
    def getStratosphereJobmanagerMemory(self):
        return self.config.get(section=self.STRATOSPHERE_SECTION_NAME, option=self.STRATOSPHERE_JOBMANAGER_MEMORY)
    
    def isMasterSpotPriceSet(self):
        '''
        returns True if a spot price is set for the master instance. False otherwise.
        '''
        try:
            self.config.get(self.MASTER_SECTION_NAME, self.MASTER_SPOT_PRICE)
            return True
        except:
            return False
        
        
    def isSlavesSpotPriceSet(self):
        '''
        True if a spot price is set for the slaves. False otherwise
        '''
        try:
            self.config.get(self.SLAVES_SECTION_NAME, self.SLAVES_SPOT_PRICE)
            return True
        except:
            return False
        
    def getMasterSpotPrice(self):
        return self.config.get(section=self.MASTER_SECTION_NAME, option=self.MASTER_SPOT_PRICE)
    
    def getSlavesSpotPrice(self):
        return self.config.get(section=self.SLAVES_SECTION_NAME, option=self.SLAVES_SPOT_PRICE)