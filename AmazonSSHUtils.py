'''
Created on 18/01/2014

@author: Tobias Wiens
'''

import time, boto

MAXIMUM_TRIES = 100

def authorizeSSH(securityGroupName, amazonConnection, ipAddress = None):
    '''
    Authorizes SSH access for everyone to a specific security group. When ipAddress is give SSH access will be authorized for 
    that specific IP address
    @param securityGroupName: Name of the security group in that ec2 region
    @type securityGroupName: String
    @param amazonConnection: Boto connection an amazon region
    @type amazonConnection: Boto connection to a specific amazon region
    @param ipAddress: IP address which is allowed to access that security group with SSH
    @type ipAddress: String in form of '127.0.0.1/32'
    '''
    #Get security Group
    try:
        jobManagerSecGroup = amazonConnection.ec2RegionConnection.get_all_security_groups(groupnames=[securityGroupName])[0]
    except:
        return False   
    
    if ipAddress is None :
        ipAddress = '0.0.0.0/0'
    
    #allow access
    try:
        jobManagerSecGroup.authorize(ip_protocol='tcp', from_port=22, to_port=22, cidr_ip= ipAddress)
    except boto.exception.EC2ResponseError as e:
        print e
        
    
    
def revokeSSH(securityGroupName, amazonConnection, ipAddress = None):
    '''
    Revokes SSH access for everyone to a specific security group. When ipAddress is give SSH access will be revoked for 
    that specific IP address if a rule which matches exactly exists.
    @param securityGroupName: Name of the security group in that ec2 region
    @type securityGroupName: String
    @param amazonConnection: Boto connection an amazon region
    @type amazonConnection: Boto connection to a specific amazon region
    @param ipAddress: IP address which is was allowed to access that security group with SSH but which rule will now be deleted
    @type ipAddress: String in form of '127.0.0.1/32'
    '''
    try:
        secGroup = amazonConnection.ec2RegionConnection.get_all_security_groups(groupnames=[securityGroupName])[0]
    except:
        return False
    
    if ipAddress is None :
        ipAddress = '0.0.0.0/0'
    
    #allow access
    try:
        secGroup.revoke(ip_protocol='tcp', from_port=22, to_port=22, cidr_ip= ipAddress)
    except boto.exception.EC2ResponseError as e:
        print e

def executeCommand(command, sshClient):
    '''
    Executed one command and waits until the command has finished.
    @param command: Command to be executed.
    @type command: String
    @param sshClient: Paramiko connected ssh client to one specific machine.
    @type sshClient: Connected and authorized Paramiko ssh client
    '''
    print command
    #get a new channel
    transport = sshClient.get_transport()
    session = transport.open_session()
    result = session.exec_command(command=command)
    for i in range(MAXIMUM_TRIES*2):
        time.sleep(1)
        if session.exit_status_ready():
            break
    return result
