'''
Created on 23/12/2013

@author: Tobias Wiens
'''

import boto.ec2
import sys

class BotoConnectionManager(object):
    '''
    Maintains one connection to one amazon region. The idea was to implement a singleton like class.
    Critics: The connection can be kept in the main script
    This is not singleton at all
    No real advantage
    '''

    ec2RegionConnection = None

    def __init__(self, aws_secret_key, aws_key_id, region):
        '''
        Connects to a amazon region and keeps open connection in class.
        '''
        self.ec2RegionConnection = boto.ec2.connect_to_region(region, aws_access_key_id=aws_key_id,  aws_secret_access_key=aws_secret_key)
        if self.ec2RegionConnection is None:
            print 'Could not connect. Wrong region specified'
            sys.exit('Change configuration file!')
        
        try:
            self.ec2RegionConnection.get_all_placement_groups()
        except:
            sys.exit('Aws key id and secret key wrong, change configuration file!')
            
        