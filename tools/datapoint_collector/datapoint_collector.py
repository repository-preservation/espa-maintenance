#!/usr/bin/env python
#
#
# Name: datapoint_collector.py
#
# Description: This script will collect defined data points for various things we want to track/measure, store
#              it into memcache and use them as triggers for checks and events.  It's reall quick and dirty 
#              right now.
#
# Author: Adam Dosch
#
# Date: 07/31/2014
#
################################################################################################################
# Change    Date            Author              Description
################################################################################################################
#  001      07/31/2014      Adam Dosch          Initial Release
#
################################################################################################################
# TODO:
#
# * There's almost non-existent exception trapping for python-memcached if a memcached sevice is down.  Just 
#   needs to be tested more but we can rely on the key timeout for now and assume that if key does not exist, 
#   then there's something wrong.
#

# -*- coding: utf-8 -*-
"""
Created on Thu Jul 31 12:32:25 2014

@author: adosch
"""

import memcache
import sys
import os
import ConfigParser
import commands
import re

config = None

class DataPointTracker(object):
    """
    Creates a DataPoint storage object to gather configured datapoints for ESPA framework
    """
    
    def __init__(self):
        
        self.datapoints = dict()
    
    def addDataPoint(self, datapoint, datapoint_command):
        """
        Add a new datapoint configuration value/command to the Datapoint tracker object to
        gather data for.
        
        NOTE:  Once added, it will immediately start gathering data.  No way to toggle it off
               other than removing the datapoint key (at this time)
        """

        if not self.datapoints.has_key(datapoint):
            self.datapoints[datapoint] = datapoint_command
        else:
            raise Exception("Datapoint '%s' already exists" % datapoint)
    
    def getDataPoint(self, datapoint):
        """
        Will return datapoint value gathered using configured datapoint command as string
        NOTE: Typecast this return at your own convenience
        """
        
        retval, output = commands.getstatusoutput(self.datapoints[datapoint].strip("\""))
        
        # If we return None, we had an issue running the command
        if retval == 0:
            return output
        else:
            return None
    
    def listDataPoints(self):
        """
        Will return list of all configured datapoint keys currently configured
        """
        
        return self.datapoints.keys()

    def removeDataPoint(self, datapoint):
        """
        Will remove a configured datapoint from the Datapoint tracker object
        
        Return 'True' if successful (or doesn't exist) or 'False' if not successful removing the datapoint
        """
        
        try:
            removed = self.datapoints.pop(datapoint, None)
            
            if removed:
                return True
            else:
                return True
                
        except KeyError:
            return False

def printUsage():
    
    print
    print " Usage: %s [-h|--help|--usage] [--config=/path/to/settings.ini]" % sys.argv[0]
    print
    print "  This script will collect defined data points for various things we want to track/measure, store"
    print "  it into memcache and use them as triggers for checks and events.  It's reall quick and dirty"
    print "  right now."
    print
    print "  A configuration file named 'settings.ini' needs to be available in the cwd or defined with '--config'"
    print
    print "  Expected memcache keyname will be the following combination of configuration values outlined below:"
    print 
    print "      keyname: <environment>:<version>:<datapoint_name>"
    print
    print "  Valid configuration sections/options are:"
    print
    print "   Section: configuration"
    print "   - Option: environment (used in memcache keyname forming)"
    print "   - Option: version (used in memcache keyname forming)"
    print
    print "   Section: memcache:"
    print "   - Option: server (comma separated list of ipaddr:port)"
    print "   - Option: key_expire_time (expire time for keys in memcache, in seconds)"
    print
    print "   Section: datapoints"
    print "   - Option: <datapoint_name> = ""/path/to/command/to/run | do_something | count_this"""
    print
    print "     You may have as many datapoints as you want defined, as long as they are unique"
    print
    sys.exit(1)

def validateArguments():
    
    global config    
    
    if re.search("^(-h|--usage|--help)", sys.argv[1]):
        printUsage()
        
    elif re.search("^--config=.*", sys.argv[1]):
        try:
            config = sys.argv[1].split("=")[1]
        except IndexError:
            # Argument not used right, no equal-sign to derive configfile
            printUsage()
    else:
        print
        print "Error: Invalid argument"

        printUsage()
              
def main():
    
    global config
    
    config = os.path.curdir + '/settings.ini'
    
    # Process arguments
    if len(sys.argv) - 1 == 1:
        validateArguments()
    elif len(sys.argv) - 1 > 1:
        printUsage()

    # Parse configuration
    Config = ConfigParser.ConfigParser()
    
    if os.path.isfile(config):
        try:
            Config.read(config)
        except ConfigParser.ParsingError, e:
            print "Bigtime parsing errors with configuration file: %s" % e
            sys.exit(2)
    else:
        print "We need a configuration file, settings.ini, in current working directory, pal.  QUITTING!"
        sys.exit(3)

    # Connect to memcache
    mc = memcache.Client(list(Config.get('memcache', 'server').strip("'").split(",")), debug=0)

    mc_key_expire = int(Config.get('memcache', 'key_expire_time'))
    
    # Load up the configured datapoints in DataTracker
    d = DataPointTracker()
  
    for datapoint in Config.options('datapoints'):
        datapoint_command = Config.get('datapoints', datapoint)
        
        d.addDataPoint(datapoint, datapoint_command)
    
    # Fetch and set datapoints in memcached - TODO: thread this
    for datapoint in d.listDataPoints():
        mc_key_name = "%s:%s:%s" % (Config.get('configuration', 'environment').strip("\""), Config.get('configuration', 'version'), datapoint)
        
        mc.set(mc_key_name, d.getDataPoint(datapoint), time=mc_key_expire)

if __name__ == '__main__':
    main()
