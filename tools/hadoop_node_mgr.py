#!/usr/bin/env python
#
#
#
# Name: hadoop_manager.py
#
# Description: Manages Map/Reduce, DFS and Slave entries for Hadoop
#
# Author: Adam Dosch
#
# Date: 4-25-2013
#
################################################################################################
# Change    Date            Author              Description
################################################################################################
#  001      04-25-2013      Adam Dosch          Initial Release
#  002      07-01-2013      Adam Dosch          Commented out test command for jps in the
#                                               masterOrNamenode() check
#  003      07-15-2013      Adam Dosch          Updating to work with Hadoop 1.1.2.  HADOOP_HOME
#                                               has been depreciated in lieu of HADOOP_PREFIX.
#                                               Going to look for both HADOOP_HOME, if it doesn't
#                                               exist, look for HADOOP_PREFIX, else bail out.
#                                               doing a '-refreshNodes' on host add for dfs or
#                                               mapreduce doesn't start Hadoop back up.  Adding
#                                               remote SSH command to start tasktracker and/or
#                                               datanode
#
################################################################################################

import sys
import os
import commands
import re
import argparse
import xml.etree.ElementTree as xml

err_code = { 'err_prelim': 2,
             'err_valconf': 3,
             'err_general': 1
}

HADOOP_HOME = ""

verbose = False

hadoopConfigs = {}

# add node entries
def addNodes(configFile, nodelist):
    # Open for adding node(s) to configuration file
    f = open(configFile, "a")
    
    for node in nodelist:
        if verbose:
            print "Adding", node, " to", configFile
        f.write(node + "\n")
        
    f.close()   

# remove node entries
def removeNodes(configFile, nodelist):
    # Open for reading in current configuration file
    if verbose:
        print configFile
        
    f = open(configFile, "r")

    lines = f.readlines()
        
    f.close()

    # Open for re-writing config and removing node(s)
    f = open(configFile, "w")

    for line in lines:
    
        matchnode = line.strip()

        if matchnode not in nodelist:
            if verbose:
                print "writing line ", line
                
            f.write(line)
        else:
            if verbose:
                print "skip writing, we matched ", line
            
    f.close()    

# perform argument actions
def performActions(args):
    
    verbose = args.verbose
    
    # Any nodes, create a list of them
    if args.nodes <> None:
        nodes = list(args.nodes.upper().split(","))
        
        if verbose:
            print "Node list: ", nodes

    if verbose:
        print "mr section"
        
    # map/reduce action section
    if args.mraction <> None:
        if "add" in args.mraction:
            try:
                if verbose:
                    print "adding nodes to mr"
                    
                # Remove Nodes from hosts.excludes
                removeNodes(hadoopConfigs["mapred.hosts.exclude"], nodes)
            
                # Add nodes to hosts
                addNodes(hadoopConfigs["mapred.hosts"], nodes)
                
                # refresh service
                if not refreshService("mradmin", nodes):
                    return False
                
                # start tasktracker remotely on node
                if not startService("mradmin", nodes):
                    return False
                
            except Exception, e:
                if verbose:
                    print e
                    
                return False
        
        if "remove" in args.mraction:
            try:
                if verbose:
                    print "removing nodes from mr"
                    
                # Remove node(s) from hosts
                removeNodes(hadoopConfigs["mapred.hosts"], nodes)
            
                # Add node(s) to hosts.exclude
                addNodes(hadoopConfigs["mapred.hosts.exclude"], nodes)
                
                # refresh service
                if not refreshService("mradmin"):
                    return False

                # stop tasktracker remotely on node
                if not stopService("mradmin"):
                    return False

            except Exception, e:
                if verbose:
                    print e
                    
                return False
        
        if "refresh" in args.mraction:
            if verbose:
                print "refreshing mr svc"
                
            if not refreshService("mradmin"):
                return False
    
    if verbose:
        print "dfs section"
        
    # hdfs action section
    if args.dfsaction <> None:
        if "add" in args.dfsaction:
            try:
                if verbose:
                    print "adding to dfs"
                    
                # Remove node(s) from hosts.excludes
                removeNodes(hadoopConfigs["dfs.hosts.exclude"], nodes)
            
                # Add node(s) to hosts
                addNodes(hadoopConfigs["dfs.hosts"], nodes)
                
                # refresh service
                if not refreshService("dfsadmin"):
                    return False
            
                # start datanode service remotely on node
                if not startService("dfsadmin"):
                    return False
            except:
                return False
        
        if "remove" in args.dfsaction:
            try:
                if verbose:
                    print "removing from dfs"
                    
                # Remove node(s) from hosts
                removeNodes(hadoopConfigs["dfs.hosts"], nodes)
            
                # Add node(s) to hosts.exclude
                addNodes(hadoopConfigs["dfs.hosts.exclude"], nodes)
                
                # refresh service
                if not refreshService("dfsadmin", nodes):
                    return False

                # stop datanode service remotely on node
                if not stopService("dfsadmin", nodes):
                    return False
            except:
                return False
        
        if "refresh" in args.dfsaction:
            if verbose:
                print "refreshing dfs svc"
            
            if refreshService("dfsadmin"):
                return True
            else:
                return False
    
    if verbose:
        print "slaves section"
    
    # slaves action section
    if args.slavesaction <> None:
        if "add" in args.slavesaction:
            try:
                if verbose:
                    print "adding to slaves"
                
                # Add node(s) to slaves
                addNodes(hadoopConfigs["slaves"], nodes)
                
            except Exception, e:
                print e
                return False
        
        if "remove" in args.slavesaction:
            try:
                if verbose:
                    print "removing from slaves"
                
                # Remove node(s) from slaves
                removeNodes(hadoopConfigs["slaves"], nodes)
            except:
                return False

# start tasktracker/datanode service on remote host(s)
def startService(service, nodelist):
    for node in nodelist:
        if verbose:
            print "Going to start %s" % node

# stop tasktracker/datanode service on remote host(s)
def stopService(service, nodelist):
    for node in nodelist:
        if verbose:
            print "Going to stop %s" % node

# refresh dfs/mradmin services
def refreshService(service):
    
    (retval, output) = commands.getstatusoutput("%s/bin/hadoop %s -refreshNodes" % (HADOOP_HOME, service))
    #retval = commands.getstatus("/usr/bin/uptime")
    #(retval, output) = commands.getstatusoutput("/usr/bin/uptime")
    
    if verbose:
        print "refresh status: ", retval
    
    if retval <> 0:
        return False
    
    if verbose:
        print "services refreshed!"
        
    return True

# checks if Namenode
def masterOrNamenode():
    
    # Make sure we are the master NameNode, otherwise bail.
    
    (jpsoutput) = commands.getoutput("/usr/java/default/bin/jps")
    #(jpsoutput) = commands.getoutput("/usr/java/default/bin/java")

    if not re.search("[0-9]+\sNameNode", jpsoutput):
    #if not re.search("^Usage: java", jpsoutput):
        return False
    
    return True

# Validating correct hadoop configs are in place
def validateHadoopConfigs(hadoopHomeEnvVar):
    # Do we have the correct files and Hadoop proper properties set to even do this?
    
    # These are the config files and options we need:
    #
    # hdfs-site.xml    ->  dfs.hosts.exclude        -> disallow hosts from being DFS clients
    # hdfs-site.xml    ->  dfs.hosts                -> allow hosts to be DFS clients
    # mapred-site.xml  ->  mapred.hosts.exclude     -> disallow hosts from map-reduce
    # slaves           ->  none                     -> Allowed to be a participating Hadoop node
    
    # hard-coded dicts
    hadoopConfigFiles = { 'hdfs-site.xml': ['dfs.hosts.exclude','dfs.hosts'],
                      'mapred-site.xml': ['mapred.hosts.exclude', 'mapred.hosts'],
                      'slaves': ''
    }
    
    for configFile, propNames in hadoopConfigFiles.iteritems():
     
        fullConfigFile = "%s/conf/%s" % (hadoopHomeEnvVar, configFile)
        
        propCounter = 0
        
        if os.path.isfile(fullConfigFile):
            
            if re.match("^.*.xml", configFile):
            
                if verbose:
                    print "analyzing", configFile
            
                for propName in propNames:
                
                    tree = xml.parse(fullConfigFile)
    
                    configfile = tree.getroot()
    
                    for prop in configfile.findall('property'):
                        name = prop.find('name').text
        
                        if name == propName:
                            propCounter = propCounter + 1
                            
                            hadoopConfigs[name] = prop.find('value').text
            
                if len(propNames) <> propCounter:
                    propNamesstr = ', '.join(propNames)
                    print "\n Error: Ensure that %s are defined in '%s'\n\n" % (propNamesstr, configFile)
                    sys.exit(err_code['err_valconf'])
            else:
                hadoopConfigs[configFile] = fullConfigFile
        else:
            
            print "\n Error: The needed configuration file,", configFile, ", does not exist or cannot be found.  Check 'HADOOP_HOME' and/or 'HADOOP_PREFIX' and re-run.\n\n"
            sys.exit(err_code['err_valconf'])

################################################################################################
#        START OF APPLICATION - DO NOT EDIT BELOW UNLESS YOU KNOW WHAT YOU ARE DOING
################################################################################################

def main():

    ###============================
    ### Preliminary checking
    ###============================
    
    global HADOOP_HOME
    
    # Make sure we have HADOOP_HOME set, otherwise bail.
    if os.environ.has_key("HADOOP_HOME"):
        HADOOP_HOME = os.environ.get("HADOOP_HOME")
    elif os.environ.has_key("HADOOP_PREFIX"):
        HADOOP_HOME = os.environ.get("HADOOP_PREFIX")
    else:
        sys.stderr.write("\n Did not find 'HADOOP_HOME' or 'HADOOP_PREFIX' env var.  Set/export HADOOP_HOME and/or HADOOP_PREFIX re-run, please.\n\n")
        sys.exit(err_code['err_prelim'])
    
    # Make sure we are a master or namenode
    if not masterOrNamenode():
        sys.stderr.write("\n Uhh, you're not running this on the Hadoop NameNode.  Shame on thou!  Try again.\n\n")
        sys.exit(err_code['err_prelim'])

    # Let's simply validate we have all the options set in proper hadoop configs
    # to continue on, otherwise bail.
    validateHadoopConfigs(HADOOP_HOME)   
    
    ###============================
    ### Set up option handling
    ###============================
    parser = argparse.ArgumentParser()
    
    # Option(s) for: mapreducehostsexclude, dfshostsexclude, slaves and verbose (suppressed)
    parser.add_argument("-m", "--mapreduce", action="store", nargs=1, dest="mraction", choices=['add','remove','refresh'], help="Action command will affect map/reduce exclude's conf file (e.g. [add|remove|refresh])")
    parser.add_argument("-d", "--dfs", action="store", nargs=1, dest="dfsaction", choices=['add','remove','refresh'], help="Action command will affect dfs exclude's conf file (e.g. [add|remove|refresh])")
    parser.add_argument("-s", "--slaves", action="store", nargs=1, dest="slavesaction", choices=['add','remove'], help="Action command will affect slaves conf file (e.g. [add|remove|refresh])")
    parser.add_argument("-v", "--verbose", action='store_true', dest="verbose", default=False, help=argparse.SUPPRESS)
    
    # Option(s) for: list of nodes/hosts to apply action and config change to
    parser.add_argument("-n", "--nodes", action="store", dest="nodes", help="List of comma separated nodes/hosts to apply action and configuration change to")
    
    # Parse those options!
    args = parser.parse_args()

    # If nothing is passed, print argparse help at a minimum
    if len(sys.argv) - 1 == 0:
        parser.print_help()
        sys.exit(1)

    # Set verbose status if we made it this far
    global verbose
    
    verbose = args.verbose

    ###============================
    ### Start doing Hadoop actions
    ###============================
    
    if verbose:
        print "args are: ", args

    retval = performActions(args)

if __name__ == '__main__':
    main()