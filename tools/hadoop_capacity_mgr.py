#!/usr/bin/env python
#
#
#
# Name: hadoop_capacity_mgr.py
#
# Description: Manages Capacity Scheduler configuration for Hadoop
#
# Author: Adam Dosch
#
# Date: 05-08-2013
#
################################################################################################
# Change    Date            Author              Description
################################################################################################
#  001      05-08-2013      Adam Dosch          Initial Release
#  002      09-24-2013      Adam Dosch          Working on completing capacity adjustment logic
#
################################################################################################

import sys
import os
import commands
import re
import argparse
import xml.etree.ElementTree as xml
import math

err_code = { 'err_prelim': 2,
             'err_valconf': 3,
             'err_general': 1
}

HADOOP_HOME = ""

verbose = False

hadoopConfigs = {}
  
# checks if Namenode
def masterOrNamenode():
    
    # Make sure we are the master NameNode, otherwise bail.
    
    #(jpsoutput) = commands.getoutput("/usr/java/default/bin/jps")
    (jpsoutput) = commands.getoutput("/usr/java/default/bin/java")

    #if not re.search("^[0-9]+ NameNode", jpsoutput):
    if not re.search("^Usage: java", jpsoutput):
        return False
    
    return True

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
                if not refreshService("mradmin"):
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
                if not refreshService("dfsadmin"):
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

# refresh dfs/mradmin services
def refreshService(service):
    
    #retval = commands.getstatus(HADOOP_HOME + "/bin/hadoop " + service + " -refreshQueues")
    #retval = commands.getstatus("/usr/bin/uptime")
    (retval, output) = commands.getstatusoutput("/usr/bin/uptime")
    
    if verbose:
        print "refresh status: ", retval
    
    if retval <> 0:
        return False
    
    if verbose:
        print "services refreshed!"
        
    return True

# Loading up Capacity Scheduler values into dictionary
def loadCapValues():
    
    if verbose:
        print "Entering loadCapValues()"
    
    try:
        tree = xml.parse(hadoopConfigs['mapred.capacitytaskscheduler.allocation.file'])

        configfile = tree.getroot()
        
    except Exception, e:
        print "Error: ", e
        sys.exit(err_code['err_prelim'])
    
    for prop in configfile.findall('property'):
        
        name = prop.find('name').text        
        
        if verbose:
            print "analyzing ", name
        
        if re.search("mapred\.capacity\-scheduler\.queue\.\S+\.\S+", name):
            
            if name.split(".")[3] in hadoopConfigs['mapred.queue.names']:
                if verbose:
                    print "  - Matched queue, ", name.split(".")[3], ", get all queue properties"
                    print "    |__ Adding ", name
                    
                hadoopConfigs[name] = prop.find('value').text
    
# Gettting Queue Capacity Percentage
def getQueueCapacity(queueName="all"):
    
    if verbose:
        print "Entering getQueueCapacity()"
    
    try:
        tree = xml.parse(hadoopConfigs['mapred.capacitytaskscheduler.allocation.file'])

        configfile = tree.getroot()
    except Exception, e:
        print "Error: ", e
        sys.exit(err_code['err_general'])

    capTotal = 0

    for prop in configfile.findall('property'):
        name = prop.find('name').text
        
        if verbose:
            print "analyzing ", name
        
        if re.match("^mapred\.capacity-scheduler\.queue\.\S+\.capacity$", name):
        
            if verbose:
                print "   - matched ", name
            
            if queueName == "all":
                
                if name.split(".")[3] in hadoopConfigs['mapred.queue.names']:
                    if verbose:
                        print "      |_ matched active queue,", name.split(".")[3], ", add that bitch up: ", prop.find('value').text, "to ", capTotal
                        
                    capTotal = capTotal + int(prop.find('value').text)
            else:
                
                #if name.split(".")[3] == queueName:
                if name.split(".")[3] in queueName:
                    if verbose:
                        print "      |_ matched active queue,", name.split(".")[3], ", adding ", prop.find('value').text, "to ", capTotal
                        
            
                    capTotal = capTotal + int(prop.find('value').text)
    
    return capTotal

# Validating correct hadoop configs are in place
def validateHadoopConfigs(hadoopHomeEnvVar):
    # Do we have the correct files and Hadoop proper properties set to even do this?
    
    # We need to validate we have the capacity-scheduler enabled in this Hadoop environment:
    #
    # mapred-site.xml:
    #
    #   mapred.jobtracker.taskScheduler == org.apache.hadoop.mapred.CapacityTaskScheduler
    #
    
    # Get the location of the capacity-scheduler.xml config file from mapred-site.xml:
    #
    #   mapred.capacitytaskscheduler.allocation.file:  HADOOP_HOME/conf/capacity-scheduler.xml


    # These are the config files and options we need:
    #
    # capacity-scheduler.xml:
    #
    #   mapred.capacity-scheduler.queue.<queuename>.capacity
    #   mapred.capacity-scheduler.queue.<queuename>,maximum-capacity
    #   mapred.capacity-scheduler.queue.<queuename>.supports-priority
    #   mapred.capacity-scheduler.queue.<queuename>.minimum-user-limit-percent
    #   mapred.capacity-scheduler.queue.<queuename>.user-limit-factor
    #   mapred.capacity-scheduler.queue.<queuename>.maximum-initialized-active-tasks
    #   mapred.capacity-scheduler.queue.<queuename>.maximum-initialized-active-tasks-per-user
    #   mapred.capacity-scheduler.queue.<queuename>.init-accept-jobs-factor
    #
    
    # hard-coded dicts
    hadoopConfigFiles = {
        'mapred-site.xml': ['mapred.jobtracker.taskScheduler', 'mapred.capacitytaskscheduler.allocation.file', 'mapred.queue.names'],
        'capacity-scheduler.xml':[]
    }
    
    for configFile, propNames in hadoopConfigFiles.iteritems():
     
        fullConfigFile = "%s/conf/%s" % (hadoopHomeEnvVar, configFile)
        
        propCounter = 0
        
        if os.path.isfile(fullConfigFile):
            
            # If it's an XML configuration file, process accordingly
            if re.match("^.*.xml$", configFile):
            
                if verbose:
                    print "analyzing", configFile

                tree = xml.parse(fullConfigFile)
    
                configfile = tree.getroot()
                    
                # Loop through all property names and try and find them in XML configuration
                for propName in propNames:

                    for prop in configfile.findall('property'):
                        
                        name = prop.find('name').text
                        
                        if name == propName:
                            propCounter = propCounter + 1
                            
                            # Interrogate taskScheduler and validate we are setup for Capacity Scheduler
                            if name == "mapred.jobtracker.taskScheduler":
                                if not re.search("CapacityTaskScheduler", prop.find('value').text):
                                    print "\nError: Hadoop 'mapred.jobtracker.taskScheduler' property must be set up to use Capacity Scheduling, not '%s'\n\n" % (prop.find('value').text)
                                    sys.exit(err_code['err_valconf'])
                            # Put mapred.queue.names in list (comma-separated)
                            elif name == "mapred.queue.names":
                                hadoopConfigs[name] = list(str(prop.find('value').text).split(","))
                            else:
                                hadoopConfigs[name] = prop.find('value').text
            
                # Did we account for every Hadoop configuration we were looking for?
                if len(propNames) <> propCounter:
                    propNamesstr = ', '.join(propNames)
                    print "\n Error: Ensure that %s are defined in '%s'\n\n" % (propNamesstr, configFile)
                    sys.exit(err_code['err_valconf'])
            else:
                hadoopConfigs[configFile] = fullConfigFile
        else:
            
            print "\n Error: The needed configuration file,", configFile, ", does not exist or cannot be found.  Check 'HADOOP_HOME' and re-run.\n\n"
            sys.exit(err_code['err_valconf'])

def printCapacitySummary(listOfQueues, slots):
    currentCapacity = getQueueCapacity(queueName=listOfQueues)  
    
    print "-" * 80
    print
    print " Current defined map slot capacity: %s" % slots
    print
    print " Total queue capacity: %s%%" % currentCapacity
    print
    
    for [curqueue,curcap],[maxqueue,maxcap] in zip(queueCapPercentageToSlotCount(listOfQueues).iteritems(), queueCapPercentageToSlotCount(listOfQueues, captype="maximum").iteritems()):
        print "   - Capacity for '%s': %s%%" % (curqueue, curcap)
        print "   - Slot capacity for '%s': %s" % (curqueue, float(float(slots) * (float(curcap)/float(100))))
        print "   - Max Capacity for '%s': %s%%" % (maxqueue, maxcap)
        print "   - Max slot capacity for '%s': %s" % (maxqueue, float(float(slots) * (float(maxcap)/float(100))))
        print
    
    print "-" * 80
    print


def performSelection(headingMsg, selectionMsg, selectionOptions, inputType=int):
    answer = False
    selection = False
    
    print " %s: " % headingMsg
    print
    
    for n, option in enumerate(selectionOptions):
        print "   %s) %s" % (n + 1, option)
    
    print
    
    try:
        while answer <> True:
            selection = raw_input("%s: " % selectionMsg)
            
            try:
                if inputType is int:
                    if int(selection) in range(1, len(selectionOptions) + 1, 1):
                        answer = True
                
                if inputType is str:
                    if str(selection) in validValues:
                        answer = True
                
            except ValueError, e:
                continue
                
    except KeyboardInterrupt, e:
        return False
    
    return selection

def queueCapPercentageToSlotCount(listOfQueues, captype="defined"):
    """
    Returns capacity queue slot count from calculated percentage.  Can have a 
    captype of 'defined' (e.g. capacity) or 'maximum' (e.g. maximum-capacity) to return
    
    """
    queueSlots = {}
    
    if captype == "maximum":  
        capacity = "maximum-capacity"
    else:
        capacity = "capacity"
    
    for queue in listOfQueues:
        queueSlots[queue] = hadoopConfigs["mapred.capacity-scheduler.queue." + queue + "." + capacity]
    
    return queueSlots

################################################################################################
#        START OF APPLICATION - DO NOT EDIT BELOW UNLESS YOU KNOW WHAT YOU ARE DOING
################################################################################################

def main():

    ###============================
    ### Preliminary checking
    ###============================
    
    # Make sure we have HADOOP_HOME set, otherwise bail.
    if os.environ.has_key("HADOOP_HOME"):
        HADOOP_HOME = os.environ.get("HADOOP_HOME")
    else:
        sys.stderr.write("\n Did not find 'HADOOP_HOME' env var.  Set/export HADOOP_HOME and re-run, please.\n\n")
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
    parser.add_argument("-s", "--slots", action="store", dest="slots", type=int, help="Define the number of cluster slots on run-time to calculate queue capacity.")
    #parser.add_argument("-m", "--maximum-capacity", action="store", nargs=1, dest="max-cap", choices=['add','remove','refresh'], help="Action command will affect map/reduce exclude's conf file (e.g. [add|remove|refresh])")
    #parser.add_argument("-d", "--dfs", action="store", nargs=1, dest="dfsaction", choices=['add','remove','refresh'], help="Action command will affect dfs exclude's conf file (e.g. [add|remove|refresh])")
    #parser.add_argument("-s", "--slaves", action="store", nargs=1, dest="slavesaction", choices=['add','remove'], help="Action command will affect slaves conf file (e.g. [add|remove|refresh])")
    parser.add_argument("-v", "--verbose", action='store_true', dest="verbose", default=False, help=argparse.SUPPRESS)
    
    # Option(s) for: list of nodes/hosts to apply action and config change to
    #parser.add_argument("-n", "--nodes", action="store", dest="nodes", help="List of comma separated nodes/hosts to apply action and configuration change to")
    parser.add_argument("-p", "--print", action="store", dest="nodes", help="Prints out current Hadoop cluster queues capacities")
    # Parse those options!
    args = parser.parse_args()

    # If nothing is passed, print argparse help at a minimum
    #if len(sys.argv) - 1 == 0:
    #    parser.print_help()
    #    sys.exit(1)

    # Set verbose status if we made it this far
    global verbose

    verbose = args.verbose
    
    if verbose:
        for k,v in hadoopConfigs.iteritems():
            print k, "-->", v    

    # Mandatory option checking here
    if not args.slots:
        parser.print_help()
        sys.exit(1)


    # Load currently 'enabled' capacity queues from configuration
    loadCapValues()

    # Print out current Capacity Configuration
    printCapacitySummary(hadoopConfigs['mapred.queue.names'], args.slots)
    
    # Setup selection menu for editing queue
    selection = performSelection("Current configured/active queue(s)", "Select queue to edit", hadoopConfigs['mapred.queue.names'])
    
    if selection:
        print selection

if __name__ == '__main__':
    main()