#!/usr/bin/env python
from abc import ABCMeta, abstractmethod, abstractproperty
import time
import subprocess
import os
import tarfile
import shutil
import xmlrpclib
import urllib2
from subprocess import *
import paramiko
import socket
import unittest
import sys
from osgeo import gdal
import numpy as np
from cStringIO import StringIO
import gc

#===============================================================================
#Utility Classes 
#===============================================================================


class Logger(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def getLogFile(self):
        pass
    
    @abstractmethod
    def log(self, message):
        pass

################################################################################

class LocalLogger(Logger):
    '''Implements logging for espa'''

    logfilename = None

    def __init__(self, context = None):
        super(LocalLogger,self).__init__()
        if context is None or not context.has_key('log.file.name'):
            self.logfilename = ("%s_%s.log") % (context['create_as_collection'], context['scene.id'])
            #self.logfilename = '/tmp/espacollections.log'
        else:
            #assume it's going into the working directory
            self.logfilename = context['log.file.path'] + context['log.file.name']
                
    def getLogFile(self):
        super(LocalLogger, self).getLogFile()
        f = open(self.logfilename, 'a')
        return f

    def log(self,message):
        super(LocalLogger, self).log(message)
        f = self.getLogFile()
        f.write(message + '\n')
        f.close()

################################################################################

class XmlRpcLogger(Logger):
    
    xmlrpcurl = None
    
    def __init__(self, xmlrpcurl):
        pass
    
    def getLogFile(self):
        pass
    
    def log(self, message):
        pass

################################################################################

class Utilities(object):
    
    logger = None
    
    def __init__(self,logger):
        self.logger = logger
    
           
     #recursively removes zeros off the supplied string and returns the cleansed value
    def stripZeros(self, value):
        returnval = None
        val = value
        
        #print ("Checking for starting 0 on:%s") %(val)
    
        if val.startswith('0'):
            returnval = val[1:len(val)]
            if returnval.startswith('0'):
                returnval = self.stripZeros(returnval)
        else:
            returnval = value

        return returnval
    
    #Cooresponding path for this scene
    def getPath(self, scene_name):
        return scene_name[3:6]

    #Corresponding row for this scene
    def getRow(self, scene_name):
        return scene_name[6:9]

    #Scene collection year
    def getYear(self, scene_name):
        return scene_name[9:13]

    #Scene collection julian date
    def getDate(self, scene_name):
        return scene_name[13:16]

    #Scene sensor (L5-TM,L7-SLC-ON,L7-SLC-OFF).  LT5 means Landsat 5,
    #Have to split L7 because of the date the SLC broke (May 30,2003 is SLC on
    # while May 31,2003 is SLC off)
    def getSensor(self, scene_name):
        if scene_name[0:3] =='LT5':
            return 'tm'
        elif scene_name[0:3] == 'LE7':
            return 'etm'
            
    #returns the station this scene was acquired from
    def getStation(self, scene_name):
        return scene_name[16:21]
        
    def findConfiguration(self, context, key):
        '''Utility method to search the local context first then the remote config repository'''
        value = None
        errmsg = 'Keys[%s] not found in local or remote configurations' % key
        
        try:
            if context.has_key(key):
                value = self.context[key]
            elif ServerProxy(context,self.logger).getConfiguration(key):
                value = ServerProxy(context,self.logger).getConfiguration(key)
            else:
                self.logger.log.log(errmsg)
        except:
            self.logger.log(errmsg)
        
        return value
    
     #Utility to remove .tar.gz from end of scene name if present
    def strip_suffix(self):
        return self.name.split('.tar.gz')[0]
    
################################################################################
#DataSources for getting/storing data

class DataSource(object):
    __metaclass__ = ABCMeta
    
    context = None
    logger = None
    
    def __init__(self,context, logger):
        self.context = context
        self.logger = logger

        
    @abstractmethod
    def getDataSourcePath(self, sceneid):
        '''The path where this scene should be found for the given data source'''
        pass
    
    @abstractmethod
    def isAvailable(self, sceneid):
        '''Is the named scene available at this datasource'''
        pass
    
    @abstractmethod
    def buildDataSourcePath(self, sceneid):
        pass
    
    @abstractmethod
    def get(self, sceneid, localpath):
        '''Retrieve the target scene and place it on local storage'''
        pass
    
    @abstractmethod
    def put(self, sceneid, targetFileName, localpath):
        '''Move the local scene to the datasource'''
        pass
    
    @abstractmethod
    def delete(self, sceneid, localpath):
        '''Delete the file from the datasource'''
        pass
    
    
################################################################################

class SFTPDataSource(DataSource):
    username = None
    password = None
    host = None
    port = None
    
    def __init__(self, logger, context, username, password, host, port):
        super(SFTPDataSource, self).__init__(context,logger)
        self.username = username
        self.password = password
        self.host = host
        self.port = port
    '''       
    def getDataSource(self, name, logger, context, username, password, host, port):
       
        mod = sys.modules[__name__]
        class_ = getattr(mod, name)
        instance = class_(logger, context, username, password, host, port)
        return instance
    '''    
    def buildSFTPPath(self, path):
         #verify/build the target path on sftp    
            sftp = None
            transport = None
            try:
                sftp,transport = self.getSFTPClient()
                parts = path.split('/')
                buildpath = ''
                for p in parts:
                    buildpath = buildpath + '/' + p
                    try:
                        dummy = sftp.listdir(buildpath)
                    except:
                        sftp.mkdir(buildpath)
                return buildpath
            finally:
                if sftp is not None:
                    sftp.close()
                    sftp = None
                if transport is not None:
                    transport.close()
                    transport = None
    
    
    def getSFTPClient(self):               
        transport = paramiko.Transport((self.host,int(self.port)))
        transport.connect(username = self.username, password = self.password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp,transport   #NEED TO RETURN BOTH OF THESE SO THE CALLER CAN WIPE OUT THE TRANSPORT TOO
    
    def listsftpdir(self, remotepath):
        '''lists all the files in the named remotepath, returns None if empty or path doesn't exist'''
        
        sftp = None
        transport = None
        contents = None
        
        try:
            sftp,transport = self.__getSFTPClient()
            try:
                contents = sftp.listdir(str(remotepath))
            except:
                self.logger.log(("Could not list directory[%s] on host[%s]") % (remotepath, self.host))
                contents = None
            return contents
        finally:
            if sftp is not None:
                sftp.close()
            if transport is not None:
                transport.close()
                
    def sftptransfer(self, targetPath, sourceFile, operation):
        '''utility to transfer files via sftp'''
        sftp = None
        transport = None
        
        try:
            sftp,transport = self.getSFTPClient()
            
            if operation == 'GET':
                sftp.get(sourceFile, targetPath)
            elif operation == 'PUT':
                sftp.put(sourceFile, targetPath)
            else:
                msg = ("Incorrect value[%s] supplied to 'operation' parameter... valid args are GET or PUT") % (operation)
                self.logger.log(msg)         
                raise ValueError(msg)
            return targetPath                                                             
        except:
            self.logger.log("... Error transferring %s \n\t  to %s" % (sourceFile,targetPath))
            #self.logger.log("... %s" % errmsg)
            return ''
        finally:
            if sftp is not None:
                sftp.close()
            if transport is not None:
                transport.close()

################################################################################

class ReadOnlySFTPDataSource(SFTPDataSource):
    
    def buildDataSourcePath(self, sceneid):
        '''Not implemented as policy does not allow writing to this datasource'''
        raise NotImplementedError("Build path is not available on read-only datasource")
        
    def put(self, sceneid, targetFileName, localpath):
        '''Not implemented as policy does not allow writing to this datasource'''
        raise NotImplementedError("Put is not available on read-only datasource")
    
    def delete(self, sceneid, localpath):
        '''Not implemented as policy does not allow writing to this datasource'''
        raise NotImplementedError("Delete is not available on read-only datasource")
        



################################################################################

class LandsatDataSource(ReadOnlySFTPDataSource):
    '''Datasource that wraps the sftp server that hosts all the landsat data resident
    on the online cache'''
    
    def getDataSourcePath(self, sceneid):        
        u = Utilities(self.logger)
        path = u.getPath(sceneid)
        path = u.stripZeros(path)
        
        row = u.getRow(sceneid)
        row = u.stripZeros(row)
        
        sensor = u.getSensor(sceneid)
        year = u.getYear(sceneid)
        
        data_path = ("/data/standard_l1t/%s/%s/%s/%s") % (sensor, path, row, year)
        
        return data_path
    
    def isAvailable(self, sceneid):
        dir = self.getDataSourcePath(sceneid)
        contents = self.listsftpdir(dir)
        
        if contents is not None and len(contents) > 0:
            try:
                #this will throw exception if not found
                contents.index(sceneid + '.tar.gz')
                return True
            except:
                #not found
                return False
        else:
            #nothing in directory
            return False
                    
    def get(self, sceneid, localpath):
        sourceFile = ("%s/%s.tar.gz") % (self.getDataSourcePath(sceneid), sceneid)
        return self.sftptransfer(localpath, sourceFile, 'GET')
    
################################################################################

class NLAPSDataSource(LandsatDataSource):
    
    def getDataSourcePath(self, sceneid):
        path = None
        row = None
        year = None
        
        u = Utilities(self.logger)
        path = u.getPath(sceneid) 
        path = u.stripZeros(path)
        
        row = u.getRow(sceneid)
        row = u.stripZeros(row)
        
        year = u.getYear(sceneid)
        
        data_path = ("/data/standard_l1t/nlaps/tm/%s/%s/%s") % (path, row, year)
        
        return data_path
        
        
################################################################################

class DistributionDataSource(SFTPDataSource):
    '''Looks for a context parameter 'create_as_collection' and if it is set to
    true then it will place the file in a separate data path under 'collections',
    with the collecion name set to chain.name    
    '''
    
    def getDataSourcePath(self, sceneid):
    
        path = None
        row = None
        sensor = None
        year = None
        data_path = None
        
        u = Utilities(self.logger)
        path = u.getPath(sceneid)
        path = u.stripZeros(path)
        
        row = u.getRow(sceneid)
        row = u.stripZeros(row)
        
        sensor = u.getSensor(sceneid)
        year = u.getYear(sceneid)
        
        chain = self.context['chain.name']
        
        if self.context.has_key('create_as_collection') and \
            self.context['create_as_collection'] != '':
            
            collection = self.context['create_as_collection']
            
            data_path = ("/data1/espa/collections/%s/%s/%s/%s/%s/%s") % (collection,chain,sensor,path,row,year)
        else:
            #this should also have 'ondemand' right after the espa dir but we'll break all the existing
            #links if we do that
            data_path = ("/data1/espa/%s/%s/%s/%s/%s") % (chain,sensor,path,row,year)
        
        return data_path
       
    
    def isAvailable(self,sceneid):
        dir = self.getDataSourcePath(sceneid)
        contents = self.listsftpdir(dir)
        
        if contents is not None and len(contents) > 0:
            try:
                #this will throw exception if not found
                contents.index(sceneid + '.tar.gz')
                return True
            except:
                #not found
                return False
        else:
            #nothing in directory
            return False
    
    def buildDataSourcePath(self, sceneid):        
       return self.buildSFTPPath(self.getDataSourcePath(sceneid))
    
    def get(self, sceneid, localpath):
        remoteFile = ("%s/%s.tar.gz") % (self.getDataSourcePath(sceneid), sceneid)
        return self.sftptransfer(targetPath = localpath, sourceFile = remoteFile, operation = 'GET')
    
    def put(self, sceneid, targetFileName, localpath):
        targetPath = self.buildDataSourcePath(sceneid)
        targetPath = ("%s/%s") % (targetPath, targetFileName)
        #targetPath = ("%s/%s.tar.gz") % (self.getDataSourcePath(sceneid), sceneid)
        return self.sftptransfer(targetPath = targetPath, sourceFile =  localpath, operation = 'PUT')
       
    def delete(self, sceneid, remotepath):
        pass


################################################################################

class GeoSolrDataSource(SFTPDataSource):
    
    def getDataSourcePath(self, sceneid):
        
        collection = self.context['create_as_collection']       
        chain = self.context['chain.name']                    
        data_path = ("/data/espa-browse/incoming/%s") % (collection)
        return data_path
       
    
    def isAvailable(self,sceneid):
        dir = self.getDataSourcePath(sceneid)
        contents = self.listsftpdir(dir)
        
        if contents is not None and len(contents) > 0:
            try:
                #this will throw exception if not found
                contents.index(sceneid + '.tar.gz')
                return True
            except:
                #not found
                return False
        else:
            #nothing in directory
            return False
    
    def buildDataSourcePath(self, sceneid):        
       return self.buildSFTPPath(self.getDataSourcePath(sceneid))
    
    def get(self, sceneid, localpath):
        pass
    
    def put(self, sceneid, targetFileName, localpath):
        targetPath = self.buildDataSourcePath(sceneid)
        targetPath = ("%s/%s") % (targetPath, targetFileName)
        return self.sftptransfer(targetPath = targetPath, sourceFile =  localpath, operation = 'PUT')
       
    def delete(self, sceneid, remotepath):
        pass
    
################################################################################

class GLSDataSource(LandsatDataSource):
    
    def getDataSourcePath(self, sceneid):
        path = None
        row = None
        year = None
        
        u = Utilities(self.logger)
        path = u.getPath(sceneid)
        row = u.getRow(sceneid)
                
        gls_year = self.context['gls.year']
                
        #/home/espa/gls/gls_2010/001/053/LE70010532009330EDC00.tar.gz
        data_path = ("/home/espa/gls/gls_%s/%s/%s") % (gls_year,path,row)
        
        return data_path
    
################################################################################

class NCEPDataSource(DataSource):
    
    def getDataSourcePath(self, sceneid):
        pass
    
    def isAvailable(self, sceneid):
        pass
    
    def buildDataSourcePath(self, sceneid):
        '''Not implemented as policy does not allow writing to this datasource'''
        raise NotImplementedError("Build path is not available on the NCEP datasource")
    
    def get(self, sceneid, localpath):
        pass
    
    def put(self, sceneid, localpath):
        '''Not implemented as policy does not allow writing to this datasource'''
        raise NotImplementedError("Put is not available on the NCEP datasource")
    
    def delete(self, sceneid, localpath):
        '''Not implemented as policy does not allow writing to this datasource'''
        raise NotImplementedError("Delete is not available on the NCEP datasource")
    
    def getUsername(self):
        username = None
        
        override_key = self.context['%s.ncepds.username'] % self.context['chain.name']
        global_key = self.context['ncepds.username']
        
        username = Utilities(self.logger).findConfiguration(context, override_key)
        if username is None:
            username = Utilities(self.logger).findConfiguration(context, global_key)
                
        return username
    
    def getPassword(self):
        password = None
        override_key = self.context['%s.ncepds.password'] % self.context['chain.name']
        global_key = self.context['ncepds.password']
        
        password = Utilities(self.logger).findConfiguration(self.context, override_key)
        if password is None:
            password = Utilities(self.logger).findConfiguration(self.context, global_key)
        
        return password
    
    def getHost(self):
        host = None
        override_key = self.context['%s.ncepds.host'] % self.context['chain.name']
        global_key = self.context['ncepds.host']
        
        host = Utilities(self.logger).findConfiguration(self.context, override_key)
        if host is None:
            host = Utilities(self.logger).findConfiguration(self.context, global_key)
        
        return host
    
    def getPort(self):
        port = None
        override_key = self.context['%s.ncepds.port'] % self.context['chain.name']
        global_key = self.context['ncepds.port']
        
        port = Utilities(self.logger).findConfiguration(self.context, override_key)
        if port is None:
            port = Utilities(self.logger).findConfiguration(self.context, global_key)
        
        return port
    
################################################################################

class TOMSDataSource(DataSource):
    
    def getDataSourcePath(self, sceneid):
        pass
      
    def isAvailable(self, sceneid):
        pass
    
    def buildDataSourcePath(self, sceneid):
        '''Not implemented as policy does not allow writing to this datasource'''
        raise NotImplementedError("Build path is not available on the TOMS datasource")
    
    def get(self, sceneid, localpath):
        pass
    
    def put(self, sceneid, localpath):
        '''Not implemented as policy does not allow writing to this datasource'''
        raise NotImplementedError("Put is not available on the TOMS datasource")
    
    def delete(self, sceneid, localpath):
        '''Not implemented as policy does not allow writing to this datasource'''
        raise NotImplementedError("Delete is not available on the TOMS datasource")
    
    def getUsername(self):
        username = None
        
        override_key = self.context['%s.tomsds.username'] % self.context['chain.name']
        global_key = self.context['tomsds.username']
        
        username = Utilities(self.logger).findConfiguration(context, override_key)
        if username is None:
            username = Utilities(self.logger).findConfiguration(context, global_key)
                
        return username
    
    def getPassword(self):
        password = None
        override_key = self.context['%s.tomsds.password'] % self.context['chain.name']
        global_key = self.context['tomsds.password']
        
        password = Utilities(self.logger).findConfiguration(self.context, override_key)
        if password is None:
            password = Utilities(self.logger).findConfiguration(self.context, global_key)
        
        return password
    
    def getHost(self):
        host = None
        override_key = self.context['%s.tomsds.host'] % self.context['chain.name']
        global_key = self.context['tomsds.host']
        
        host = Utilities(self.logger).findConfiguration(self.context, override_key)
        if host is None:
            host = Utilities(self.logger).findConfiguration(self.context, global_key)
        
        return host
    
    def getPort(self):
        port = None
        override_key = self.context['%s.tomsds.port'] % self.context['chain.name']
        global_key = self.context['tomsds.port']
        
        port = Utilities(self.logger).findConfiguration(self.context, override_key)
        if port is None:
            port = Utilities(self.logger).findConfiguration(self.context, global_key)
        
        return port
    
################################################################################

class ServerProxy(object):
    '''Utility class to report status of chain execution to an xmlrpc server'''
    server = None
    context = None
    logger = None
        
    def __init__(self, context,logger):
        self.context = context
        self.logger = logger
        url = context['xmlrpcurl']
        #check to see if we have a url try/catch
        self.server = xmlrpclib.ServerProxy(url)
        #check to see if we could contact server. try/catch

    #MIGRATE THIS DOWN TO UPDATESTATUS()  
    def markSceneComplete(self):
        response = ''
        try:
            source_l1t = 'not_available'
            if self.context.has_key('source.l1t.distro.location') and self.context['source.l1t.distro.location'] != None:
                source_l1t = self.context['source.l1t.distro.location']
                
            response = self.server.markSceneComplete(self.context['scene.id'],
                                                     self.context['chain.name'],
                                                     self.context['completed.scene.location'],
                                                     source_l1t,
                                                     xmlrpclib.Binary(self.context['scene.log.file']))
        except Exception, err:
            self.logger.log("... Error marking scene complete:%s" % err)
        return response

    #Valid statuses are:
    # On Order
    # On Cache
    # Queued
    # Staging
    # Processing
    # Distributing
    # Complete
    # Purged
    # Error
    
    #this method is bad... needs much more robust error and condition checking in the if statement
    def updateStatus(self):
        hostname = socket.gethostname()
        
        try:
            if self.context['scene.status'] == 'Complete':
                response = self.server.markSceneComplete()
                
            elif self.context['scene.status'] == 'Error':
                response = self.server.setSceneError(hostname,
                                                     self.context['error'],
                                                     self.context['scene.id'],
                                                     self.context['chain.name'])
            else:
                response = self.server.updateStatus(hostname,
                                                    self.context['scene.id'],
                                                    self.context['chain.name'],
                                                    self.context['scene.status'])
            return response
        except Exception, err:
            self.logger.log("... Error updating scene status:%s" % err)
            return err

    def getConfiguration(self,key):
        response = ''
        try:
            response = self.server.getConfiguration(key)
        except Exception, err:
           self.logger.log("... Error getting configuration key:%s" % err)
                                
        return response

################################################################################
#===============================================================================
# Command and Chain Section -- TODO: Pull out this out and use pychained instead
#===============================================================================
################################################################################

class Command(object):
    '''
    Abstract base class for command implementations to subclass
    '''
    __metaclass__ = ABCMeta

    name = 'Default'
    logger = None
    
    def __init__(self, logger):
        self.logger = logger

    @abstractmethod
    def expectedParameters(self):
        pass
    
    @abstractmethod
    def createsParameters(self):
        pass

    @abstractmethod
    def execute(self, context):
        '''Abstract execute method that will be called on all Commands

        Keyword arguments:
        context -- A python dictionary (key -> value) that contains
               all the variables in use by the Chain of Commands
        '''
        print "--- Context executing ---"

################################################################################

class Chain(Command):
    '''Base implementation of a Chain to allow us to string commands together.
    The Chain class subclasses Command so that we can construct a Chain
    of Chains if necessary.
    '''
    __commands = []     
    
    def __init__(self, logger,name='Default'):
         super(Chain,self).__init__(logger)
         self.name = name
         self.__commands = []
         
    def expectedParameters():
        return None
    
    def createsParameters():
        return None
    
    def addCommand(self, Command):
        """Method to add a new Command to the end of the Chain

        Keyword arguments:
        Command -- A Command to be appended to the end of the Chain
        """
        self.__commands.append(Command)

    def execute(self, context):        
        self.logger.log("--- Chain[%s] ---" % self.name)
        
        for c in range(len(self.__commands)):
            returnval =  self.__commands[c].execute(context)

            if returnval == 'CONTINUE':
                #Everything is good, keep going
                continue
            elif returnval == 'ERROR':
                #If there is an error we must still finish the chain since
                #there are some cleanup tasks that might need to run
                #We assume that the status database has already been updated
                error = context['error']
                continue
            elif returnval == 'STOP':
                #Most severe condition, the chain must stop.
                break
            else:
                #Unknown return type.  Treat the same as a stop error
                break
            
            #if returnval == 'True':
            #    self.logger.log("Command returned true, time to stop")
            #    break
                #return True
        #return False

################################################################################

################################################################################
# ESPA Command Section
################################################################################
class CleanUpDirs(Command):
    
    def expectedParameters():
        return ('work.dir', 'output.dir')
        
    def createsParameters():
        return ('error.message')
        
    '''Command that deletes temporary work directories located at context['work.dir']'''
    def execute(self, context):
        self.logger.log("Executing CleanUpDirs() for %s" % context['scene.id'])

        try:
            if os.path.exists(context['work.dir']):
                shutil.rmtree(context['work.dir'], ignore_errors=True)
            if os.path.exists(context['output.dir']):
                shutil.rmtree(context['output.dir'], ignore_errors=True)
        except:
            pass
                
        self.logger.log("CleanUpDirs() complete...")

        if context.has_key('error') and len(context['error']) > 0:
            return 'ERROR'
        else:
            return 'CONTINUE'

################################################################################



################################################################################

class ReadMetadata(Command):
    
    def expectedParameters():
        return ('work.dir')
    
    def createsParameters():
        return ('metadata', 'mtl_file')
    
    def execute(self, context):
        self.logger.log(("Executing ReadMetadata() for :%s") % (context['scene.id']))
        #find the metadata file
        mtl_file = ''
        items = os.listdir(context['work.dir'])
        for i in items:
            if not i.startswith('lnd') and (i.find('_MTL') > 0):
                mtl_file = i
                self.logger.log("Located MTL file:%s" % mtl_file)
                break

        if mtl_file == '':
            self.logger.log("Could not locate the landsat MTL file in %s" % context['work.dir'])
            return True
        
        #ledaps needs this value because it only operates on the current
        #working directory
        context['mtl_filename'] = mtl_file.replace('.TIF', '.txt')
        
        mtl_file = os.path.join(context['work.dir'], mtl_file)
        #print("Opening %s") % mtl_file
        
        
        f = open(mtl_file, 'r')
        data = f.readlines()
        f.close()
    
        #this will fix the problem ledaps has with binary characters at the end
        #of some of the gls metadata files
        length = len(data)
        buff = StringIO()
    
        count = 1
        for d in data:
            if count < length:
                buff.write(d)
                count = count + 1
    
        #fix the stupid error where the metadata txt file is named TIF
        mtl_file = mtl_file.replace('.TIF', '.txt')
            
        f = open(mtl_file, 'w+')
        fixedmeta = buff.getvalue()
        f.write(fixedmeta)
        f.flush()
        f.close()
        #print buffer.getvalue()
        buff.close()
        
        
        #print ("Fixedmeta:%s" % fixedmeta)
        #now we are going to read all the metadata into the context{} as
        #a dictionary.  Needed later for generating the solr index et. al.
        metadata = {}
        fixedmeta = fixedmeta.split('\n')
        for line in fixedmeta:
            line = line.strip()
            #print ('Meta line:%s' % line)
            if not line.startswith('END') and not line.startswith('GROUP'):
                parts = line.split('=')
                if len(parts) == 2:
                    metadata[parts[0].strip()] = parts[1].strip().replace('"', '')
        
        context['metadata'] = metadata
        #print context['metadata']
        
        
        #not to be confused with the mtl_filename... this one is the full
        #path to the file
        context['mtl_file'] = mtl_file
                
        self.logger.log("ReadMetadata() complete...")
        return 'CONTINUE'
    


################################################################################

class DistributeGeoSolr(Command):
    datasource = None
    
    def __init__(self, logger, datasource):
        super(DistributeGeoSolr, self).__init__(logger)
        self.datasource = datasource
    
    def expectedParameters():
        return ('create_as_collection', 'scene_id', 'work.dir')
    
    def createsParameters():
        return None
    
           
    def execute(self, context):
        if not context.has_key('error') or not len(context['error']) > 0:
            self.logger.log(("Executing DistributeGeoSolr() for :%s") % (context['scene.id']))
            browsename = ("%s.tif") % (context['scene.id'])
            solrname = ("%s-index.csv") % (context['scene.id'])
            browse = ("%s/browse/%s") % (context['work.dir'], browsename)
            solr = ("%s/solr/%s") % (context['work.dir'], solrname)
            br_result = self.datasource.put(context['scene.id'],browsename, browse)
            sl_result = self.datasource.put(context['scene.id'],solrname, solr)
            self.logger.log("DistributeGeoSolr() complete... ")
            return 'CONTINUE'
        else:
            self.logger.log("DistributeGeoSolr() complete... (previous errors detected)")
            return "ERROR"
        

################################################################################

class StopOnNlaps(Command):
    datasource = None
    
    def __init__(self, logger, datasource):
        super(StopOnNlaps, self).__init__(logger)
        self.datasource = datasource
    
    def expectedParameters():
        return None
    
    def createsParameters():
        return None
    
    def __init__(self, datasource):
        self.datasource = datasource
        
    def execute(self, context):
        pass
      

################################################################################

class DistributeProductToSFTP(Command):
    
    datasource = None
    
    def __init__(self,logger,datasource):
        super(DistributeProductToSFTP, self).__init__(logger)
        self.datasource = datasource   
   
    def expectedParameters():
        return ('product.filename.suffix', 'chain.name', 'distribution.productfile')
        
    def createsParameters():
        return ('distributed.product.location', 'scene.status', 'error.message')
        
    def execute(self,context):
        if not context.has_key('error') or not len(context['error']) > 0:
            self.logger.log("Executing DistributeProductToSFTP() for %s" % context['scene.id'])
            context['scene.status'] = 'Distributing'
            #ServerProxy(context,self.logger).updateStatus()

            chain = context['chain.name']
           
            #what local file are we distributing?
            productFile = context['distribution.productfile']
        
            if productFile == None or len(productFile) < 1:
                self.logger.log("No productFile supplied via context['distribution.productfile'], stopping")
                raise KeyError("context['distribution.productfile'] not found")

            attempt = 0
            waittime = 0
            success = False
            while success == False:
                attempt = attempt + 1
                if attempt > 3:
                    message = "Could not distribute product for %s.. stopping" % (productFile)
                    self.logger.log(message)
                    context['scene.status'] = 'Error'
                    context['error']= message
                    #ServerProxy(context,logger).updateStatus()
                    return "ERROR"
                else:
                    self.logger.log("Distribution attempt:%i Waiting %i seconds before transfer" % (attempt,waittime))
                    time.sleep(waittime)        
                    waittime = waittime + 5
                    #result = Utilities(self.logger).transfer(context,targetPath,sourceFile,username,password,host,port,'PUT')
                                        
                    targetFileName = context['scene.id'] + '-' + context['product.filename.suffix'] + '.tar.gz'
                    result = self.datasource.put(context['scene.id'],targetFileName,productFile)
                    
                    if result != '' :
                        context['distributed.productfile.location'] = result
                        self.logger.log("DistributeProductToSFTP() complete...")
                        return "CONTINUE"
        else:
            self.logger.log("DistributeProductToSFTP() complete...")
            return "ERROR"

################################################################################

class DistributeSourceToSFTP(Command):
    
    datasource = None
    
    def __init__(self,logger,datasource):
        super(DistributeSourceToSFTP, self).__init__(logger)
        self.datasource = datasource   
   
    def expectedParameters():
        return ('chain.name', 'distribution.sourcefile')
        
    def createsParameters():
        return ('distributed.source.location', 'scene.status', 'error.message')
        
    def execute(self,context):
        if not context.has_key('error') or not len(context['error']) > 0:
            
            #bail out if we don't want to distribute the source file
            if not context.has_key('distribute.sourcefile') or context['distribute.sourcefile'] != 'yes':
                self.logger.log("Skipping distribution of source file for %s..." % context['scene.id'])
                return CONTINUE
                           
                    
            self.logger.log("Executing DistributeSourceToSFTP() for %s" % context['scene.id'])
            context['scene.status'] = 'Distributing'
            #ServerProxy(context,self.logger).updateStatus()

            chain = context['chain.name']
           
            #what local file are we distributing?
            sourceFile = context['distribution.sourcefile']
        
            if sourceFile == None or len(sourceFile) < 1:
                self.logger.log("No sourceFile supplied via context['distribution.sourcefile'], stopping")
                raise KeyError("context['distribution.sourcefile'] not found")

            attempt = 0
            waittime = 0
            success = False
            while success == False:
                attempt = attempt + 1
                if attempt > 3:
                    message = "Could not distribute source for %s... stopping" % (sourceFile)
                    self.logger.log(message)
                    context['scene.status'] = 'Error'
                    context['error']= message
                    #ServerProxy(context,logger).updateStatus()
                    return "ERROR"
                else:
                    self.logger.log("Distribution attempt:%i Waiting %i seconds before transfer" % (attempt,waittime))
                    time.sleep(waittime)        
                    waittime = waittime + 5
                    #result = Utilities(self.logger).transfer(context,targetPath,sourceFile,username,password,host,port,'PUT')
                    targetFileName = context['scene.id'] + '.tar.gz'
                    result = self.datasource.put(context['scene.id'],targetFileName, sourceFile)
                    
                    if result != '':
                        context['distributed.sourcefile.location'] = result
                        self.logger.log("DistributeSourceToSFTP() complete...")
                        return "CONTINUE"
        else:
            self.logger.log("DistributeSourceToSFTP() complete...")
            return "ERROR"
        
################################################################################

class Ledaps(Command):
    '''Command to run Ledaps against a scene untarred in a directory located at context['work.dir']
    Requires the ancillary data path to be available via context['anc.path'], the lndpm executable to
    be available at context['lndpm_executable'], lndcal at context['lndcal_executable'], lndcsm at
    context['lndcsm_executable'], and lndsr at context['lndsr_executable']'''
    
    def expectedParameters():
        return ('scene.id', 'ledaps.anc.path', 'ledaps.bin.path', 'work.dir', 'ledaps.executable', 'mtl_file')
        
    def createsParameters():
        return None
    
    def execute(self, context):
        #don't do anything if there was a previous error
        if not context.has_key('error') or not len(context['error']) > 0:
            #logger = Logging(context)
            logfile = self.logger.getLogFile()
        
            try:           
                self.logger.log("Executing Ledaps() for %s" % context['scene.id'])
                context['scene.status'] = 'Processing'
                #ServerProxy(context,self.logger).updateStatus()
                os.environ["ANC_PATH"] = context.get('ledaps.anc.path')
                currentpath = os.environ['PATH']
                os.environ['PATH'] = context['ledaps.bin.path'] + ':' + os.environ['PATH']
                self.logger.log("Running ledaps")

                #Find the Landsat MTL file
                mtl_file = context['mtl_filename']
                
                #self.logger.log("Ledaps using " + mtl_file + " as mtl file") 
                #items = os.listdir(context['work.dir'])
                #for i in items:
                #    if not i.startswith('lnd') and i.endswith('_MTL.txt'):
                #        mtl_file = i
                #        break

                #if mtl_file == '':
                #    self.logger.log("Could not locate the landsat MTL file in %s" % context['work.dir'])
                #    return True

                ledaps = context['ledaps.executable'] + ' %s' % mtl_file

                #Execute ledaps                           
                proc = Popen(ledaps, cwd=context['work.dir'], stdout=logfile, stderr=logfile, shell=True)
                proc.wait()
                        
                self.logger.log("Ledaps() complete ...")
                return "CONTINUE"
           
            except Exception as e:
                self.logger.log("Ledaps() exception occurred:%s" % e)
                context['error'] = "An exception occurred running ledaps:%s" % e
                context['scene.status'] = 'Error'
                #ServerProxy(context,logger).updateStatus()
                return "ERROR"
            finally:
                logfile.close()
        else:
            return "ERROR"


################################################################################

class MarkSceneComplete(Command):
    '''Final command in the chain to make sure the orderservice knows that this scene has been completed'''
    
    def expectedParameters():
        return ('scene.log.file')
        
    def createsParameters():
        return None
    
    def execute(self,context):
        if not context.has_key('error') or not len(context['error']) > 0:
            self.logger.log("Executing MarkSceneComplete() for %s" % context['scene.id'])
            logfile = self.logger.getLogFile()
            filename = logfile.name
            logfile.close()
            
            handle = open(filename, 'r+')
            contents = handle.read()
            handle.close()
            
            #store it for insert to db
            context['scene.log.file'] = contents
            
            #ServerProxy(context,self.logger).markSceneComplete()
            #delete it from local disk
            
            os.unlink(filename)
            self.logger.log("MarkSceneComplete() complete...")
            return "CONTINUE"
        else:
            self.logger.log("MarkSceneComplete() complete...")
            return "ERROR"

################################################################################

class PrepareDirectories(Command):
    '''Creates working and output directories for each chain configuration in the config.properties.
    Looks for context['base.work.dir'], context['chain.name'], context['base.output.dir'] and creates
    two new context variables, context['output.dir'] and context['work.dir'].'''
    
    def expectedParameters():
        return ('scene.id', 'base.work.dir', 'chain.name', 'base.output.dir')
        
    def createsParameters():
        return ('work.dir', 'output.dir')
    
    def execute(self, context):
        if not context.has_key('error') or not len(context['error']) > 0:
            self.logger.log("Executing PrepareDirectories() for %s" % context['scene.id'])

            #TODO -- remove any leading slashes from the front of these values
            #since we are using os.path.join... otherwise it will fail
        
            sceneid = context.get("scene.id")            
            workDir = context.get("base.work.dir")
            workPath = context.get("chain.name")
            outputDir = context.get("base.output.dir")
            homedir = os.environ['HOME']
            workDir = os.path.join(workDir,workPath,sceneid)

            try:
                os.makedirs(workDir)
            except Exception:
                pass
                              
            context["work.dir"] = workDir

            #outputDir = os.path.join(homedir,outputDir,workPath,sceneid)
            outputDir = os.path.join(outputDir,workPath,sceneid)

            try:
                os.makedirs(outputDir)
            except Exception, e:
                pass
                                       
            context["output.dir"] = outputDir
            self.logger.log("PrepareDirectories() complete...")
            return "CONTINUE"
        else:
            self.logger.log("PrepareDirectories() complete...")
            return "ERROR"

################################################################################       

class PurgeFiles(Command):
    '''Command that will remove unneeded artifacts from the final SurfaceReflectance product.  Should be run
        before the TarFiles command'''

    def expectedParameters():
        return ('scene.id', 'work.dir')
        
    def createsParameters():
        return None

    def execute(self,context):
        self.logger.log("Executing PurgeFiles() for %s" % context['scene.id'])
                
        workdir = context['work.dir']
        for filename in os.listdir(workdir):
            file_path = os.path.join(workdir, filename)
            try:
                if os.path.isfile(file_path):
                    if (filename.endswith('TIF')
                        or filename.endswith('tar.gz')
                        or filename == 'README.GTF'
                        or filename.endswith('MTL.txt')
                        or filename.endswith('_GCP.txt')
                        or filename.startswith('lndcsm')
                        or filename.startswith('lndth')):
                            os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        if filename == 'browse' or filename == 'solr':
                            os.unlink(file_path)
                   
            except Exception,e:
                self.logger.log("Could not purge directories:%s" % e)
                context['scene.status'] = 'error'
                context['error'] = "Could not purge directories:%s" % e
                #ServerProxy().updateStatus(context,logger)
                self.logger.log("PurgeFiles() complete with errors...")
                return "ERROR"
       
        if os.path.exists(context['work.dir'] + '/browse'):
       	    shutil.rmtree(context['work.dir'] + '/browse')

        if os.path.exists(context['work.dir'] +	'/solr'):
       	    shutil.rmtree(context['work.dir'] +	'/solr')

        
        #Need to return error if one was previously detected.  This method runs because we want to clean
        #up our mess no matter what
        if context.has_key('error') and len(context['error']) > 0:
            self.logger.log("PurgeFiles() complete...")
            return "ERROR"
        else:
            self.logger.log("PurgeFiles() complete ...")
            return "CONTINUE"

################################################################################

class StageSceneFromLandsat(Command):
    
    datasource = None

    def __init__(self, logger,datasource):
        super(StageSceneFromLandsat, self).__init__(logger)
        self.datasource = datasource

    def expectedParameters():
        return ('scene.id')
        
    def createsParameters():
        return ('scene.status', 'error.message', 'staged.file')

    def execute(self,context):
        if not context.has_key('error') or not len(context['error']) > 0:
            try:
                self.logger.log("Executing StageSceneFromLandsat() for %s" % context['scene.id'])
        
                context['scene.status'] = 'Staging'
                #ServerProxy(context,self.logger).updateStatus()
        
                targetPath = context['output.dir']
             
                attempt = 0
                waittime = 0
                success = False
                while success == False:
                    attempt = attempt + 1
                    if attempt > 3:
                        message = "... Could not stage %s to %s... stopping" % (context['scene.id'], targetPath)
                        self.logger.log(message)
                        context['scene.status'] = 'Error'
                        context['error']= message
                        #ServerProxy(context,self.logger).updateStatus()
                        self.logger.log("StageSceneFromLandsat() complete with errors...")
                        
                        return "ERROR"
                    else:
                        self.logger.log("... Staging attempt:%i Waiting %i seconds before transfer" % (attempt,waittime))
                        time.sleep(waittime)
                        waittime = waittime + 5
            
                        result = self.datasource.get(context['scene.id'], targetPath + '/' + context['scene.id'] + '.tar.gz')
                    
                        if result != '':
                            #context['staged.file'] = targetPath + '/' + context['scene.id'] + '.tar.gz'
                            context['staged.file'] = result
                            context['distribution.sourcefile'] = context['staged.file']
                            self.logger.log("StageSceneFromLandsat() complete...")
                            return "CONTINUE"
            finally:
                pass
        else:
            self.logger.log("StageSceneFromLandsat() complete...")
            return "ERROR"
  
################################################################################

class TarFile(Command):
    '''Command that tars the work directory up for the scene located at context['work.dir']/context['input.filename']
    to context['output.dir']/context['input.filename']'''
    
    #PLEASE PUT ERROR HANDLING IN HERE
    
    def expectedParameters():
        return ('scene.id', 'output.dir', 'work.dir')
        
    def createsParameters():
        return None
    
    def execute(self, context):
        
        if not context.has_key('error') or not len(context['error']) > 0:
            self.logger.log("Executing TarFile() for %s" % context['scene.id'])
                                       
            #fulloutputpath = os.path.join(context['output.dir'], context['input.filename'])
            fulloutputpath = os.path.join(context['output.dir'], context['scene.id'] + '-sr.tar.gz')
            context['distribution.productfile'] = fulloutputpath
            tar = tarfile.open(fulloutputpath, "w:gz")

            os.chdir("%s/../" % context['work.dir'])
            
            #we should now be one level about the actual working directory and should
            #be including the whole work directory, which shares the name of the scene.
            tar.add(context['scene.id'])
            tar.close()
            self.logger.log("TarFile() complete...")
            return "CONTINUE"
        else:
            return "ERROR"
        
class TarFileNative(Command):
    '''Command that tars the work directory up for the scene located at context['work.dir']/context['input.filename']
    to context['output.dir']/context['input.filename']'''
    
    #PLEASE PUT ERROR HANDLING IN HERE
    
    def expectedParameters(self):
        return ('scene.id', 'output.dir', 'work.dir')
        
    def createsParameters(self):
        return ('distribution.productfile',)
    
    def execute(self, context):
        
        if not context.has_key('error') or not len(context['error']) > 0:
            
            self.logger.log("Executing TarFileNative() for %s" % context['scene.id'])
            
            fulloutputpath = os.path.join(context['output.dir'], context['scene.id'] + '-sr.tar')
            
            tartarget = "../%s" % context['scene.id']
            
            tarcmd = 'tar -cf %s %s' % (fulloutputpath, tartarget)
            
            proc = Popen(tarcmd, cwd=context['work.dir'], stdout=PIPE, stderr=PIPE, shell=True)
            
            proc.wait()
            
            gzcmd = 'gzip %s' % fulloutputpath
            #gzcmd = 'pigz %s' % fulloutputpath
            
            proc = Popen(gzcmd, cwd=context['output.dir'], stdout=PIPE, stderr=PIPE, shell=True)
            
            proc.wait()
            
            context['distribution.productfile'] = fulloutputpath + '.gz'        
            
            self.logger.log("TarFileNative() complete...")
            
            return "CONTINUE"
        else:
            self.logger.log("TarFileNative() complete... (previous errors detected)")
            return "ERROR"
        
################################################################################            

class UntarFile(Command):
    '''Command that looks for a context value of "hdfs.input.file" and untars it in
    a location specified by a context value of "work.dir"'''
    
    #PLEASE PUT ERROR HANDLING IN HERE
    
    def expectedParameters():
        return ('scene.id', 'staged.file', 'work.dir')
        
    def createsParameters():
        return None
    
    def execute(self, context):
        if not context.has_key('error') or not len(context['error']) > 0:
            
            self.logger.log("Executing UntarFile() for %s" % context['scene.id'])
            tf = tarfile.open(context.get("staged.file"), "r:gz")
            filename = str(tf.name).split(os.sep)[str(tf.name).count(os.sep)]

            #get the name of the 'work' directory from a configuration setting
            #instead
            targetDir = context.get("work.dir")

            #create dir specifically for the scene before extracting
            tf.extractall(targetDir)
            tf.close()
            self.logger.log("UntarFile() complete...")
            return "CONTINUE"
        else:
            self.logger.log("UntarFile() complete...")
            return "ERROR"


class UntarFileNative(Command):
    '''Command that looks for a context value of "staged.file" and untars it in
    a location specified by a context value of "work.dir"'''
    
    #PLEASE PUT ERROR HANDLING IN HERE
    
    def expectedParameters(self):
        return ('scene.id', 'staged.file', 'work.dir')
        
    def createsParameters(self):
        return None
    
    def execute(self, context):
        if not context.has_key('error') or not len(context['error']) > 0:
            
            self.logger.log("Executing UntarFileNative() for %s" % context['scene.id'])
            
            targetDir = context.get("work.dir")
    
            cpfl = 'cp %s %s' % (context['staged.file'], context['work.dir'])
            
            proc = Popen(cpfl, cwd=context['work.dir'], stdout=PIPE, stderr=PIPE, shell=True)
            
            proc.wait()
            
            ungz = 'gunzip %s' % (context['work.dir'] + '/*.gz')
            #ungz = 'pigz -d %s' % (context['work.dir'] + '/*.gz')
            
            proc = Popen(ungz, cwd=context['work.dir'], stdout=PIPE, stderr=PIPE, shell=True)
            
            proc.wait()
    
            untar = 'tar -xf %s' % (context['work.dir'] + '/*.tar')
            
            proc = Popen(untar, cwd=context['work.dir'], stdout=PIPE, stderr=PIPE, shell=True)
            
            proc.wait()
    
            rmtar = 'rm -f %s' % (context['work.dir'] + '/*.tar')
            
            proc = Popen(rmtar, cwd=context['work.dir'], stdout=PIPE, stderr=PIPE, shell=True)
            
            proc.wait()
            
            self.logger.log("UntarFileNative() complete...")
            
            return "CONTINUE"
        
        else:
            
            self.logger.log("UntarFileNative() complete... (previous errors detected)")
            
            return "ERROR"

################################################################################
class MakeBrowse(Command):
    def __init__(self, logger):
        super(MakeBrowse, self).__init__(logger)
        
    def expectedParameters(self):
        return ('scene.id', 'work.dir')
        
    def createsParameters(self):
        return ('browse.image', 'browse.tiles.dir')

    def getXY(self, value):
        '''Returns the xy coordinates for the given line from gdalinfo'''
        parts = value.split('(')    
        p = parts[1].split(')')
        p = p[0].split(',')
        return (p[1].strip(),p[0].strip())

    def parseGdalInfo(self, browsedir, sceneid):
        cmd = (('gdalinfo %s/%s.tif |grep \(') % (browsedir, context['scene.id']))
        proc = Popen(cmd, cwd=browsedir, stdout=PIPE, stderr=PIPE, shell=True)
        f = proc.stdout
        proc.wait()
        contents = f.read()
        f.close()
        proc = None
        f = None

        results = dict()
        
        lines = contents.split('\n')
        for l in lines:
            if l.startswith('Upper Left'):
                results['browse.ul'] = self.getXY(l)
                #print ("UL:%s,%s") % getXY(l)
            elif l.startswith('Lower Left'):
                results['browse.ll'] = self.getXY(l)
                #print ("LL:%s,%s") % getXY(l)
            elif l.startswith('Upper Right'):
                results['browse.ur'] = self.getXY(l)
                #print ("UR:%s,%s") % getXY(l)
            elif l.startswith('Lower Right'):
                results['browse.lr'] = self.getXY(l)
                #print ("LR:%s,%s") % getXY(l)

        return results
    
    def execute(self, context):
        if not context.has_key('error') or not len(context['error']) > 0:
            
            self.logger.log("Executing MakeBrowse() for %s" % context['scene.id'])
            logfile = self.logger.getLogFile()
            
            try:
                browsedir = os.path.join(context['work.dir'], 'browse')
                                
                if os.path.exists(browsedir):
                    shutil.rmtree(browsedir, ignore_errors=True)
                    os.makedirs(browsedir)
                else:
                    os.makedirs(browsedir)
              
                cmds = []
                cmds.append(('gdal_translate -of GTIFF -sds %s/lndsr*hdf %s/out.tiff') % (context['work.dir'], browsedir))
                cmds.append(('gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF %s/out.tiff5 %s/browse.tiff5') % (browsedir, browsedir))
                cmds.append(('gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF %s/out.tiff4 %s/browse.tiff4') % (browsedir, browsedir))
                cmds.append(('gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF %s/out.tiff3 %s/browse.tiff3') % (browsedir, browsedir))
                cmds.append(('gdal_merge_simple -in %s/browse.tiff5 -in %s/browse.tiff4 -in %s/browse.tiff3 -out %s/final.tif') % (browsedir, browsedir,browsedir,browsedir))

                #cmds.append(('gdalwarp -dstalpha -srcnodata 0 -t_srs EPSG:4326 -of GTIFF L71001054_05420070206_B10.TIF tst.TIF') % ()
                cmds.append(('gdalwarp -dstalpha -srcnodata 0 -t_srs EPSG:4326 %s/final.tif %s/warped.tif') % (browsedir, browsedir))

                #cmds.append(('gdal_translate -a_srs EPSG:4326 -co COMPRESS=DEFLATE -co PREDICTOR=2 -outsize 50%% 50%% -a_nodata -9999 -of GTIFF %s/warped.tif %s/%s.tif') % (browsedir, browsedir,context['scene.id']))
                cmds.append(('gdal_translate -co COMPRESS=DEFLATE -co PREDICTOR=2 -outsize 50%% 50%% -a_nodata -9999 -of GTIFF %s/warped.tif %s/%s.tif') % (browsedir, browsedir,context['scene.id']))
                cmds.append(('rm -rf %s/warped.tif') % (browsedir))
                cmds.append(('rm -rf %s/*tiff*') % (browsedir))
                cmds.append(('rm -rf %s/*out*') % (browsedir))
                cmds.append(('rm -rf %s/final.tif') % (browsedir))
                
                for cmd in cmds:
                    proc = Popen(cmd, cwd=context['work.dir'], stdout=logfile, stderr=logfile, shell=True)
                    proc.wait()

                #add the browse cornerpoints to the context here
                
                coords = self.parseGdalInfo(browsedir, context['scene.id'])
                metadata = context['metadata']
                metadata['BROWSE_UL_CORNER_LAT'] = coords['browse.ul'][0]
                metadata['BROWSE_UL_CORNER_LON'] = coords['browse.ul'][1]
                metadata['BROWSE_UR_CORNER_LAT'] = coords['browse.ur'][0]
                metadata['BROWSE_UR_CORNER_LON'] = coords['browse.ur'][1]
                metadata['BROWSE_LL_CORNER_LAT'] = coords['browse.ll'][0]
                metadata['BROWSE_LL_CORNER_LON'] = coords['browse.ll'][1]
                metadata['BROWSE_LR_CORNER_LAT'] = coords['browse.lr'][0]
                metadata['BROWSE_LR_CORNER_LON'] = coords['browse.lr'][1]
                context['metadata'] = metadata
               
                #
                #
                #
                #
                                    
                self.logger.log("MakeBrowse() complete...")
            finally:
                logfile.close()
                
            return "CONTINUE"
        else:
            self.logger.log("MakeBrowse() complete... (previous errors detected)")
            return "ERROR"

################################################################################    
class MakeSolrIndex(Command):
    def __init__(self, logger):
        super(MakeSolrIndex, self).__init__(logger)
        
    def expectedParameters(self):
        return ('metadata', 'scene.id', 'work.dir', 'create_as_collection')
    
    def createsParameters(self):
        return ('solr.index')
        
    def execute(self, context):
                
        if not context.has_key('error') or not len(context['error']) > 0:
            self.logger.log("Executing MakeSolrIndex() for %s" % context['scene.id'])
            header =  "sceneid;acquisitionDate;sensor;path;row;"
            header += "upperLeftCornerLatLong;upperRightCornerLatLong;"
            header += "lowerLeftCornerLatLong;lowerRightCornerLatLong;"
            header += "sunElevation;"
            header += "sunAzimuth;groundStation;collection"
            
            metadata = context['metadata']
            #print metadata
            
            sceneid = context['scene.id']
            acquisitionDate = metadata['ACQUISITION_DATE'] + "T00:00:01Z"
            sensor = metadata['SENSOR_ID']
            path = metadata['WRS_PATH']
            row = metadata['STARTING_ROW']
            #upper_left_LL = "%s,%s" % (metadata['PRODUCT_UL_CORNER_LAT'], metadata['PRODUCT_UL_CORNER_LON'])
            #upper_right_LL = "%s,%s" % (metadata['PRODUCT_UR_CORNER_LAT'], metadata['PRODUCT_UR_CORNER_LON'])
            #lower_left_LL = "%s,%s" % (metadata['PRODUCT_LL_CORNER_LAT'], metadata['PRODUCT_LL_CORNER_LON'])
            #lower_right_LL = "%s,%s" % (metadata['PRODUCT_LR_CORNER_LAT'], metadata['PRODUCT_LR_CORNER_LON'])
            upper_left_LL = "%s,%s" % (metadata['BROWSE_UL_CORNER_LAT'], metadata['BROWSE_UL_CORNER_LON'])
            upper_right_LL = "%s,%s" % (metadata['BROWSE_UR_CORNER_LAT'], metadata['BROWSE_UR_CORNER_LON'])
            lower_left_LL = "%s,%s" % (metadata['BROWSE_LL_CORNER_LAT'], metadata['BROWSE_LL_CORNER_LON'])
            lower_right_LL = "%s,%s" % (metadata['BROWSE_LR_CORNER_LAT'], metadata['BROWSE_LR_CORNER_LON'])
            sun_elevation = metadata['SUN_ELEVATION']
            sun_azimuth = metadata['SUN_AZIMUTH']
            ground_station = metadata['STATION_ID']
            collection = context['create_as_collection']
            
            index_string = ("%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s") % (
                sceneid, acquisitionDate,sensor,path,row,
                upper_left_LL, upper_right_LL,lower_left_LL, lower_right_LL,
                sun_elevation, sun_azimuth, ground_station, collection,'EOL'
                )
            
            p = os.path.join(context['work.dir'], 'solr')
            if not os.path.exists(p):
                os.makedirs(p)
            
            index_file = ("%s-index.csv") % context['scene.id']
            index_file = os.path.join(p, index_file)
            f = open(index_file, 'w')
            f.write(header + '\n')
            f.write(index_string + '\n')
            f.close()
            
            self.logger.log("MakeSolrIndex() complete...")
            return 'CONTINUE'
        else:
            self.logger.log("MakeSolrIndex() complete... (previous errors detected)")
            return "ERROR"
        pass

################################################################################ 

    ################################################################################
class NDVI(Command):

    def expectedParameters():
        return ('scene.id', 'work.dir')
        
    def createsParameters():
        return None

    def execute(self, context):
        if not context.has_key('error') or not len(context['error']) > 0:

            self.logger.log("Executing NDVI() for %s" % context['scene.id'])
            logfile = self.logger.getLogFile()
            
            try:
                # create context['work.dir']/tmp and context['work.dir']/ndvi if they
                # don't exist. Clean context['work.dir']/tmp if it does

                #srDir = "%s/sr" % context['work.dir']
                ndviDir = "%s/ndvi" % context['work.dir']

                # check that the srDir is there
                #if not os.path.exists(srDir) or not os.path.isdir(srDir):
                #    context['error'] = 'NDVI: No such directory: %s' % srDir
                #    return 'ERROR'
                #raise IOException('No such directory: %s' % srDir)

                # start with a clean slate
                #for d in (tmpDir, ndviDir):
                if os.path.exists(ndviDir):
                    shutil.rmtree(ndviDir, ignore_errors=True)
                    os.makedirs(ndviDir)
                else:
                    os.makedirs(ndviDir)


                # convert the source lndsr*hdf (in context[work.dir])
                # into geotiff, store in context[work.dir]/ndvidir
                cmd = ('gdal_translate -a_nodata -9999 -a_nodata 12000 -of GTIFF -sds %s/lndsr*hdf %s/out.tiff') % (context['work.dir'], ndviDir)
                proc = Popen(cmd, cwd=context['work.dir'], stdout=logfile, stderr=logfile, shell=True)
                proc.wait()
                proc = None
                gc.collect()
            
                # load the proper geotiff bands into GDAL from context[work.dir]/ndviDir
                red_file = ("%s/out.tiff3") % (ndviDir)
                in_ds = gdal.Open(red_file) 
                red = in_ds.ReadAsArray()
                geo = in_ds.GetGeoTransform()  # get the datum
                proj = in_ds.GetProjection()   # get the projection
                shape = red.shape          # get the image dimensions - format (row, col)

                in_ds = None

                

           
                nir_file = ("%s/out.tiff4") % (ndviDir)
                in_ds = gdal.Open(nir_file)
                nir = in_ds.ReadAsArray()
                in_ds = None


                # NDVI = (nearInfrared - red) / (nearInfrared + red)
                nir = np.array(nir, dtype = float)  # change the array data type from integer to float to allow decimals
                red = np.array(red, dtype = float)

                np.seterr(divide='ignore')
                
                numerator = np.subtract(nir, red) 
                denominator = np.add(nir, red)
                nir = None
                red = None
                gc.collect()

                ndvi = np.divide(numerator,denominator)
                numerator = None
                denominator = None
                gc.collect()

                #put this into 10000 range
                ndvi = np.multiply(ndvi, 10000)
                gc.collect()
                
                #set all negative values to 0
                np.putmask(ndvi, ndvi < 0, 0)
                
                #set all values greater than 10000 to 10000
                np.putmask(ndvi, ndvi > 10000, 10000)
                
                

                #geo = in_ds.GetGeoTransform()  # get the datum
                #proj = in_ds.GetProjection()   # get the projection
                #shape = red.shape          # get the image dimensions - format (row, col)

                driver = gdal.GetDriverByName('GTiff')

                # create this in the context['work.dir']/ndvi folder
                ndvifile = ('%s/ndvi.tif') % (ndviDir)
                dst_ds = driver.Create( ndvifile, shape[1], shape[0], 1, gdal.GDT_Float32)
                                                         # here we set the variable dst_ds with 
                                                         # destination filename, number of columns and rows
                                                         # 1 is the number of bands we will write out
                                                         # gdal.GDT_Float32 is the data type - decimals
                dst_ds.SetGeoTransform( geo ) # set the datum
                dst_ds.SetProjection( proj )  # set the projection

                dst_ds.GetRasterBand(1).WriteArray( ndvi)  
                stat = dst_ds.GetRasterBand(1).GetStatistics(1,1)  # get the band statistics (min, max, mean, standard deviation)
                dst_ds.GetRasterBand(1).SetStatistics(stat[0], stat[1], stat[2], stat[3]) # set the stats we just got to the band
                dst_ds = None

                gc.collect()

                in_ds = None
                dst_ds = None

                output_filename = ("%s/%s.tif") % (ndviDir, context['scene.id'])
                #cmd = ('gdal_translate -ot UInt -scale 0 10000 0 10000 -co COMPRESS=DEFLATE -of GTIFF %s %s') % (ndvifile, ndvifile)
                cmd = ('gdal_translate -ot UInt16 -scale 0 10000 0 10000 -of GTiff %s %s') % (ndvifile, output_filename)
                proc = Popen(cmd, cwd=ndviDir, stdout=logfile, stderr=logfile, shell=True)
                proc.wait()
                proc = None
                
                cmd = ('rm -rf %s/*tiff* %s/ndvi.tif') % (ndviDir, ndviDir)
                proc = Popen(cmd, cwd=context['work.dir'], stdout=logfile, stderr=logfile, shell=True)
                proc.wait()
            finally:
                logfile.close()
                gc.collect()

            # done
            self.logger.log("NDVI() complete...")
            return "CONTINUE"
        else:
            self.logger.log("NDVI() complete... (previous errors detected)")
            return "ERROR"
#===============================================================================
#End of Commands
#===============================================================================


################################################################################

if __name__ == '__main__':
    logfilename = '/tmp/espa.log'

    
    for line in sys.stdin:
        parts = line.split('\t')
        
        sceneid = parts[0].strip()
        collection_name = parts[1].strip()
        gls_year = parts[2].strip()
        
        print ("Processing %s into collection:%s for gls year:%s") % (sceneid, collection_name, gls_year)
        

        #check to make sure we've got a good record before proceeding
        if (not sceneid.startswith('L')): #or not xmlrpcurl.startswith('http')):
            continue;
        
        context = {}
        
        context['chain.name'] = 'sr'
        
        #context['base.work.dir'] = '/data/espa/work'
        #context['base.output.dir'] = '/data/espa/output'
        base = os.getcwd()
        
        context['base.work.dir'] = '%s/espa_tmp/work' % base
        context['base.output.dir'] = '%s/espa_tmp/output' % base

        context['ledaps.anc.path'] = '/usr/local/ledaps/ANC'
        context['ledaps.bin.path'] = '/home/espa/bin/ledaps/bin'
        context['ledaps.executable'] = context['ledaps.bin.path'] + '/do_ledaps.csh'
        context['hadoop_executable_path'] = '/home/espa/bin/hadoop/bin/hadoop'

        
        context['scene.id'] = sceneid
        context['create_as_collection'] = collection_name
        context['gls.year'] = gls_year
        context['product.filename.suffix'] = 'sr'
        context['distribute.sourcefile'] = 'no'
        
       
        ##############################################################################################
        #End autoconfiged values
        ##############################################################################################
        
        logger = LocalLogger(context)    
        
        #ds = GLSDataSource(logger, context, 'espa', '', 'edcsns7.cr.usgs.gov', 22)
        #ds = GLSDataSource(logger, context, 'espa', '', 'edcsns7.cr.usgs.gov', 22)
        #ds = LandsatDataSource(logger, context, 'espa', '', 'edclxs140.cr.usgs.gov', 22)
        ds = GLSDataSource(logger, context, 'espa', 'password', 'localhost', 22)

        #dds = DistributionDataSource(logger, context, 'espa', '', 'edclxs70.cr.usgs.gov', 22)
        #dds = DistributionDataSource(logger, context, 'espa', '', 'edclxs70.cr.usgs.gov', 22)
        dds = DistributionDataSource(logger, context, 'espa', 'password', 'localhost', 22)
        
        #gsds = GeoSolrDataSource(logger, context, 'espa', '', 'l8srlscp03.cr.usgs.gov', 22)
        gsds = GeoSolrDataSource(logger, context, 'espa', 'password', 'localhost', 22)
        
        
        chain = Chain(name='sr', logger=logger)
        chain.addCommand(PrepareDirectories(logger))
        chain.addCommand(StageSceneFromLandsat(logger,ds))
        chain.addCommand(UntarFileNative(logger))
        chain.addCommand(ReadMetadata(logger))
        chain.addCommand(Ledaps(logger))
        chain.addCommand(MakeBrowse(logger))
        chain.addCommand(MakeSolrIndex(logger))
        chain.addCommand(DistributeGeoSolr(logger, gsds))
        chain.addCommand(NDVI(logger))
        chain.addCommand(PurgeFiles(logger))
        chain.addCommand(TarFileNative(logger))
        chain.addCommand(DistributeProductToSFTP(logger, dds))
        chain.addCommand(CleanUpDirs(logger))
        #chain.addCommand(MarkSceneComplete(logger))
        
        chain.execute(context)
    
    


