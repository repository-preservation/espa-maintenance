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
            self.logfilename = '/tmp/espa.log'
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
    
    def __unicode__(self):
        return "DataSource"
    
    def __init__(self,context, logger):
        
        if context is not None:
            self.context = context
        else:
            self.context = dict()
            
        if logger is not None:
            self.logger = logger
        else:
            self.logger = LocalLogger(context)

        
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
    
    def __unicode__(self):
        return "SFTPDataSource"
    
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
            sftp,transport = self.getSFTPClient()
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
        #This is showing up as invalid syntax
        except:
            self.logger.log("... Error transferring %s \n\t  to %s" % (sourceFile,targetPath))
            #self.logger.log("... %s" % err)
            return ''
        finally:
            if sftp is not None:
                sftp.close()
            if transport is not None:
                transport.close()

################################################################################

class ReadOnlySFTPDataSource(SFTPDataSource):
    
    def __unicode__(self):
        return "ReadOnlySFTPDataSource"
    
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
    
    def __unicode__(self):
        return "GenericLandsatDataSource"
    
    
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
    
class ETMDataSource(LandsatDataSource):
    
    def __unicode__(self):
        return "ETMDataSource"
    
class TMDataSource(LandsatDataSource):
    
    def __unicode__(self):
        return "TMDataSource"
################################################################################

class NLAPSDataSource(LandsatDataSource):
    
    def __unicode__(self):
        return "NLAPSDataSource"
    
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
    def __unicode__(self):
        return "DistributionDataSource"
    
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
            #data_path = ("/data1/espa/%s/%s/%s/%s/%s") % (chain,sensor,path,row,year)
            data_path = ("/data1/espa/orders/%s/%s") % (self.context['order.id'],chain)
        
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

class GLSDataSource(LandsatDataSource):
    
    def __unicode__(self):
        return "GLSDataSource"
    
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
    
    def __unicode__(self):
        return "NCEPDataSource"
    
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
    
    def __unicode__(self):
        return "TOMSDataSource"
    
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
    hostname = None
    
    def __init__(self, context,logger):
        self.context = context
        self.logger = logger
        url = context['xmlrpcurl']
        self.server = xmlrpclib.ServerProxy(url)
        self.hostname = socket.gethostname()
    
    def markSceneComplete(self):
        response = ''
        try:
            source_l1t = 'not_available'
            if self.context.has_key('distributed.sourcefile.location') and self.context['distributed.sourcefile.location'] != None:
                source_l1t = self.context['distributed.sourcefile.location']
                
            response = self.server.markSceneComplete(self.context['scene.id'],
                                                     self.context['order.id'],
                                                     self.hostname,
                                                     self.context['distributed.productfile.location'],
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
        
        
        try:
            if self.context['scene.status'] == 'complete':
                response = self.server.markSceneComplete()
                
            elif self.context['scene.status'] == 'error':
                response = self.server.setSceneError(self.context['scene.id'],
                                                     self.context['order.id'],
                                                     self.hostname,
                                                     self.context['error'],
                                                     )
            else:
                response = self.server.updateStatus(self.context['scene.id'],
                                                    self.context['order.id'],
                                                    self.hostname,
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
#TODO: wrap calls in try/except, report error to xmlrpc if error

class CleanUpDirs(Command):
    
    def expectedParameters():
        return ('work.dir', 'output.dir')
        
    def createsParameters():
        return ('error.message')
        
    '''Command that deletes temporary work directories located at context['work.dir']'''
    def execute(self, context):
        self.logger.log("Executing CleanUpDirs() for %s" % context['scene.id'])
        shutil.rmtree(context['work.dir'])
        shutil.rmtree(context['output.dir'])
        self.logger.log("CleanUpDirs() complete...")

        if context.has_key('error') and len(context['error']) > 0:
            return 'ERROR'
        else:
            return 'CONTINUE'

################################################################################

class CreateBrowse(Command):
    
    def expectedParameters():
        return None
    
    def createsParameters():
        return None
    
    def execute(self, context):
        pass

################################################################################

class CreateMetadata(Command):
    
    def expectedParameters():
        return None
    
    def createsParameters():
        return None
    
    def execute(self, context):
        pass

################################################################################

class CreateGeoLayer(Command):
    
    def expectedParameters():
        return None
    
    def createsParameters():
        return None
    
    def execute(self, context):
        pass

################################################################################

class DistributeBrowse(Command):
    datasource = None
    
    def __init__(self, logger, datasource):
        super(DistributeBrowse, self).__init__(logger)
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

class DistributeGeoLayer(Command):
    datasource = None
    
    def __init__(self, logger, datasource):
        super(DistributeGeoLayer, self).__init__(logger)
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

class GenerateGLSList(Command):
    datasource = None
    
    def __init__(self, logger, datasource):
        super(GenerateGLSList, self).__init__(logger)
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
            context['scene.status'] = 'distributing'
            ServerProxy(context,self.logger).updateStatus()

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
                    context['scene.status'] = 'error'
                    context['error']= message
                    ServerProxy(context,logger).updateStatus()
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
            context['scene.status'] = 'distributing'
            ServerProxy(context,self.logger).updateStatus()

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
                    context['scene.status'] = 'error'
                    context['error']= message
                    ServerProxy(context,logger).updateStatus()
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
        return ('scene.id', 'ledaps.anc.path', 'ledaps.bin.path', 'work.dir', 'ledaps.executable')
        
    def createsParameters():
        return None
    
    def execute(self, context):
        #don't do anything if there was a previous error
        if not context.has_key('error') or not len(context['error']) > 0:
            #logger = Logging(context)
            logfile = self.logger.getLogFile()
        
            try:           
                self.logger.log("Executing Ledaps() for %s" % context['scene.id'])
                context['scene.status'] = 'processing'
                ServerProxy(context,self.logger).updateStatus()
                os.environ["ANC_PATH"] = context.get('ledaps.anc.path')
                currentpath = os.environ['PATH']
                os.environ['PATH'] = os.environ['PATH'] + ':' + context['ledaps.bin.path']
                self.logger.log("Running ledaps")

                #Find the Landsat MTL file
                mtl_file = ''
                items = os.listdir(context['work.dir'])
                for i in items:
                    if not i.startswith('lnd') and i.endswith('_MTL.txt'):
                        mtl_file = i
                        break

                if mtl_file == '':
                    self.logger.log("Could not locate the landsat MTL file in %s" % context[work.dir])
                    return True

                ledaps = context['ledaps.executable'] + ' %s' % mtl_file

                #Execute ledaps                           
                proc = Popen(ledaps, cwd=context['work.dir'], stdout=logfile, stderr=logfile, shell=True)
                proc.wait()
                        
                self.logger.log("Ledaps() complete ...")
                return "CONTINUE"
           
            except Exception as e:
                self.logger.log("Ledaps() exception occurred:%s" % e)
                context['error'] = "An exception occurred running ledaps:%s" % e
                context['scene.status'] = 'error'
                ServerProxy(context,logger).updateStatus()
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
            filename = self.logger.logfilename
            handle = open(filename, 'r')
            contents = handle.read()
            handle.close()
            #store it for insert to db
            context['scene.log.file'] = contents
            ServerProxy(context,self.logger).markSceneComplete()
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
            
            except Exception,e:
                self.logger.log("Could not purge directories:%s" % e)
                context['scene.status'] = 'error'
                context['error'] = "Could not purge directories:%s" % e
                ServerProxy().updateStatus(context,logger)
                self.logger.log("PurgeFiles() complete with errors...")
                return "ERROR"
        
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
        
                context['scene.status'] = 'staging'
                ServerProxy(context,self.logger).updateStatus()
        
                targetPath = context['output.dir']
             
                attempt = 0
                waittime = 0
                success = False
                while success == False:
                    attempt = attempt + 1
                    if attempt > 3:
                        message = "... Could not stage %s to %s... stopping" % (context['scene.id'], targetPath)
                        self.logger.log(message)
                        context['scene.status'] = 'error'
                        context['error']= message
                        ServerProxy(context,self.logger).updateStatus()
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
    
#===============================================================================
#End of Commands
#===============================================================================
#class testDistributeToSFTP(unittest):
#    pass
    #run one of the test classes or whatever else you wish for testing
    #context = {}
    #context['chain.name'] = 'test'
    #context['scene.id'] = 'LE70290292001234EDC00'
    #context['test.distribution.cache.host'] = '127.0.0.1'
    #context['test.distribution.cache.port'] = 22
    #context['test.distribution.cache.username'] = 'espa'
    #context['test.distribution.cache.password'] = 'password'
    #context['distribution.source.file'] = '/tmp/dave222.txt'
    #context['distribution.target.path'] = '/tmp/failed.txt'
    #DistributeFileToSFTP().execute(context)

#class TestSFTPTransfer(unittest.TestCase):

    #def setUp(self):
    #    pass

    #def runTest(self):
    #    self.assertEqual('5', '5')

    #def tearDown(self):
    #    pass
    
    #targetPath = '/tmp/testfile-transferred.txt'
    #sourceFile = '/tmp/testfile.txt'
    #username = 'espa'
    #password = ''
    #host = 'l8srlscp07.cr.usgs.gov'
    #port = 22
    #operation = 'FOSHIZZLE' #Should be GET or PUT
    #print Utilities().transfer({},targetPath,sourceFile,username,password,host,port,operation)

#class TestStageFileFromSFTP(unittest.TestCase):

    #def setUp(self):
    #    pass

    #def runTest(self):
    #     self.assertEqual(10, 10)

    #def tearDown(self):
    #    pass

################################################################################

if __name__ == '__main__':
    #unittest.main()
  #  context = {}
  #  context['order.id'] = 'dhill@usgs.gov-abc123'
  #  context['scene.id'] = 'LT50370322010273EDC00'
    #context['scene.id'] = 'LE70290302003113EDC00'
  #  context['base.work.dir'] = '/tmp/espa_work'
  #  context['base.output.dir'] = '/tmp/espa_output'
  #  context['chain.name'] = 'sr'
  #  context['ledaps.anc.path'] = '/usr/local/ledaps/ANC'
  #  context['ledaps.bin.path'] = '/home/espa/bin/ledaps/bin'
  #  context['ledaps.executable'] = context['ledaps.bin.path'] + '/do_ledaps.csh'
  #  context['xmlrpcurl'] = 'http://localhost/rpc'
  #  context['create_as_collection'] = 'gls-2010'
  #  context['gls.year'] = '2010'
  #  context['product.filename.suffix'] = 'sr'
  #  context['distribute.sourcefile'] = 'no'
        
    for line in sys.stdin:
        
                
        parts = line.split('\t')
        
        orderid = parts[0].strip()
        sceneid = parts[1].strip()
        xmlrpcurl = parts[2].strip()
        
        #This will allow us to submit all the jobs in the system mixed in no matter
        #what chain they are supposed to use or even if they are a collection vs.
        #on demand
        #if chain == 'sr-ondemand':
        #    pass
        #elif chain == 'sr-collection':
        #    pass
        #else:
        #    pass
        
        print "Processing %s" % sceneid
        
        #url of the xmlrpc server to report status against
        #xmlrpcurl = parts[1]

        #check to make sure we've got a good record before proceeding
        if (not sceneid.startswith('L')): #or not xmlrpcurl.startswith('http')):
            #print ' '
            continue;
        
        context = {}
        context['chain.name'] = 'sr'
        context['base.work.dir'] = '/data/espa/work'
        context['base.output.dir'] = '/data/espa/output'
        context['ledaps.anc.path'] = '/usr/local/ledaps/ANC'
        context['ledaps.bin.path'] = '/home/espa/bin/ledaps/bin'
        context['ledaps.executable'] = context['ledaps.bin.path'] + '/do_ledaps.csh'
        context['hadoop_executable_path'] = '/home/espa/bin/hadoop/bin/hadoop'
        context['scene.id'] = sceneid
        context['order.id'] = orderid
        context['xmlrpcurl'] = xmlrpcurl
        context['product.filename.suffix'] = 'sr'
        context['distribute.sourcefile'] = 'yes'
        context['log.file.path'] = '/tmp/'
        context['log.file.name'] = 'espa_' + context['chain.name'] + '_' + context['scene.id'] + '.log'
        
        
        ##############################################################################################
        #End autoconfiged values
        ##############################################################################################
        
        logger = LocalLogger(context)    
        
        server = xmlrpclib.ServerProxy(context['xmlrpcurl'])
        ls_username = server.getConfiguration('landsatds.username')
        ls_password = server.getConfiguration('landsatds.password')
        ls_host = server.getConfiguration('landsatds.host')
        ls_port = server.getConfiguration('landsatds.port')
        
        dds_username = server.getConfiguration('distributionds.username')
        dds_password = server.getConfiguration('distributionds.password')
        dds_host = server.getConfiguration('distributionds.host')
        dds_port = server.getConfiguration('distributionds.port')
        server = None
        
        #Look these values up from xmlrpc service or from database
        ds = LandsatDataSource(logger, context, ls_username, ls_password, ls_host, ls_port)
        dds = DistributionDataSource(logger, context, dds_username, dds_password, dds_host, dds_port)
        
        chain = Chain(name='sr', logger=logger)
        chain.addCommand(PrepareDirectories(logger))
        chain.addCommand(StageSceneFromLandsat(logger,ds))
        chain.addCommand(UntarFile(logger))
        chain.addCommand(Ledaps(logger))
        chain.addCommand(PurgeFiles(logger))
        chain.addCommand(TarFile(logger))
        chain.addCommand(DistributeProductToSFTP(logger, dds))
        if context['distribute.sourcefile'] == 'yes':
            chain.addCommand(DistributeSourceToSFTP(logger, dds))
        chain.addCommand(CleanUpDirs(logger))
        chain.addCommand(MarkSceneComplete(logger))
        chain.execute(context)
    
    


