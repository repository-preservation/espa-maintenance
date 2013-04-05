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

'''
The following context parameters are expected to be initialized prior to running any espa commands:

context['log.file.name']                Name of logfile for all commands to write to
context['log.file.path']                Directory to use for writing the logfile
context['ledaps.lndpm_executable']      Full path to lndpm executable for ledaps
context['ledaps.lndcsm_executable']     Full path to the lndcsm executable for ledaps
context['ledaps.lndcal_executable']     Full path to the lndcal executable for ledaps
context['ledaps.lndsr_executable']      Full path to the lndsr executable for ledaps
context['ledaps.anc.path']              Full path to the ledaps ancillary data
context['input.filename']               Name of file to process, no path
context['base.work.dir']                Base path on filesystem to use for processing
context['base.output.dir']              Base path on filesystem to use for staging finished products
context['hadoop_executable_path']       Full path to the hadoop executables
context['scene.id']                     Name of scene (not the file) to process
context['scene.path']                   WRS path scene resides in
context['scene.row']                    WRS row scene resides in
context['scene.year']                   Year scene was collected
context['scene.day']                    Julian day of collection
context['scene.sensor']                 The sensor that was used to collect the scene
context['scene.status']                 Status of this scene.  Should either be blank or set to 'On Cache' if this job is being run
context['scene.completed.location']     Should be populated at the end of processing for reporting back to the controller node.
context['online.cache.host']            Hostname for the landsat online cache
context['online.cache.port']            Port number for landsat online cache
context['online.cache.username']        Username for the landsat online cache
context['online.cache.password']        Password for the landsat online cache
context['distribution.cache.host']      Hostname for the distribution cache
context['distribution.cache.port']      Port for the distribution cache
context['distribution.cache.username']  Username for the distribution cache
context['distribution.cache.password']  Password for the distribution cache

The following commands generate these additional context parameters:
PrepareDirectories()
context['output.dir']               Actual path to the output directory for the particular scene
context['work.dir']                 Actual path to the scenes working directory

StageFromHDFS()
context['staged.file']              Full path to the file that has been staged and ready for untarring

'''

#===============================================================================
#Utility Classes 
#===============================================================================

#TODO: wrap all calls in try/except, report error to xmlrpc if error
class Utilities():
    
    def buildDistributionPath(self, context,sftp):

        try:
            Logging(context).log("Building distribution path for %s" % context['scene.id'])
            server = ServerProxy(context)
            path = server.getScenePath()
            row = server.getSceneRow()
            year = server.getSceneYear()
            sensor = server.getSceneSensor()

            if str(path).startswith('0'):
                path = str(path)[1:len(str(path))]

            if str(row).startswith('0'):
                row = str(row)[1:len(str(row))]



            #MAKE THIS HAPPEN, NEED TO CONFIGURE THE BASE PATH
            #base_path = None
            #if self.context.has_key('distribution.cache.base.path') and self.context('distribution.cache.base.path') != None:
            #    base_path = self.context('distribution.cache.base.path')
            #else:
            #    base_path = ServerProxy(context).getConfiguration('distribution.cache.base.path')


            
            if context.has_key('collection.name') and context['collection.name'] != None:
                path = ('data1/espa/collections/%s/%s/%s/%s/%s/%s') % (self.context['collection.name'],str(sensor),str(context['chain.name']), str(path), str(row), str(year))
            else:
                path = ('data1/espa/%s/%s/%s/%s/%s') % (str(context['chain.name']),str(sensor), str(path),str(row),str(year))

            #recursively verify/build the target path on sftp    
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
            pass
            #if sftp is not None:
            #    sftp.close()

    
class ServerProxy():
    #This is the docstring: Command to report status of chain execution to an xmlrpc server
    server = None
    context = None
    
    def __init__(self, context):
        self.context = context
        url = context['xmlrpcurl']
        #check to see if we have a url try/catch
        self.server = xmlrpclib.ServerProxy(url)
        #check to see if we could contact server. try/catch
        

    def getSceneInputPath(self):
        response = ''
        try:
            response = self.server.getSceneInputPath(self.context['scene.id'])
        except Exception, err:
            Logging(self.context).log("Error getting scene input path:%s" % err)
        return response

    def getScenePath(self):
        response = ''
        try:
            response = self.server.getScenePath(self.context['scene.id'])
        except Exception, err:
            Logging(self.context).log("Error getting scene path:%s" % err)
        return response

    def getSceneRow(self):
        response = ''
        try:
            response = self.server.getSceneRow(self.context['scene.id'])
        except Exception, err:
            Logging(self.context).log("Error getting scene row:%s" % err)
        return response

    def getSceneYear(self):
        response = ''
        try:
            response = self.server.getSceneYear(self.context['scene.id'])
        except Exception, err:
            Logging(self.context).log("Error getting scene year:%s" % err)
        return response

    def getSceneDay(self):
        response = ''
        try:
            response = self.server.getSceneDay(self.context['scene.id'])
        except Exception, err:
            Logging(self.context).log("Error getting scene day:%s" % err)
        return response
    
    def getSceneStation(self):
        response = ''
        try:
            response = self.server.getSceneStation(self.context['scene.id'])
        except Exception, err:
            Logging(self.context).log("Error getting scene station:%s" % err)
        return response

    def getSceneSensor(self):
        response = ''
        try:
            response = self.server.getSceneSensor(self.context['scene.id'])
        except Exception, err:
            Logging(self.context).log("Error getting scene sensor:%s" % err)
        return response
    

    def markSceneComplete(self):
        response = ''
        try:
            source_l1t = 'not_available'
            if self.context.has_key('source.l1t.distro.location') and self.context['source.l1t.distro.location'] != None:
                source_l1t = self.context['source.l1t.distro.location']
                
            response = self.server.markSceneComplete(self.context['scene.id'],
                                                     self.context['completed.scene.location'],
                                                     source_l1t,
                                                     xmlrpclib.Binary(self.context['scene.log.file']))
        except Exception, err:
            Logging(self.context).log("Error marking scene complete:%s" % err)
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
    
    #this method is bad... needs much more robust error and condition checking in the if statement
    def updateStatus(self):
        try:
            if self.context['scene.status'] == 'Complete':
                response = self.server.markSceneComplete(self.context['scene.id'], self.context['completed.scene.location'])
            else:
                response = self.server.updateStatus(self.context['scene.id'], self.context['scene.status'])
            return response
        except Exception, err:
            Logging(self.context).log("Error updating scene status:%s" % err)
            return err

    def getConfiguration(self,key):
        response = ''
        try:
            response = self.server.getConfiguration(key)
        except Exception, err:
            Logging(self.context).log("Error getting configuration key:%s" % err)
                                
        return response

    
#===============================================================================
# Logging Section
#===============================================================================
class Logging():
    '''Implements logging for espa'''

    logfilename = None

    def __init__(self, context = None):
        
        
        if context is None or not context.has_key('log.file.name') or context['log.file.name'] is None:
            self.logfilename = '/tmp/espa.log'
        else:
            #assume it's going into the working directory
            self.logfilename = context['log.file.path'] + context['log.file.name']
            
    
    def getLogFile(self):
        f = open(self.logfilename, 'a')
        return f


    def log(self,message):
        f = self.getLogFile()
        f.write(message + '\n')
        f.close()
    

#===============================================================================
# Command and Chain Section -- TODO: Pull out this out and use pychained instead
#===============================================================================
#
# define component as an abstract base class
#
class Command():
    '''
    Abstract base class for command implementations to subclass
    '''
    __metaclass__ = ABCMeta

    name = 'Default'

    @abstractmethod
    def execute(self, context):
        '''Abstract execute method that will be called on all Commands

        Keyword arguments:
        context -- A python dictionary (key -> value) that contains
               all the variables in use by the Chain of Commands
        '''
        print "--- Context executing ---"

        
class Chain(Command):
    '''Base implementation of a Chain to allow us to string commands together.
    The Chain class subclasses Command so that we can construct a Chain
    of Chains if necessary.
    '''
    __commands = []     
    
    def __init__(self, name='Default'):
         self.name = name
         self.__commands = []
         
    def addCommand(self, Command):
        """Method to add a new Command to the end of the Chain

        Keyword arguments:
        Command -- A Command to be appended to the end of the Chain
        """
        #print "Command list is now %i long" % len(self.__commands)
        self.__commands.append(Command)

    def execute(self, context):
        logger = Logging(context)
        
        logger.log("--- Chain[%s] ---" % self.name)
        for c in range(len(self.__commands)):
            returnval =  self.__commands[c].execute(context)

            if returnval == 'True':
                logger.log("Command returned true, time to stop")
                break
                #return True
        #return False
        
class PersistentChain(Chain):
    '''Chain implementation to provide context persistence and the ability for commands to return 'WAIT'
    to signify that the command did not complete or fail, but needs to be run again in a specified amount
    of time.
    '''
     
    def execute(self, context):
        '''Override execute in Chain'''
     
        #print "--- PersistentChain[%s] ---" % self.name

        #for c in range(len(self.__commands)):
        #    returnval =  self.__commands[c].execute(context)

            #Need to mod this to include three statuses...
            #CONTINUE, WAIT, and ERROR

        #    context.persist()
               
        #    if returnval == 'ERROR':
        #        break
        #    elif returnval == 'WAIT':
            #go into loop with sleep and try this command again
        #        pass
        #    elif returnval == 'CONTINUE':
                #command executed successfully, continue down the chain
        #        pass
        #    else:
        #        logger.error('Unknown return value received from command')
        #        break
        pass

class ReportableChain(Chain):
    '''Chain implementation that looks for an xmlrpcurl parameter in the context object
    to report progress of chain components'''

    def execute(self, context):
        '''Override execute in Chain'''
     
        print "--- ReportableChain[%s] ---" % self.name


        #url = context.get('xmlrpcurl')
        
        for c in range(len(self.__commands)):

            #Update status that a command is about to run
            #url.updateStatus(self.__commands[c].getName(), "Running", ipaddress)

            returnval =  self.__commands[c].execute(context)

            #Update status that a command ran and its status
            #url.updateStatus(self.__commands[c].getName(), returnval, ipaddress)    
            
            
            #Need to mod this to include three statuses...
            #CONTINUE, WAIT, and ERROR
               
            if returnval == 'ERROR':
                break
            elif returnval == 'WAIT':
            #go into loop with sleep and try this command again
                pass
            elif returnval == 'CONTINUE':
                #command executed successfully, continue down the chain
                pass
            else:
                logger.error('Unknown return value received from command')
                break
    
#===============================================================================
#End of Chains 
#===============================================================================

#===============================================================================
# ESPA Command Section
#===============================================================================
#TODO: wrap calls in try/except, report error to xmlrpc if error
class CleanUpDirs(Command):
    '''Command that deletes temporary work directories located at context['work.dir']'''
    def execute(self, context):
        Logging(context).log("Executing CleanUpDirs() for %s" % context['scene.id'])
                
        shutil.rmtree(context['work.dir'])
        shutil.rmtree(context['output.dir'])
        return False
        
        
#TODO: wrap calls in try/except, report error to xmlrpc if error
class DistributeFileToSFTP(Command):
    '''Moves the completed file the configured location'''
    def execute(self,context):
        Logging(context).log("Executing DistributeFileToSFTP() for %s" % context['scene.id'])
        context['scene.status'] = 'Distributing'
        ServerProxy(context).updateStatus()
        
        host = context['distribution.cache.host']
        port = context['distribution.cache.port']
        password = context['distribution.cache.password']
        username = context['distribution.cache.username']

        if host == None or len(host) < 1:
            host = ServerProxy(context).getConfiguration('distribution.cache.host')
        if port == None or len(str(port)) < 1:
            port = ServerProxy(context).getConfiguration('distribution.cache.port')
        if password == None or len(password) < 1:
            password = ServerProxy(context).getConfiguration('distribution.cache.password')
        if username == None or len(username) < 1:
            username = ServerProxy(context).getConfiguration('distribution.cache.username')


        #print("Host:%s Port:%i Username:%s Password:%s") % (host,port,username,password)
        sftp = None
        transport = None
        try:
            transport = paramiko.Transport((host,int(port)))
            transport.connect(username = username, password = password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            targetPath = Utilities().buildDistributionPath(context,sftp)

            #Will need to make this generic to accept a 'file.to.distribute' parameter so
            #we can get rid of the second Command 'DistributeSourceL1tToSFTP'
            source = context['output.dir'] + '/' + context['scene.id'] + '-SR.tar.gz'
        
            targetPath = targetPath + '/' + context['scene.id'] + '-SR.tar.gz'
            sftp.put(source, targetPath)

            #Will need to further genericize this parameter to be able to account for source l1ts and
            #completed output scenes
            context['completed.scene.location'] = targetPath
            return False
        finally:
            if sftp is not None:
                sftp.close()
            if transport is not None:
                transport.close()
            

   

#TODO: wrap calls in try/except, report error to xmlrpc if error
class DistributeSourceL1TToSFTP(Command):
    '''Moves the completed file the configured location'''
    def execute(self,context):
        sftp = None
        transport = None
        try:
            Logging(context).log("Executing DistributeSourceL1TToSFTP() for %s" % context['scene.id'])
            context['scene.status'] = 'Distributing'
            ServerProxy(context).updateStatus()
        
            host = context['distribution.cache.host']
            port = context['distribution.cache.port']
            password = context['distribution.cache.password']
            username = context['distribution.cache.username']

            if host == None or len(host) < 1:
                host = ServerProxy(context).getConfiguration('distribution.cache.host')
            if port == None or len(str(port)) < 1:
                port = ServerProxy(context).getConfiguration('distribution.cache.port')
            if password == None or len(password) < 1:
                password = ServerProxy(context).getConfiguration('distribution.cache.password')
            if username == None or len(username) < 1:
                username = ServerProxy(context).getConfiguration('distribution.cache.username')
        
            transport = paramiko.Transport((host,int(port)))
            transport.connect(username = username, password = password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            targetPath = Utilities().buildDistributionPath(context,sftp)
            source = context['source.l1t']
            targetPath = targetPath + '/' + context['input.filename']

            msg = "DistributeSourceL1TToSFTP() Source:" + source + " Target:" + targetPath
            Logging(context).log(msg)
        
            sftp.put(source, targetPath)
            context['source.l1t.distro.location'] = targetPath
            return False
        finally:
            if sftp is not None:
                sftp.close()
            if transport is not None:
                transport.close()

class DistributeFileToFilesystem(Command):

    def execute(self,context=None):
        pass

#TODO: wrap calls in try/except, report error to xmlrpc if error.  Scan output file for known ledaps errors and report those.
class Ledaps(Command):
    '''Command to run Ledaps against a scene untarred in a directory located at context['work.dir']
    Requires the ancillary data path to be available via context['anc.path'], the lndpm executable to
    be available at context['lndpm_executable'], lndcal at context['lndcal_executable'], lndcsm at
    context['lndcsm_executable'], and lndsr at context['lndsr_executable']'''

    
    def execute(self, context):
        logger = Logging(context)
        logfile = logger.getLogFile()
        
        try:
           
            logger.log("Executing Ledaps() for %s" % context['scene.id'])

            context['scene.status'] = 'Processing'
            ServerProxy(context).updateStatus()
                          
            os.environ["ANC_PATH"] = context.get('ledaps.anc.path')
            currentpath = os.environ['PATH']
            os.environ['PATH'] = os.environ['PATH'] + ':' + context['ledaps.bin.path']

            logger.log("Running ledaps")

            #Find the Landsat MTL file
            mtl_file = ''
            items = os.listdir(context['work.dir'])
            for i in items:
                if not i.startswith('lnd') and i.endswith('_MTL.txt'):
                    mtl_file = i
                    break

            if mtl_file == '':
                logger.log("Could not locate the landsat MTL file in %s" % context[work.dir])
                return True

            ledaps = context['ledaps.executable'] + ' %s' % mtl_file

            #Execute ledaps                           
            proc = Popen(ledaps, cwd=context['work.dir'], stdout=logfile, stderr=logfile, shell=True)
            proc.wait()
                        
            #return false to keep the chain moving
            return False
           
        except Exception as e:
            logger.log("An exception occurred:%s" % e)
            return True
        finally:
            logfile.close()


#TODO: wrap calls in try/except, report error to xmlrpc if error            
class MarkSceneComplete(Command):
    '''Final command in the chain to make sure the orderservice knows that this scene has been completed'''
    
    def execute(self,context):
        Logging(context).log("Executing MarkSceneComplete() for %s" % context['scene.id'])
        filename = Logging(context).logfilename
        handle = open(filename, 'r')
        contents = handle.read()
        handle.close()
        
        #store it for insert to db
        context['scene.log.file'] = contents
        ServerProxy(context).markSceneComplete()

        #delete it from local disk
        os.unlink(filename)

        return False

                    
        
#TODO: wrap calls in try/except, report error to xmlrpc if error
class PrepareDirectories(Command):
    '''Creates working and output directories for each chain configuration in the config.properties.
    Looks for context['base.work.dir'], context['chain.name'], context['base.output.dir'] and creates
    two new context variables, context['output.dir'] and context['work.dir'].'''
    
    def execute(self, context):       
        Logging(context).log("Executing PrepareDirectories() for %s" % context['scene.id'])

        #TODO -- remove any leading slashes from the front of these values
        #since we are using os.path.join... otherwise it will fail
        
        sceneid = context.get("scene.id")            
        workDir = context.get("base.work.dir")
        workPath = context.get("chain.name")
        outputDir = context.get("base.output.dir")
        homedir = os.environ['HOME']
                
        #workDir = os.path.join(homedir,workDir,workPath,sceneid)
        workDir = os.path.join(workDir,workPath,sceneid)

        #print ("Trying to make:%s" % workDir)
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
        return False
       
#TODO: wrap calls in try/except, report error to xmlrpc if error
class PurgeFiles(Command):
    '''Command that will remove unneeded artifacts from the final SurfaceReflectance product.  Should be run
        before the TarFiles command'''

    def execute(self,context):
        Logging(context).log("Executing PurgeFiles() for %s" % context['scene.id'])
                
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
                #print e
                pass
            
        return False

#TODO: wrap calls in try/except, report error to xmlrpc if error
class StageFileFromSFTP(Command):
    #build a connection to the online cache and returns it
    
    def execute(self,context):
        sftp = None
        transport = None
        try:
            Logging(context).log("Executing StageFileFromSFTP() for %s" % context['scene.id'])
            context['scene.status'] = 'Staging'
            ServerProxy(context).updateStatus()
        
            host = context['online.cache.host']
            port = context['online.cache.port']
            password = context['online.cache.password']
            username = context['online.cache.username']

            if host == None or len(host) < 1:
                host = ServerProxy(context).getConfiguration('online.cache.host')
            
            if port == None or len(str(port)) < 1:
                port = ServerProxy(context).getConfiguration('online.cache.port')
            
            if password == None or len(password) < 1:
                password = ServerProxy(context).getConfiguration('online.cache.password')
            
            if username == None or len(username) < 1:
                username = ServerProxy(context).getConfiguration('online.cache.username')

            #print("Host:%s Port:%i Username:%s Password:%s") % (host,port,username,password)
            
            transport = paramiko.Transport((host,int(port)))
            transport.connect(username = username, password = password)
            sftp = paramiko.SFTPClient.from_transport(transport)
    
            #context['staged.file'] = ('%s/%s') % (context['work.dir'], context['input.filename'])
            #Changed this to stage the original l1t to the output directory so we don't have to
            #move it here after processing is done.  We want to distribute the source l1t alongside
            #the processed scene
            context['staged.file'] = os.path.join(context['output.dir'],context['input.filename'])
            context['source.l1t'] = context['staged.file']

            #build input path
            #This will need to be modified to support pulling scenes from more than 1 source cache.
            inputPath = ServerProxy(context).getSceneInputPath()
            sftp.get(inputPath, context['staged.file'])
            sftp.close()
            return False
        finally:
            if sftp is not None:
                sftp.close()
            if transport is not None:
                transport.close()


#class StageFileFromFilesystem(Command):

#    def execute(self,context=None):
#        filehandle = open(context['stage.input.file'], 'rb')
#        filecontents = filehandle.read()
#        filehandle.close()

#        filehandle = open(context['work.dir'], 'wb')
#        filehandle.write(filecontents)
#        filehandle.close()
        
        #need another context variable that points to the staged tar file
#        fu = FileUtilities()

        #find the original filename to use as a base for the
        #tarfile
#        filename = fu.getInputFileName(context['stage.input.file'])
#        filename = filename + ".tar.gz"
#        context['staged.file'] = ('%s/%s') % (context['work.dir'], filename)        
#        return False

#TODO: wrap calls in try/except, report error to xmlrpc if error
class TarFile(Command):
    '''Command that tars the work directory up for the scene located at context['work.dir']/context['input.filename']
    to context['output.dir']/context['input.filename']'''
    
    def execute(self, context):
        Logging(context).log("Executing TarFile() for %s" % context['scene.id'])
                                       
        #fulloutputpath = os.path.join(context['output.dir'], context['input.filename'])
        fulloutputpath = os.path.join(context['output.dir'], context['scene.id'] + '-SR.tar.gz')
        tar = tarfile.open(fulloutputpath, "w:gz")

        os.chdir("%s/../" % context['work.dir'])
            
        #we should now be one level about the actual working directory and should
        #be including the whole work directory, which shares the name of the scene.
        tar.add(context['scene.id'])
        tar.close()

        return False
            
            
#TODO: wrap calls in try/except, report error to xmlrpc if error
class UntarFile(Command):
    '''Command that looks for a context value of "hdfs.input.file" and untars it in
    a location specified by a context value of "work.dir"'''
    
    def execute(self, context):
        Logging(context).log("Executing UntarFile() for %s" % context['scene.id'])
        
        tf = tarfile.open(context.get("staged.file"), "r:gz")
        filename = str(tf.name).split(os.sep)[str(tf.name).count(os.sep)]

        #get the name of the 'work' directory from a configuration setting
        #instead
        targetDir = context.get("work.dir")

        #create dir specifically for the scene before extracting
        tf.extractall(targetDir)
        tf.close()

        #return false to keep the chain moving
        return False
            
#===============================================================================
#End of Commands
#===============================================================================


if __name__ == '__main__':
    #run one of the test classes or whatever else you wish for testing
    pass


