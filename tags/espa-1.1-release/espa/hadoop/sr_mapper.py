#!/usr/bin/env python
import sys
from espa import *





if __name__ == '__main__':
    logfilename = '/tmp/espa.log'

    
    for line in sys.stdin:
        parts = line.split('\t')
       
        orderid = parts[0]
       
        #full path to the scene... for now assume sftp...
        sceneid = parts[1]
        
        #url of the xmlrpc server to report status against
        xmlrpcurl = parts[2]

        #check to make sure we've got a good record before proceeding
        if (not sceneid.startswith('L') or not xmlrpcurl.startswith('http')):
            #print ' '
            continue;
        
        context = {}
        
        context['chain.name'] = 'sr'

        #add this parameter to create a collection instead of on-demand order
        #context['collection.name'] = 'gls'

        context['base.work.dir'] = '/data/espa/work'
        context['base.output.dir'] = '/data/espa/output'

        context['ledaps.anc.path'] = '/usr/local/ledaps/ANC'
        context['ledaps.bin.path'] = '/home/espa/bin/ledaps/bin'
        context['ledaps.executable'] = context['ledaps.bin.path'] + '/do_ledaps.csh'

        #Populated from the incoming hdfs job file
        context['xmlrpcurl'] = xmlrpcurl
        context['scene.id'] = sceneid
        context['order.id'] = orderid

        #These will be populated by the espa job
        context['scene.path'] = ''
        context['scene.row'] = ''
        context['scene.year'] = ''
        context['scene.day'] = ''
        context['scene.sensor'] = ''
        context['scene.status'] = 'On Cache'
        context['scene.completed.location'] = ''
        #End espa populated items
        
        context['input.filename'] = context['scene.id'] + '.tar.gz'
        
        context['hadoop_executable_path'] = '/home/espa/bin/hadoop/bin/hadoop'

        ##############################################################################################
        #Start autoconfiged values
        ##############################################################################################
        #These can be configured here, if they are not espa will pull the values from the orderservice
        #webservice
        context['online.cache.host'] = ''
        context['online.cache.port'] = 22
        context['online.cache.username'] = ''
        context['online.cache.password'] = ''


        context['distribution.cache.host'] = ''
        context['distribution.cache.port'] = 22
        context['distribution.cache.username'] = ''
        context['distribution.cache.password'] = ''

        #Populate to set the http server and base path for file downloads
        context['distribution.cache.home.url'] = ''
        context['distribution.cache.sr.path'] = ''

        context['log.file.name'] = 'espa_' + context['chain.name'] + '_' + context['scene.id'] + '.log'
        context['log.file.path'] = '/tmp/'
        ##############################################################################################
        #End autoconfiged values
        ##############################################################################################
        
        #add in calls to get datasources/pull credentials from db
        
        chain = Chain()
        chain.addCommand(PrepareDirectories())
        chain.addCommand(StageFileFromSFTP())
        chain.addCommand(UntarFile())
        chain.addCommand(Ledaps())
        chain.addCommand(PurgeFiles())
        chain.addCommand(TarFile())
        chain.addCommand(DistributeFileToSFTP())
        chain.addCommand(DistributeSourceL1TToSFTP())
        chain.addCommand(CleanUpDirs())
        chain.addCommand(MarkSceneComplete())

        chain.execute(context)

        #trying this to see if we can get the job to return
        #print " "

    

    
