#!/usr/bin/env python
import sys

from espacollections import *

if __name__ == '__main__':
    logfilename = '/tmp/espa.log'

    
    for line in sys.stdin:
        parts = line.split('\t')
       
        #full path to the scene... for now assume sftp...
        sceneid = parts[0]
        
        #url of the xmlrpc server to report status against
        #xmlrpcurl = parts[1]

        #check to make sure we've got a good record before proceeding
        if (not sceneid.startswith('L')): #or not xmlrpcurl.startswith('http')):
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
        context['hadoop_executable_path'] = '/home/espa/bin/hadoop/bin/hadoop'

        ##############################################################################################
        #Start autoconfiged values
        ##############################################################################################
        #These can be configured here, if they are not espa will pull the values from the orderservice
        #webservice
        context['scene.id'] = sceneid
        context['create_as_collection'] = 'gls-2010'
        context['gls.year'] = '2010'
        context['product.filename.suffix'] = 'sr'
        context['distribute.sourcefile'] = 'no'
        
       
        ##############################################################################################
        #End autoconfiged values
        ##############################################################################################
        
        logger = LocalLogger(context)    
        
        ds = GLSDataSource(logger, context, 'espa', '3ew23ew2#EW@#EW@', 'edcsns7.cr.usgs.gov', 22)
        dds = DistributionDataSource(logger, context, 'espa', '4re34re3$RE#$RE#', 'edclxs70.cr.usgs.gov', 22)
        
        chain = Chain(name='sr', logger=logger)
        chain.addCommand(PrepareDirectories(logger))
        chain.addCommand(StageSceneFromLandsat(logger,ds))
        chain.addCommand(UntarFile(logger))
        chain.addCommand(Ledaps(logger))
        chain.addCommand(PurgeFiles(logger))
        chain.addCommand(TarFile(logger))
        chain.addCommand(DistributeProductToSFTP(logger, dds))
        #chain.addCommand(DistributeSourceToSFTP(logger, dds))
        chain.addCommand(CleanUpDirs(logger))
        chain.execute(context)

    
    

    
