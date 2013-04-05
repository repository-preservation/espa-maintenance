#!/usr/bin/env python
import sys
#from espa.espa import Command,Chain,PrepareDirectories,StageFileFromSFTP,UntarFile
#from espa.espa import Ledaps,PurgeFiles,TarFile,DistributeFileToSFTP,CleanUpDirs,MarkSceneComplete,DistributeSourceL1TToSFTP
import cStringIO
import xml.etree.ElementTree as xml


if __name__ == '__main__':
    output = set()
 
    for line in sys.stdin:
        output.add(line.strip())
        #print line.strip()
        
    for item in output:
        print item.strip()

    

        

    

    
