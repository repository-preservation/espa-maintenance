import unittest
from espa import *
import os
import sys

class EspaUnitTests(unittest.TestCase):

    
    def testGetLogFile(self):
        context = {}
        context['logfilename'] = '/tmp/testlogfile.log'
        logger = Logging(context)
        logger.getLogFile()
        self.assertIsNotNone(logger)
        #self.assertEquals(context['logfilename'], logger.getLogFile())
        self.assertTrue(os.path.exists(context['logfilename']))

    def testGetDefaultLogFile(self):
        context = {}
        logger = Logging()
        self.assertIsNotNone(logger)
        logger.getLogFile()
        self.assertTrue(os.path.exists('/tmp/espa.log'))
        
    def testLog(self):
        self.failUnless(False)

    def testExecuteCommand(self):
        self.failUnless(False)

    def testAddCommandToChain(self):
        self.failUnless(False)

    def testExecuteChain(self):
        self.failUnless(False)

    def testAddCommandToPersistentChain(self):
        self.failUnless(False)

    def testExecutePersistentChain(self):
        self.failUnless(False)

    def testAddCommandToReportableChain(self):
        self.failUnless(False)

    def testExecuteReportableChain(self):
        self.failUnless(False)

    def testCleanUpDirsCommand(self):
        self.failUnless(False)

    def testDistributeFileToSFTPCommand(self):
        self.failUnless(False)

    def testDistributeFileToFilesystemCommand(self):
        self.failUnless(False)

    def testLedapsCommand(self):
        self.failUnless(False)
        
    def testPrepareDirectoriesCommand(self):
        self.failUnless(False)

    def testPurgeFilesCommand(self):
        self.failUnless(False)

    def testReportStatusCommand(self):
        self.failUnless(False)

    def testStageFileFromSFTPCommand(self):
        self.failUnless(False)

    def testStageFileFromFilesystemCommand(self):
        self.failUnless(False)

    def testTarFileCommand(self):
        self.failUnless(False)

    def testUntarFileCommand(self):
        self.failUnless(False)

    def testFileUtilities(self):
        self.failUnless(False)

def main():
    unittest.main()

if __name__ == '__main__':
    main()
   
    
