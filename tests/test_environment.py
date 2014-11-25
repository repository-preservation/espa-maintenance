import unittest
import sys, os

import subprocess

class TestESPAEnvironment1(unittest.TestCase):

    def setUp(self):
        pass
    
    def test_espaenv_envar(self):
        """Checks to see if ESPA_ENV exists in environment"""
        self.assertIsNotNone(os.getenv('ESPA_ENV'))
    
    def test_espaworkdir_envar(self):
        """Checks to see if ESPA_WORK_DIR exists in environment"""
        self.assertIsNotNone(os.getenv('ESPA_WORK_DIR'))
    
    def test_espaxmlrpc_envar(self):
        """Checks to see if ESPA_XMLRPC exists in environment"""
        self.assertIsNotNone(os.getenv('ESPA_XMLRPC'))
    
    def test_ancpath_envar(self):
        """Checks to see if LEDAPS Ancillary path exists in environment"""
        self.assertIsNotNone(os.getenv('ANC_PATH'))
    
class TestESPADependencies(unittest.TestCase):
    """Software dependency checking"""

    def setUp(self):
        # Hard-coded versions for now.  Could use ' rpm -qg ESPA --qf "%{NAME} %{VERSION}" '
        # to dynamically populate dict, but that would only let us interrogate what is currently
        # installed vs. what should be --- this still TBD
        
        self.sw_deps = {
                        'cfmask':                   '1.1.2',
                        'dans-gdal-scripts-espa':   '0.16',
                        'dem':                      '1.1.1',
                        'gdal-espa':                '1.9.1',
                        'ledaps':                   '1.3.0',
                        'python-espa':              '2.7.1',
                        'spectral-indices':         '1.1.2',
                        'swe':                      '1.0.0'
        }
        
        # LEDAPS Ancillary directory path and EP/TOMS and NCEP dir list
        self.paths = {
                        'LEDAPSANC':                '/usr/local/ledaps/ANC/',
                        'LEDAPSANCDIRS':            ['REANALYSIS','EP_TOMS'],
                        'PYTHONBIN':                '/usr/bin/python',
                        'RPMBIN':                   '/bin/rpm'
        }
    
        # Python module name and import module name map
        self.python_modules = {
                        'suds':                     ['suds'],
                        'pycrypto':                 ['pycryptopp'],
                        'setuptools':               ['setuptools'],
                        'paramiko':                 ['paramiko'],
                        'httplib2':                 ['httplib2'],
                        'dwins':                    ['geoserver'],
                        'GDAL':                     ['gdal'],
                        'MySQL':                    ['MySQLdb'],
                        'Django':                   ['django','django.core.handlers.wsgi'],
                        'memcached':                ['memcache'],
                        'scipy':                    ['scipy'],
                        'numpy':                    ['numpy'],
                        'pymongo':                  ['pymongo'],
                        'pexpect':                  ['pexpect'],
                        'bottle':                   ['bottle'],
                        'nose':                     ['nose'],
                        'dateutil':                 ['dateutil'],
                        'gviz_api_py':              ['gviz_api_py'],
                        'Paste':                    ['Paste']
        }
    
    def pythonModuleCheck(self, module):
        """Internal method to check python module importing"""
        r = subprocess.call("%s -m %s &> /dev/null" % (self.paths['PYTHONBIN'], module), shell=True)
        
        if r == 0:
            return True
        else:
            return False
    
    def packageCheck(self, package):
        """Internal method to check if package is installed/exists"""
        r = subprocess.call("/bin/rpm ")
        
        if r == 0:
            return True
        else:
            return False
    
    def test_gdal_exists(self):
        """Check it GDAL is installed"""
        pass

    def test_proj_exists(self):
        """Check if PROJ.4 is installed"""
        pass

    def test_proj_definitions_exists(self):
        """Check if PROJ.4 has needed projections"""
        pass

    def test_python_exists(self):
        """Check if Python is available"""
        self.assertTrue(subprocess.call("/bin/rpm -q spectral-indices &> /dev/null", shell=True))
        pass

    def test_ledaps_exists(self):
        """Check if ledaps is installed"""
        self.assertTrue(subprocess.call("/bin/rpm -q spectral-indices &> /dev/null", shell=True))
        pass

    def test_ledaps_auxillary_exists(self):
        """Check if LEDAPS Aux data is available"""
        for directory in self.paths['LEDAPSANCDIRS']:
            self.assertTrue(os.path.isdir(directory))

    def test_cfmask_exists(self):
        """Check if CFMask is available"""
        self.assertTrue(subprocess.call("/bin/rpm -q spectral-indices &> /dev/null", shell=True))

    def test_spectral_indices_exists(self):
        """Check if SI is available"""
        self.assertTrue(subprocess.call("/bin/rpm -q spectral-indices &> /dev/null", shell=True))

    def test_suds_exists(self):
        """Check if suds is available"""
        for m in self.python_modules['suds']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_setuptools_exists(self):
        """Check if setuptools is available"""
        for m in self.python_modules['setuptools']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_pycrypto_exists(self):
        """Check if pycrypto is available"""
        for m in self.python_modules['pycrypto']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_paramiko_exists(self):
        """Check if paramiko is available"""
        for m in self.python_modules['paramiko']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_httplib2_exists(self):
        """Check if httplib2 is available"""
        for m in self.python_modules['httplib2']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_dwins_exists(self):
        """Check if dwins-gsconfig is available"""
        for m in self.python_modules['dwins']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_GDAL_exists(self):
        """Check if Python-GDAL is available"""
        for m in self.python_modules['GDAL']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_numpy_exists(self):
        """Check if numpy is available"""
        for m in self.python_modules['numpy']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_pymongo_exists(self):
        """Check if pymongo is available"""
        for m in self.python_modules['pymongo']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_pexpect_exists(self):
        """Check if pexpect is available"""
        for m in self.python_modules['pexpect']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_MySQL_exists(self):
        """Check if Python-MySQL is available"""
        for m in self.python_modules['MySQL']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_Django_exists(self):
        """Check if Django is available"""
        for m in self.python_modules['Django']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_memcached_exists(self):
        """Check if python-memecached is available"""
        for m in self.python_modules['memcached']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_bottle_exists(self):
        """Check if bottle is available"""
        for m in self.python_modules['bottle']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_nose_exists(self):
        """Check if nose is available"""
        for m in self.python_modules['nose']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_dateutil_exists(self):
        """Check if python-dateutil is available"""
        for m in self.python_modules['dateutil']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_gviz_api_py_exists(self):
        """Check if gviz_api_py is available"""
        for m in self.python_modules['gviz_api_py']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_Paste_exists(self):
        """Check if Paste is available"""
        for m in self.python_modules['Paste']:
            self.assertTrue(self.pythonModuleCheck(m))

    def test_scipy_exists(self):
        """Check if scipy is available"""
        for m in self.python_modules['scipy']:
            self.assertTrue(self.pythonModuleCheck(m))


class TestESPANetwork(unittest.TestCase):
    """Network related tests for ESPA"""
    
    def setUp(self):
        pass

    def test_read_online_cache(self):
        """Check read access to online cache"""
        pass

    def test_write_online_cache(self):
        """Check write access to online cache"""
        pass
   
    def test_nfs_mounts(self):
        """Check NFS mount is available"""
        pass


def main():
    unittest.main()
    suite = unittest.TestLoader().loadTestsFromTestCase("TestESPAEnvironment1")
    
    unittest.TextTestResult(suite)

if __name__ == '__main__':
    #unittest.main()
    #suite = unittest.TestLoader().loadTestsFromTestCase(TestESPAEnvironment)
    #unittest.TextTestResult().run(suite)
    
    main()

#suite = unittest.TestLoader().loadTestsFromTestCase(TestESPAEnvironment)
#unittest.TextTestRunner(verbosity=2).run(suite)
