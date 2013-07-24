import unittest
import sys, os

class TestESPAEnvironment(unittest.TestCase):

    def setUp(self):
        #self.seq = range(10)
        pass

    def test_espa_env(self):
        '''Check if ESPA_ENV is set.

           ESPA_ENV controls which urls
           are used when calling into LTA'''
        
        self.assertIsNotNone(os.getenv("ESPA_ENV"))

class TestESPADependencies(unittest.TestCase):
    """Software dependency checking"""
    def setUp(self):
        pass

    def test_gdal_exists(self):
        """Check it GDAL is installed"""
        pass

    def test_proj_exists(self):
        """Check if PROJ.4 is installed"""
        pass

    def test_proj_definitions_exist(self):
        """Check if PROJ.4 has needed projections"""
        pass

    def test_ledaps_exists(self):
        """Check if PROJ.4 has needed projections"""
        pass

    def test_ledaps_auxillary_exists(self):
        """Check if LEDAPS Aux data is available"""
        pass

    def test_cfmask_exists(self):
        """Check if CFMask is available"""
        pass

    def test_spectral_indices_exists(self):
        """Check if SI is available"""
        pass

class TestESPANetwork(unittest.TestCase):
    """
    Network related tests for ESPA
    """
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

    
if __name__ == '__main__':
    unittest.main()
#suite = unittest.TestLoader().loadTestsFromTestCase(TestESPAEnvironment)
#unittest.TextTestRunner(verbosity=2).run(suite)
