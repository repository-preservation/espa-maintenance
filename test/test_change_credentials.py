#!/usr/bin/env python

import unittest

from mock import patch
from attrdict import AttrDict
from maintenance import change_credentials

class TestChangeCredentials(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('argparse.ArgumentParser.parse_args', lambda x: AttrDict([('username', ['bilbo', 'baggins']), ('configfile', '/home/cfgnfo')]))
    def test_arg_parser(self):
        result = change_credentials.arg_parser()
        self.assertEqual(result, ('bilbo', '/home/cfgnfo'))

    def test_gen_password(self):
        result = change_credentials.gen_password()
        self.assertEqual(type(result), str)
        self.assertEqual(len(result), 16)

        
