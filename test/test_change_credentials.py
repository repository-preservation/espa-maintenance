#!/usr/bin/env python

import unittest
import os

from mock import patch
from attrdict import AttrDict
from maintenance import change_credentials, utils

cfg_path = os.environ.get('ESPA_CONFIG_PATH', '')
db_info = utils.get_cfg(cfg_path, section='config')
default_pwd = 'default'

class TestChangeCredentials(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @patch('argparse.ArgumentParser.parse_args', lambda x: AttrDict([('username', ['bilbo', 'baggins']), ('configfile', cfg_path)]))
    def test_arg_parser(self):
        result = change_credentials.arg_parser()
        self.assertEqual(result, ('bilbo', cfg_path))

    def test_gen_password(self):
        result = change_credentials.gen_password()
        self.assertEqual(type(result), str)
        self.assertEqual(len(result), 16)

    def test_update_db(self):
        new_password = 'foobar'
        result = change_credentials.update_db(new_password, db_info)
        self.assertTrue(result)
        # reset
        reset = change_credentials.update_db(default_pwd, db_info)
        self.assertTrue(reset)

    def test_current_pass(self):
        result = change_credentials.current_pass(db_info)
        self.assertEqual(result, default_pwd)

    def test_get_addresses(self):
        result = change_credentials.get_addresses(db_info)
        self.assertEqual(result, (['username@emailhost'], ['system@mail_address']))

    @patch('argparse.ArgumentParser.parse_args', lambda x: AttrDict([('username', ['bilbo', 'baggins']), ('configfile', cfg_path)]))
    @patch('maintenance.change_credentials.change_pass', lambda x: "foo")
    @patch('maintenance.utils.send_email', lambda a, b, c, d: (a, b, c, d))
    def test_run(self):
        result = change_credentials.run()
        self.assertEqual(result, "User: bilbo password has been updated")
