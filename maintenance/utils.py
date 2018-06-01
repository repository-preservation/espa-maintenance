import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import ConfigParser
import os
import datetime

import paramiko
from plumbum.machines.paramiko_machine import ParamikoMachine

from dbconnect import DBConnect


CONF_FILE = cfg_path = os.path.join(os.path.expanduser('~'), '.cfgnfo')

def get_cfg(cfg_path=None, section=''):
    """
    Retrieve the configuration information from the .cfgnfo file
    located in the current user's home directory

    :param cfg_path: Path to the configuration file to use, defaults to
        '/home/<user>/.usgs/.cfgnfo'
    :return: dict represention of the configuration file
    """
    if not cfg_path:
        cfg_path = CONF_FILE

    if not os.path.exists(cfg_path):
        print('! DB configuration not found: {c}'.format(c=cfg_path))
        sys.exit(1)

    cfg_info = {}
    config = ConfigParser.ConfigParser()
    config.read(cfg_path)

    for sect in config.sections():
        cfg_info[sect] = {}
        for opt in config.options(sect):
            cfg_info[sect][opt] = config.get(sect, opt)

    if section:
        if section not in cfg_info:
            print('! Section {s} not found in {c}'.format(s=section, c=cfg_path))
            sys.exit(1)
        cfg_info = cfg_info[section]

    return cfg_info


def send_email(sender, recipient, subject, body, html=None):
    """Send out an email to give notice of success or failure.

    Args:
        sender (list): who the email is from
        recipient (list): list of recipients of the email
        subject (str): subject line of the email
        body (str): success or failure message to be passed
        html (bool): whether the content is HTML
    """
    # This does not need to be anything fancy as it is used internally,
    # as long as we can see if the script succeeded or where it failed
    # at, then we are good to go
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject

    # Expecting tuples from the db query
    msg['From'] = ', '.join(sender)
    msg['To'] = ', '.join(recipient)

    # Format email according to RFC 2046
    part = MIMEText(body, 'plain')
    msg.attach(part)
    if html is not None:
        part = MIMEText(html, 'html')
        msg.attach(part)

    smtp = smtplib.SMTP("localhost")
    smtp.sendmail(sender, recipient, msg.as_string())
    smtp.quit()


def get_email_addr(dbinfo, who):
    """
    Retrieve email address(es) from the database
    for a specified role
    """
    key = 'email.{0}'.format(who)
    sql = 'select value from ordering_configuration where key = %s'

    with DBConnect(**dbinfo) as db:
        db.select(sql, key)
        out = db[0][0].split(',')

    return out


def get_config_value(dbinfo, key):
    """
    Retrieve a specified configuration value

    :param dbinfo: DB connection information
    :param key: table key to get the value for
    :return: value
    """

    sql = ('SELECT value from ordering_configuration '
           'WHERE key = %s')

    with DBConnect(**dbinfo) as db:
        db.select(sql, key)
        ret = db[0][0]

    return ret


def query_connection_info(dbinfo, env):
    """
    Copy the web log file from a remote host to the local host
    for processing

    :param dbinfo: DB configuration
    :param env: dev/tst/ops
    :return: dict of username, password, host, port
    """
    username = get_config_value(dbinfo, 'landsatds.username')
    password = get_config_value(dbinfo, 'landsatds.password')
    log_locations = get_config_value(dbinfo, 'url.{}.weblogs'.format(env)).split(',')
    return {'username': username, 'password': password, 'log_locs': log_locations}


class RemoteConnection():
    def __init__(self, host, user, password=None, port=22):
        """
        Initialize connection to a remote host

        :param host: hostname to connect to
        :param user: username to connect as
        :param password: the password of the user
        :param port: the port number on the host machine
        """
        self.host, self.user, self.port = host, user, port
        self.remote = ParamikoMachine(self.host, user=self.user, password=password, port=self.port,
                                      missing_host_policy=paramiko.AutoAddPolicy())

    def list_remote_files(self, remote_dir, prefix):
        """
        List files in folder on a remote host which start with a given prefix

        :param remote_dir: the absolute location of the folder to search
        :param prefix: the beginning of all files names to find
        :return: list of remote full paths
        """
        r_ls = self.remote['ls']
        files = r_ls(remote_dir).split('\n')

        if len(files):
            files = [os.path.join(remote_dir, f) for f in files if f.startswith(prefix)]
            return files
        else:
            raise ValueError('No files found at {host}:{loc}'.format(host=self.host, loc=remote_dir))

    def download_remote_file(self, remote_path, local_path):
        """
        Transfer a file from a remote host locally

        :param remote_path: the absolute location of the file to grab (including host)
        :param local_path: the local file to write to
        """
        self.remote.download(remote_path, local_path)


def subset_by_date(files, begin, stop, tsfrmt='%Y%m%d.gz'):
    """
    Find files that are within the timestamp range

    :param files: list of file names which have a timestamp
    :param begin: date to begin searching
    :param stop: date to stop searching
    :param tsfrmt: How to parse the timestamp
    :return: list
    """
    def parser(x):
        return datetime.datetime.strptime(os.path.basename(x).split('-')[-1], tsfrmt).date()

    def criteria(x):
        # This assumes every month on the first, the previous month's
        # logs will be archived. Searching for 11/1-11/30? Needs 11/2-12/1 logs!
        return ((x[1] <= stop + datetime.timedelta(days=1))
                & (x[1] >= begin))

    timestamps = map(parser, files)
    return [x[0] for x in filter(criteria, zip(files, timestamps))]
