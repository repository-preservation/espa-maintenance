import sys
import smtplib
from email.mime.text import MIMEText
import ConfigParser
import os
import datetime

import paramiko

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


def send_email(sender, recipient, subject, body):
    """
    Send out an email to give notice of success or failure

    :param sender: who the email is from
    :type sender: list
    :param recipient: list of recipients of the email
    :type recipient: list
    :param subject: subject line of the email
    :type subject: string
    :param body: success or failure message to be passed
    :type body: string
    """
    # This does not need to be anything fancy as it is used internally,
    # as long as we can see if the script succeeded or where it failed
    # at, then we are good to go
    msg = MIMEText(body)
    msg['Subject'] = subject

    # Expecting tuples from the db query
    msg['From'] = ', '.join(sender)
    msg['To'] = ', '.join(recipient)

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


def find_remote_files_sudo(user, password, remote_dirs, prefix, port=22):
    """
    List files in folder on a remote host which start with a given prefix (`sudo ls`)

    :param user: username to connect as (must have sudo rights)
    :param password: the password of the user
    :param port: the port number on the host machine
    :param remote_dirs: the absolute location of the folder to search (including host)
    :param prefix: the beginning of all files names to find
    :return: list of remote full paths
    """
    files = []
    for rloc in remote_dirs:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host, remote_dir = rloc.split(':')
        ssh.connect(host, username=user, password=password, port=port, timeout=60)
        transport = ssh.get_transport()
        session = transport.open_session()
        session.set_combine_stderr(True)
        session.get_pty()

        # for testing purposes we want to force sudo to always to ask for password. because of that we use "-k" key
        session.exec_command('sudo -k ls "{remote}"'.format(remote=remote_dir))
        stdin = session.makefile('wb', -1)
        stdout = session.makefile('rb', -1)
        # you have to check if you really need to send password here
        stdin.write(password + '\n')
        stdin.flush()

        lines = stdout.read().splitlines()
        for line in lines:
            if line.startswith(prefix) and 'access_log' in line and line.endswith('.gz'):
                files.append('{h}:{p}'.format(h=host, p=os.path.join(remote_dir, line)))

    if len(files):
        return files
    else:
        print('\n'.join(lines))
        raise ValueError('No files found at {0}:{1}'.format(host, os.path.join(remote_dir, prefix)))


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


def download_remote_file_sudo(user, password, remote_paths, local_path, port=22):
    """
    Transfer a file from a remote host locally via SSH transfer (`sudo cat`)

    :param user: username to connect as (must have sudo rights)
    :param password: the password of the user
    :param port: the port number on the host machine
    :param remote_paths: the absolute location of the file to grab (including host)
    :param local_path: the local file to write to
    """
    # A lot of this code is nearly identical to `find_remote_files_sudo`
    # because it requires a new session for every `exec_command` call
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    host, remote_path = remote_paths.split(':')

    ssh.connect(host, username=user, password=password, port=port, timeout=60)
    transport = ssh.get_transport()
    with transport.open_channel(kind='session') as session:
        session.get_pty()

        session.exec_command('sudo -k cat {0} > {1} && wc -c {1}'.format(remote_path, os.path.basename(remote_path)))
        stdin = session.makefile('wb', -1)
        stdout = session.makefile('rb', -1)
        stdin.write(password + '\n')
        stdin.flush()
        nbytes = int(stdout.readlines()[-1].split()[0])

    transport = ssh.get_transport()
    with transport.open_channel(kind='session') as session:
        session.exec_command('cat {0} && rm {0}'.format(os.path.basename(remote_path)))
        while True:
            if session.recv_ready():
                break
        stdout_data = []
        try:
            part = session.recv(4096)
            while part:
                stdout_data.append(part)
                part = session.recv(nbytes)
        except:
            raise

    if os.path.isdir(local_path):
        local_path = os.path.join(local_path, '{h}_{p}'.format(h=host, p=os.path.basename(remote_paths)))
    with open(local_path, 'w') as fid:
        fid.write(''.join(stdout_data))
