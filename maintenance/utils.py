import smtplib
from email.mime.text import MIMEText
import ConfigParser
import os
import datetime
import subprocess

from dbconnect import DBConnect
import paramiko


def get_cfg(cfg_path=None):
    """
    Retrieve the configuration information from the .cfgnfo file
    located in the current user's home directory

    :param cfg_path: Path to the configuration file to use, defaults to
        '/home/<user>/.usgs/.cfgnfo'
    :return: dict represention of the configuration file
    """
    if not cfg_path:
        cfg_path = os.path.join(os.path.expanduser('~'), '.usgs', '.cfgnfo')

    cfg_info = {}
    config = ConfigParser.ConfigParser()
    config.read(cfg_path)

    for sect in config.sections():
        cfg_info[sect] = {}
        for opt in config.options(sect):
            cfg_info[sect][opt] = config.get(sect, opt)

    return cfg_info


def send_email(sender, recipient, subject, body):
    """
    Send out an email to give notice of success or failure

    :param sender: who the email is from
    :type sender: string
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


def backup_cron():
    """
    Make a backup of the current user's crontab
    to /home/~/backups/
    """
    bk_path = os.path.join(os.path.expanduser('~'), 'backups')
    if not os.path.exists(bk_path):
        os.makedirs(bk_path)

    ts = datetime.datetime.now()
    cron_file = ts.strftime('crontab-%m%d%y-%H%M%S')

    with open(os.path.join(bk_path, cron_file), 'w') as f:
        subprocess.call(['crontab', '-l'], stdout=f)


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


def fetch_web_log(dbinfo, remote_path, local_path, env):
    """
    Copy the web log file from a remote host to the local host
    for processing

    :param dbinfo: DB configuration
    :param remote_path: path on the remote to copy
    :param local_path: local path to place the copy
    :param env: dev/tst/ops
    """
    username = get_config_value(dbinfo, 'landsatds.username')
    password = get_config_value(dbinfo, 'landsatds.password')
    host = get_config_value(dbinfo, 'url.{}.webtier'.format(env))

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client.connect(host, username=username, password=password, timeout=60)
    sftp = client.open_sftp()

    sftp.get(remote_path, local_path)
