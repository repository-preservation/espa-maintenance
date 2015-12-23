import smtplib
from email.mime.text import MIMEText
import ConfigParser
import os
import datetime
import subprocess

from dbconnect import DBConnect


def get_cfg():
    """
    Retrieve the configuration information from the .cfgnfo file
    located in the current user's home directory

    :return: dict
    """
    cfg_path = os.path.join(os.path.expanduser('~'), '.cfgnfo')

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
    msg['From'] = sender
    msg['To'] = ', '.join(recipient)

    smtp = smtplib.SMTP("localhost")
    smtp.sendmail(sender, recipient, msg.as_string())
    smtp.quit()


def get_email_addr(dbinfo, who):
    sql = "select value from configuration where key = 'email.%s'"

    with DBConnect(**dbinfo) as db:
        db.select(sql, who)
        out = db[0][0]

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
