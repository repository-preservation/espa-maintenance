#!/usr/bin/env python
#
#
#
# Name: change_credentials.py
#
# Description: Changes credentials supplied for username and updates configuration table
#              in Django view in ESPA admin.  Right now it needs to run on the same host
#              where the MySQL database lives for ESPA Django framework.
#
#              Although this isn't perfect, you should really schedule it a week or better
#              away from your actual password expiration.
#
#              An example is:
#
#                User 'x' password expires on 09/11/2013 and frequency change is every 60
#                days.
#
#              You should this up in cron to run every other month but change on the 1st
#              day of every month.
#
#              Case in point:  Schedule this manually around your last password change
#              with some buffer days in there, then schedule it out in cron and you should
#              be good to go.
#
# Author: Adam Dosch
#
# Date: 08-02-2013
#
##########################################################################################
# Change        Date            Author              Description
##########################################################################################
#  001          08-02-2013      Adam Dosch          Initial release
#  002          08-05-2013      Adam Dosch          Working on e-mail functionality for
#                                                   to e-mail out on error or success
#  003          09-04-2013      Adam Dosch          Adding functionality to auto-update
#                                                   crontab for next scheduled run to do
#                                                   credential changing.
#                                                   Adding -f option for frequency of the
#                                                   credential change (in days)
#                                                   Adding function update_crontab() to do
#                                                   cron updating for user running script
#                                                   which will always assumed to be the
#                                                   user who needs their creds changed
#  004          10-29-2013      Adam Dosch          Typo in -u options for user selection
#  005          10-30-2013      Adam Dosch          Missing import for commands module! WTF
#                                                   Missing import for datetime module! WTF
#                                                   Removing some debugging code.
#  006          09-11-2014      Adam Dosch          Adding 'espatst' as a valid username
#                                                   argument
#                                                   Adding fix for homedir in crontab gen
#  007          11-01-2014      Adam Dosch          Fixing credentials stomping updates
#
##########################################################################################

__author__ = "Adam Dosch"

import os
import sys

import platform

import pexpect

import MySQLdb

from string import digits, lowercase, uppercase
import random

import argparse

# for mail out
import smtplib

try:
    # Python 2.[67].x
    from email.mime.multipart import MIMEMultipart
except ImportError:
    # Python 2.4.x
    from email.MIMEMultipart import MIMEMultipart

try:
    # Python 2.[67].x
    from email.mime.text import MIMEText
except ImportError:
    # Python 2.4.x
    from email.MIMEText import MIMEText

from email.Header import Header
from email.Utils import parseaddr, formataddr

import time

import commands

import datetime

verbose = False

# E-mail Recipients/Subject
email_from = 'espa@espa.cr.usgs.gov'
#email_from = 'espa'
email_to = ['adosch@usgs.gov']
#email_to = ['adosch@usgs.gov']

email_subject = "LSRD - Auto-credential changing"

def send_email(sender, recipient, subject, body):

    # Header class is smart enough to try US-ASCII, then the charset we
    # provide, then fall back to UTF-8.
    header_charset = 'ISO-8859-1'

    # We must choose the body charset manually
    for body_charset in 'US-ASCII', 'ISO-8859-1', 'UTF-8':
        try:
            body.encode(body_charset)
        except UnicodeError:
            pass
        else:
            break

    # Split real name (which is optional) and email address parts
    sender_name, sender_addr = parseaddr(sender)
    recipient_name, recipient_addr = parseaddr(recipient)

    # We must always pass Unicode strings to Header, otherwise it will
    # use RFC 2047 encoding even on plain ASCII strings.
    sender_name = str(Header(unicode(sender_name), header_charset))
    recipient_name = str(Header(unicode(recipient_name), header_charset))

    # Make sure email addresses do not contain non-ASCII characters
    sender_addr = sender_addr.encode('ascii')
    recipient_addr = recipient_addr.encode('ascii')

    # Create the message ('plain' stands for Content-Type: text/plain)
    msg = MIMEText(body.encode(body_charset), 'plain', body_charset)
    msg['From'] = formataddr((sender_name, sender_addr))
    msg['To'] = formataddr((recipient_name, recipient_addr))
    msg['Subject'] = Header(unicode(subject), header_charset)

    # Send the message via SMTP to localhost:25
    smtp = smtplib.SMTP("localhost")
    smtp.sendmail(sender, recipient, msg.as_string())
    smtp.quit()

def update_crontab(frequency, user, backDate=True):
    """
    Update crontab to schedule new cron entry for next password change for account.
    
    NOTE:  Whatever frequency passed in, it will be subtracted by two days by default unless
           you set backDate=False
    """
    
    tmpfile = "/tmp/c.out"
    
    # Dump current crontab to a temp file
    (retval, output) = commands.getstatusoutput("crontab -l > %s" % tmpfile)
    
    # Read in the crontab dump temp file
    try:
        f = open(tmpfile, "r")
        
        data = f.readlines()
        
        f.close()
        
    except OSError, e:
        print "Error: ", e
    
    # Calculate new frequency if backDate is True:
    if backDate:
        newfrequency = frequency - 2
    else:
        newfrequency = frequency
    
    # Loop through crontab and see if we find our change_credentials line.
    # if we do, update it
    for idx, cronline in enumerate(data):
        if "change_credentials.py" in cronline:
            (month, day) = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(days=newfrequency), "%m|%d").split("|")
            
            data[idx] = "00 05 %s %s * /usr/local/bin/python /home/%s/espa-site/tools/change_credentials.py -u %s -f %s\n" % (day, month, user, user, 60)
    
    # Re-write out new temp file with any crontab updates we did above
    try:
        f = open(tmpfile, "w")
        
        for line in data:
            f.writelines(line)
        
        f.close()
        
    except OSError, e:
        print "Error: ", e

    # Update c
    (retval, output) = commands.getstatusoutput("crontab %s && rm %s" % (tmpfile, tmpfile))

def change_password(user, current_passwd, new_passwd, command):
    """
    Changing password for user using 'passwd' binary call-out.
    
    Works for local accounts and LDAP accounts so far.
    """
    
    # Starting off password change on shell
    try:
        # Hackery regex password string
        pass_auth_string = '.*[Pp]assword: ' #NOTE: space at end of string
        
        # Incoming password change regex string
        change_pass_string = "Changing password for user.*"
        
        # Spawn 'passwd' command to start passwd change
        child = pexpect.spawn(command)
        
        # Let's validate initial expect
        i = child.expect([pexpect.TIMEOUT, change_pass_string, pass_auth_string])
    
        # Send our current password --- since that's what passwd will ask us for first
        child.sendline(current_passwd)
    except pexpect.ExceptionPexpect, e:
        return False
    
    # We expect to be asked for our new pasword, lets offer it up
    try:
        child.expect('(New UNIX|New) [Pp]assword: ')
        child.sendline(new_passwd)
    except pexpect.ExceptionPexpect, e:
        return False
    
    # We expect to be asked to re-enter our password, lets offer it up again
    try:
        child.expect('([Rr]etype new UNIX|[Rr]e-enter new|[Rr]etype new) [Pp]assword:')
        child.sendline(new_passwd)
    except pexpect.ExceptionPexpect, e:
        return False
    
    # Since we cant rely on retval of 'passwd', if we don't see some sort of 'successfully changed/updated' message, then we assume that it failed
    try:
        r = child.expect([pexpect.TIMEOUT,'.*(updated)* successfully( changed|\.)*'])
    except pexpect.ExceptionPexpect, e:
        return False
        
    if r == 0:
        # Failure --- fucked, should be 1, NOT 0
        if verbose:
            print "Password change FAILED!"
            print "---------debug---------"
            print str(child)
            print "-----------------------"
            print child.before, child.after
        
        return False

    if r == 1:
        # Success --- fucked, should be 0, NOT 1
        child.sendline('exit')
        
        return True

    # Always return false if we don't trigger any logic
    return False

def containsAny(str, set):
    # Checks list to see if any values exist in it
    for c in set:
        if c in str: return 1
    return 0

def genpass():
    """
    Generate random password for account
    """

    s = ""
    
    # Password length requirement
    length = 12
    
    # Special character requirement
    specials = "!@#$%^&*()"
    
    # Generate password --- looping until we get at least one upper/lower/special and digit at the proper length
    while (not containsAny(s, specials)) or (not containsAny(s, digits)) or (not containsAny(s, uppercase)) or (not containsAny(s, lowercase)):
        s = ""
        
        for i in range(length):
            s += random.choice(digits + uppercase + lowercase + specials)
        
    return str(s)

def connect_db(host, user, password, db, port=3306):
    """
    Connecting to a MySQL database
    """
    
    try:
        return MySQLdb.connect(host=host, port=port, user=user, passwd=password, db=db)
    except MySQLdb.Error, e:
        sys.stderr.write("[ERROR] %d: %s\n" % (e.args[0], e.args[1]))
        
    return False

def main():
    
    # When testing, make a copy of the ordering_configuration table and call it ordering_configuration_test
    # or some shit and test against that
    TABLE = "ordering_configuration"
    
    #Set up option handling
    parser = argparse.ArgumentParser(description="Changes credentials supplied for -u/--username and updated Django configuration table for ESPA admin site.  Right now it needs to run on the same host where the MySQL database lives for ESPA.  This script will also auto-update a crontab for the user running this")
    
    parser.add_argument("-u", "--username", action="store", nargs=1, dest="username", choices=['espa','espadev','espatst'], help="Username to changed credentials for (e.g. [espa|espadev|espatst] )")
    parser.add_argument("-f", "--frequency", action="store", type=int, default=60, dest="frequency", help="Frequency (in days) to change the following credentials")
    
    parser.add_argument("-v", "--verbose", action='store_true', dest="verbose", default=False, help=argparse.SUPPRESS)
    
    # Parse those options!
    args = parser.parse_args()

    # If nothing is passed, print argparse help at a minimum
    if len(sys.argv) - 1 == 0:
        parser.print_help()
        sys.exit(1)

    # Set verbose status if we made it this far
    global verbose    
    verbose = args.verbose

    # Username
    username = "".join(args.username)

    # Lets append the username of creds we are changing to e-mail subject (in case we need to send one out)
    global email_subject
    email_subject = email_subject + " - " + username

    # Define creds dict
    creds = {}
    
    # Lets check and see if our dbcreds file exists?
    # This is where we will know what DB environment to update
    credfile = "/home/%s/.dbnfo" % username
    if os.path.isfile(credfile):
        try:
            # open and read data from creds file
            f = open(credfile, "r")
        
            data = f.readlines()
            
            f.close()
            
        except Exception, e:
            err = "Problems opening up cred file for processing, BAILING!  Password changes for '%s' didn't happen on %s." % (username, platform.node())
            send_email(email_from, email_to, email_subject + " - Error", err)
            print err
            sys.exit(1)
        
        # stuff creds in a dict
        for line in data:
            (k, v) = line.split("=")
            creds[k] = v.strip("\n")
    else:
        err = "DB creds file doesn't exist, BAILING!  Password changes for '%s' didn't happen on %s." % (username, platform.node())
        send_email(email_from, email_to, email_subject + " - Error", err)
        print err
        sys.exit(1)


    # Connect to MySQL DB
    db_conn = connect_db(host=creds["h"], user=creds["u"], password=creds["p"], db=creds["d"])

    if not db_conn:
        err = "Cound not establish connection to the DB on %s!  Either the username, password or database options are muffed or you're locked out." % (platform.node())
        send_email(email_from, email_to, email_subject + " - Error", err)
        print err
        sys.exit(1)
    
    # Establish DB cursor
    cursor = db_conn.cursor()
 
    # Let's nab our current password from the database    
    try:
        sql = "select oc.value as value from " + TABLE + " as oc where oc.key like 'land%password'"
        cursor.execute(sql)
        
        rows = cursor.fetchall()
     
        if len(rows) == 1:
            for row in rows:
                cp = row[0]
                
                if verbose:
                    print "Fetched credential '%s' from database" % cp
        else:
            err = "Odd expectation of more than one result back for current password --- BAILING!  Password changes for '%s' didn't happen on %s." % (username, platform.node())
            send_email(email_from, email_to, email_subject + " - Error", err)
            print err
            sys.exit(1)
    
    except Exception, e:
        err = "Couldn't fetch current password for user '%s' from database '%s' --- BAILING!  Password was not changed for user on %s." % (username, creds["d"], platform.node())
        send_email(email_from, email_to, email_subject + " - Error", err)
        print err
        sys.exit(1)
    
    if verbose:
        print "Changing from: %s" % cp

    # Generate new password
    np = genpass()

    if verbose:
        print "Changing to: %s" % np

    # Change password on *NIX side of the house
    if change_password("".join(args.username), cp, np, "passwd"):
        
        if verbose:
            print "UNIX side password change was successful, now attempting to update the DB"
        
        # Successful *NIX password change, lets update DB
        try:
            sql = "update " + TABLE + " as oc set oc.value = '" + np + "' where oc.key like 'land%.password'"
            
            cursor.execute(sql)
            
            db_conn.commit()
            
            #body = "Successfully updated password for user '%s' to '%s' in database '%s'" % (username, np, creds["d"])
            body = "Successfully updated password for user '%s' in database '%s' on %s." % (username, creds["d"], platform.node())
            
            send_email(email_from, email_to, email_subject + " - Success", body)
            
            if verbose:
                print "Successfully updated password for user '%s' to '%s' in database '%s' on %s" % (username, np, creds["d"], platform.node())
            
        except Exception, e:
            err = "Could not update password for '%s' in database '%s' -- BAILING!  Password was changed UNIX side but not DB side on %s." % (username, creds["d"], platform.node())
            send_email(email_from, email_to, email_subject + " - Error", err)
            print err
            sys.exit(1)
    else:
        # Bail and notify that we couldn't change the password on *NIX or DB
        err = "Could not update password for '%s' for *NIX environment --- BAILING!  Password was not changed on UNIX side or DB side on %s." % (username, platform.node())
        send_email(email_from, email_to, email_subject + " - Error", err)
        print err
        sys.exit(1)
    
    # Close out DB connection
    db_conn.close()
    
    
    # Lastly, regardless of success or not, let's set up the next cron job
    update_crontab(args.frequency, username)
    
if __name__ == '__main__':
    main()



