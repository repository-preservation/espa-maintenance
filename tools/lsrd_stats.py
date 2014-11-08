#!/usr/bin/env python
#
#
# Name: lsrd_stats.py
#
# Description: Script to gather monthly statitics for LSRD
#
# Author: Adam Dosch
#
# Date: 10-28-2012
#
#########################################################################################
# Change    	Date            Author          Description
#########################################################################################
#  001              10-28-2012      Adam Dosch      Initial release
#  002              02-04-2013      Adam Dosch      Complete overhaul and added mysql stats
#                                                   gathering into the mix, e-mail.  There's ALOT
#                                                   of bitch hacks in this shit.  Needs to be
#                                                   revisited but probably won't until I get the
#                                                   data into mongoDB.
# 003               03-01-2013      Adam Dosch      Got pig command to successfully run and dump
#                                                   any errors to STDOUT
# 004               05-06-2013      Adam Dosch      Adding 'resultsondemandledaps' pig store
#                                                   results to output report
# 005               05-31-2013      Adam Dosch      Making order separation by source so we can
#                                                   tell the difference between which interface
#                                                   someone used to submit order (ee or espa)
# 006               06-03-2013      Adam Dosch      Adding os.environ['JAVA_HOME'] to top-level
#                                                   to fix environment sourcing issue from cron
# 007               08-21-2013      Adam Dosch      Adding '__author__' references
#                                                   Updated email_from to FQDN host
# 008               11-25-2013      Adam Dosch      Removing GLS collection and UI reporting from
#                                                   the scripts --- we be tearing that shit OUT.
#                                                   I will keep logic in pig scripts, but just not
#                                                   report those in the e-mail output
# 009               01-30-2014      Adam Dosch      Updating 'resultsondemandledaps' to 'resultsondemand'
#                                                   as the outdir name has changed in lsrd_ondemand_metrics
#                                                   pig script
# 010               05-01-2014      Adam Dosch      Cleaning up dotfile temp cred mess and db_connect fuction
# 011               09-08-2014      Adam Dosch      Updating unique users query to use a substring index
#                                                   since our email field does not get populated in the
#                                                   DB anymore since 2.5.0 release
# 012               11-07-2014      Adam Dosch      Updated queries to distinguish USGS vs. non-USGS better 
#                                                   and added to distribution list
#
#########################################################################################

__author__ = "Adam Dosch"

import os
import commands
import sys
import re

import datetime
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

# For mysql
import MySQLdb

# For giveLastMonthDate()
from dateutil.relativedelta import relativedelta

# Disabling MySQL warnings
from warnings import filterwarnings
import MySQLdb as Database

filterwarnings('ignore', category = Database.Warning)

global verbose
verbose = False

# E-mail Recipients/Subject
email_from = 'espa@espa.cr.usgs.gov'
#email_from = 'espa'
email_to = ['adosch@usgs.gov','jenkerson@usgs.gov','lowen@usgs.gov']
#email_to = ['adosch@usgs.gov']

email_subject = "LSRD Monthly Statitics"

# Result files to process from pig runs
# - NOTE: there are other generated statistics so if we
#         define 
pigresults = ['resultsui',
              'results2000',
              'results2005',
              'results2010',
              'resultsall'
             ]

# Order sources
order_sources = ['espa', 'ee']

# Setting JAVA_HOME
if os.environ.get("JAVA_HOME") == None:
	os.environ["JAVA_HOME"] = "/usr/java/latest"

class PigProcessor:
    #count = 0
    
    def __init__(self, pigscript, outputfile):
        global verbose
        
        if verbose == True: print "Setting up new PigProcessor object for pig script:", pigscript
        
        # Does this pig file exist?
        if verbose == True: print "Is " + pigscript + " a file that exists?"
        
        if os.path.isfile(pigscript):
                if verbose == True: print pigscript + " exists, setting property and continuing..."
                self.pigscript = pigscript
        else:
            sys.stderr.write("\nError: " + pigscript + " is not a file or does not exist!\n")
            sys.exit(1)
            
        # Keeping track of how many Pig Processors there are to do
        #PigProcessor.count += 1        
        
        # Holder for any pig errors on runtime
        self.pigerror = ""
        
        # Holder for pig retval
        self.pigretval = ""
        
        # Pig tmplog
        self.pigtmplog = "/tmp/pig.log"
        
        # Open handle to Results output file
        if verbose == True: print "Setting up output file", outputfile, " for writing results to..."
        
        try:
            self.outputFile = open(outputfile, "w")
        except Exception, e:
            print "Error: ", e
            
        # Creating a dictionary of all Pig storage results points
        # by parsing the pigfile and generating a dictionary of them
        # so we know where to go look for these statistics
        #
        # Must look for this line:
        #
        # STORE resultsall INTO '/tmp/resultsall' USING PigStorage(',');
        #
        self.results = {}
        
        if verbose == True: print "Opening", self.pigscript, " to save pig STORE result locations into dict."
        
        pf = open(self.pigscript, "r")
        
        for line in pf.readlines():
            # Match our Pig STORE line
            if re.match("^STORE\sresult\S+\sINTO\s\S+\sUSING\sPigStorage\S+", line):
                if verbose == True: print "Matched STORE line: ", line
                
                # Break out positions (space delimited)
                pig_mthd, result_name, pig_into, result_location, pig_verb, pig_stortype = line.split(" ") #STORE resultsall INTO '/tmp/resultsall' USING PigStorage(',');
                #line_split = line.split(" ")
                
                # Add to results dictionary
                self.results[result_name] = "%s/%s" % (result_location.strip("'"), "part-r-00000")
                
                if verbose == True: print "Adding result location of", result_location.strip("'"), "/part-r-00000 to dict"
                
        # Close pigfile
        pf.close()
        
        # If we didn't match any Pig STORE lines, bail out
        if len(self.results.items()) == 0:
            sys.stderr.write("\nError: Did not find any Pig STORE procedures to parse.  Check pigscript or regex match!\n")
            sys.exit(1)
    
    def __openOutputFile(outputfile):
        
        if os.path.isfile(outputfile):
            os.remove(outputfile)
            
        return open(outputfile, "w")
    
    def RunPig(self):
        global verbose
 
        if verbose == True: print "Running pig: pig -b -F -x local -l /tmp/pig.log -f " + self.pigscript
        
        (self.pigretval, self.pigerror) = commands.getstatusoutput("~/pig/bin/pig -b -F -x local -l " + self.pigtmplog + " -f " + self.pigscript)
        
        #self.pigretval = 0
        
        return self.pigretval

    def writeHeader(self, results_name):
        #
        # To add a pig result header, add its STORE name and string
        # header to the dict below
        # - NOTE: The keyname MUST match what is in your Pig STORE Line in
        #         the pig file
        
        header = {'resultsui': ' GLS User Interface',
                  'results2000':' GLS 2000 Collection',
                  'results2005':' GLS 2005 Collection',
                  'results2010':' GLS 2010 Collection',
                  'resultsall':' All GLS Collections',
		  'resultsondemand':' On-demand - Download Info'}
        
        global verbose

        if verbose == True: print "Checking pig result header key of ", results_name, " to make sure it exists"

        if header.has_key(results_name):

            self.outputFile.write("\n==========================================\n")
            
            self.outputFile.write(header[results_name] + "\n")
            
            self.outputFile.write("==========================================\n")
            
            if verbose == True: print results_name, "exists, writing out header to outputfile"
            
        else:
            print "writeHeader(): key ", results_name, " not found, not processing"
            sys.exit(1)

    def CleanupPigReults(self):
        # Squatter function to clean up pig results after usage
	(retval, output) = commands.getstatusoutput("rm -rf /tmp/results*")

    def ProcessResults(self):
        #
        # To process a new pig result, add it's corresponding if conditional below
        # - NOTE: Make sure to get the number of csv fields in the pig output
        #         to match what is read in + get verbage right
        global verbose
    
        for results_name, results_file in self.results.iteritems():
            if verbose == True: print "Reading pig results file for", results_name, "from", results_file
            
            rf = open(results_file, "r")
            
            #-----------------
            # GLS UI results
            #-----------------
            #if results_name == "resultsui":
            #    
            #    if verbose == True: print "Matched", results_name, ", reading metrics and writing header + metrics to outputfile"
            #
            #    for line in rf.readlines():
            #        unique_total, unique_usgs, unique_eros, num_useragents = line.split(",")
            #    
            #    self.writeHeader(results_name)
            #    
            #    self.outputFile.write("Number of unique visitors: " + unique_total + "\n")
            #    self.outputFile.write("Number of unique total USGS visitors: " + unique_usgs + "\n")
            #    self.outputFile.write("Number of unique EROS visitors: " + unique_eros + "\n")

            #-----------------
            # GLS results
            #-----------------
            #if re.match("^results[all|20[0-9]{2}", results_name):
            #
            #    if verbose == True: print "Matched", results_name, ", reading metrics and writing header + metrics to outputfile"
            #
            #    for line in rf.readlines():
            #        unique_downloaders, num_downloaded_scenes, volume_distributed = line.split(",")
            #    
            #    self.writeHeader(results_name)
            #    
            #    self.outputFile.write("Number of unique downloaders: " + unique_downloaders + "\n")
            #    self.outputFile.write("Total number of scenes downloaded: " + num_downloaded_scenes + "\n")
            #    self.outputFile.write("Total volume distributed (GB): " + volume_distributed + "\n")

            #-----------------
            # Ondemand results
            #-----------------
            if re.match("^resultsondemand", results_name):
            
                if verbose == True: print "Matched", results_name, ", reading metrics and writing header + metrics to outputfile"
            
                for line in rf.readlines():
                    num_downloaded_scenes, volume_downloaded = line.split(",")
                
                self.writeHeader(results_name)
                
                self.outputFile.write("Total number of ordered scenes downloaded through ESPA order interface order links: " + num_downloaded_scenes + "\n")
                self.outputFile.write("Total volume of ordered scenes downloaded (GB): " + volume_downloaded + "\n")

        # We need to NUKE all results after we are done with them so the next pig run doesn't
        # freak out since the STORE directory space lives on
        #os.rmdir()

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
    
# Returns prior month when run on the 1st of the month
def giveLastMonthDate():
    
    # Today's date
    today = datetime.datetime.today()

    # Using relativedelta by adding a month
    delta = relativedelta(months=+1)

    # Subtracting the added month (works backwards) to get
    # the month previous
    last_month_date = today - delta

    return last_month_date.strftime("%Y-%m")

# Connect to MySQL DB
def connect_db(host, user, password, db, port=3306):
    try:
        return MySQLdb.connect(host=host, port=port, user=user, passwd=password, db=db)
    except MySQLdb.Error, e:
        sys.stderr.write("[ERROR] %d: %s\n" % (e.args[0], e.args[1]))
        
    return False

    
#=====================================================================================================
#             START OF SCRIPT BELOW - DO NOT EDIT UNLESS YOU KNOW WHAT YOU ARE DOING
#=====================================================================================================
    
def main():
    global verbose
    
    #------------------------------
    # Setting up argparse arguments
    #------------------------------
    
    parser = argparse.ArgumentParser(prog="lsrd_stats.py", description="Generate statitics for LSRD")
    parser.add_argument('--pigscripts', action='store', help='Pig script to run')
    parser.add_argument('--outputfile', action='store', help='File to store output in')
    parser.add_argument('--verbose', action='store_true', help='Enable verbosity for debugging or troubleshooting', default=False)
    
    args = parser.parse_args()
    
    #------------------------------
    # Hackish way to process args
    #------------------------------
    
    if not args.pigscripts or not args.outputfile:
        parser.error("Must set --pigscripts and --outputfile")
    
    if args.verbose == True:
        verbose = True
        print "Verbose mode enabled"
    
    #------------------------------
    # Split pigscripts out if a
    # comma separated list of any
    # scripts were passed in, and run
    # Pig against them sequentially
    #------------------------------
    
    if verbose == True: print "Parsing pigscript options..."
    
    # List of pigscripts from --pigscripts
    pigscripts = args.pigscripts
    
    # Outputfile from --outputfile
    outfile = args.outputfile
    
    # Processing each pigscript
    for script in pigscripts.split(","):
        if verbose == True: print "------", script
        
        # Set up new PigProcessor object for each pigscript
        pig = PigProcessor(script, outfile)
        
        # Run pig
        retval = pig.RunPig()
        
        if retval == 0:
            pig.ProcessResults()
        else:
            # Print error and bail ungracefully - should collect all errors
            # and have an error incrementor and print all on error - TODO
            print "-----"
            print "Error running '%s' pig script: %s" % (script, pig.pigerror)
            print "-----"
            sys.exit(1)
    
    # Clean-up Pig Results (since we didn't have any failure)
    pig.CleanupPigReults()
       
    # Close that report file any PigProcessor objects have open
    pig.outputFile.close()
    
    # Username
    username = os.getlogin()
    
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

    # Lastly lets connect to the database and get our metrics
    db_conn = connect_db(host=creds["h"], user=creds["u"], password=creds["p"], db=creds["d"])

    if not db_conn:
        sys.stderr.write("\nCound not establish connection to the DB!\n")
        sys.exit(1)
    
    cursor = db_conn.cursor()

    f = open(outfile, "a")
    
    
    # Looping through order sources and gathering stats - EE and ESPA
    for source in order_sources:
	
	# Write out header
	f.write("\n==========================================\n")
	f.write(" On-demand - " + source.upper() + "\n")
	f.write("==========================================\n")
    
	#==================================================================================
	#=================================SCENE COUNT======================================
	#==================================================================================
	
	# Looping through order_sources to break out statics per order interface (either Earth Explorer or ESPA)

    
	#----------------------------------------------
	# Total scenes ordered in a month
	#----------------------------------------------
    
	SQL = "select COUNT(*) from ordering_scene inner join ordering_order on ordering_scene.order_id = ordering_order.id where ordering_order.order_date like \'" + giveLastMonthDate() + "-%\' and ordering_order.order_source = '" + source + "'"
    
	cursor.execute(SQL)
	
	rows = cursor.fetchall()
     
	if len(rows) >= 1:
	    for row in rows:
		total = str(row[0])
		
		#print " Total scenes ordered in the month:", total
		f.write(" Total scenes ordered in the month for '" + source + "' interface: " + total + "\n")
	else:
	    f.write(" Total scenes ordered in the month for '" + source + "' interface: 0\n")
	    #print " Total scenes ordered in the month: 0"
	    #print
    
	#----------------------------------------------
	# Number of total scenes ordered in a month are USGS
	#----------------------------------------------
    
	SQL = "select COUNT(*) as usgs_scene_orders from ordering_scene inner join  ordering_order on ordering_scene.order_id = ordering_order.id where ordering_order.order_date like \'" + giveLastMonthDate() + "-%\' and ordering_order.orderid like \'%@usgs.gov-%\' and ordering_order.order_source = '" + source + "';"
	
	cursor.execute(SQL)
	
	rows = cursor.fetchall()
     
	if len(rows) >= 1:
	    for row in rows:
		total = str(row[0])
		
		f.write(" Number of scenes ordered in the month (USGS) for '" + source + "' interface: " + total + "\n")
		#print " Number of scenes ordered in the month (USGS):", total
	else:
	    f.write(" Number of scenes ordered in the month (USGS) for '" + source + "' interface: 0\n")
	    #print " Number of scenes ordered in the month (USGS): 0"
	    #print
	    
	#----------------------------------------------
	# Number of total scenes ordered in a month are non-USGS
	#----------------------------------------------
    
	SQL = "select COUNT(*) as usgs_scene_orders from ordering_scene inner join  ordering_order on ordering_scene.order_id = ordering_order.id where ordering_order.order_date like \'" + giveLastMonthDate() + "-%\' and ordering_order.orderid not like \'%@usgs.gov-%\' and ordering_order.order_source = '" + source + "';"
    
	cursor.execute(SQL)
	
	rows = cursor.fetchall()
     
	if len(rows) >= 1:
	    for row in rows:
		total = str(row[0])
		
		f.write(" Number of scenes ordered in the month (non-USGS) for '" + source + "' interface: " + total + "\n")
		#print " Number of scenes ordered in the month (non-USGS):", total
	else:
	    f.write(" Number of scenes ordered in the month (non-USGS) for '" + source + "' interface: 0\n")
	    #print " Number of scenes ordered in the month (non-USGS): 0"
	    #print
        
    
	#==================================================================================
	#======================================ORDERS======================================
	#==================================================================================
	
	# Looping through order_sources to break out statics per order interface (either Earth Explorer or ESPA)
	# which is at the top
    
	#----------------------------------------------
	# Total orders placed in a given month
	#----------------------------------------------
    
	SQL = "select COUNT(*) from ordering_order where order_date like \'" + giveLastMonthDate() + "-%\' and ordering_order.order_source = '" + source + "'"
       
	cursor.execute(SQL)
	
	rows = cursor.fetchall()
     
	if len(rows) >= 1:
	    for row in rows:
		total = str(row[0])
		
		f.write(" Total orders placed in the month for '" + source + "' interface: " + total + "\n")
		#print " Total orders placed in the month:", total
	else:
	    f.write(" Total orders placed in the month for '" + source + "' interface: 0\n")
	    #print " Total orders placed in the month: 0"
	    #print  
    
	#----------------------------------------------
	# Number of total orders placed in a month are USGS
	#----------------------------------------------
    
	SQL = "select COUNT(*) from ordering_order where order_date like \'" + giveLastMonthDate() + "-%\' and orderid like \'%@usgs.gov-%\' and ordering_order.order_source = '" + source + "'"
	
	cursor.execute(SQL)
	
	rows = cursor.fetchall()
     
	if len(rows) >= 1:
	    for row in rows:
		total = str(row[0])
		
		f.write(" Number of total orders placed in the month (USGS) for '" + source + "' interface: " +  total + "\n")
		#print " Number of total orders placed in the month (USGS):", total
	else:
	    f.write(" Number of total orders placed in the month (USGS) for '" + source + "' interface: 0\n")
	    #print " Number of total orders placed in the month (USGS): 0"
	    #print   
    
	#----------------------------------------------
	# Number of total orders placed in a month are non-USGS
	#----------------------------------------------
	
	SQL = "select COUNT(*) from ordering_order where order_date like \'" + giveLastMonthDate() + "-%\' and orderid not like \'%@usgs.gov-%\' and ordering_order.order_source = '" + source + "'"
	
	cursor.execute(SQL)
	
	rows = cursor.fetchall()
     
	if len(rows) >= 1:
	    for row in rows:
		total = str(row[0])
		
		f.write(" Number of total orders placed in the month (non-USGS) for '" + source + "' interface: " + total + "\n")
		#print " Number of total orders placed in the month (non-USGS):", total
	else:
	    f.write(" Number of total orders placed in the month (non-USGS) for '" + source + "' interface: 0\n")
	    #print " Number of total orders placed in the month (non-USGS): 0"
	    #print    
	
	#----------------------------------------------
	# Total number of unique on-demand users
	#----------------------------------------------
	
	SQL = "select COUNT(DISTINCT(substring_index(orderid,'-',1))) from ordering_order where ordering_order.order_source = '" + source + "'"
	
	cursor.execute(SQL)
    
	rows = cursor.fetchall()
     
	if len(rows) >= 1:
	    for row in rows:
		total = str(row[0])
		
		f.write(" Total number of unique On-Demand users for '" + source + "' interface: " + total + "\n")
		#print " Total number of unique On-Demand users:", total
	else:
	    f.write(" Total number of unique On-Demand users for '" + source + "' interface: 0\n")
	    #print " Total number of unique On-Demand users: 0"
	    #print    
    
	f.write("\n")
    
    #------------end of massive loop-----------
    
    # Close db and report-file handles
    db_conn.close()
    f.close()
    
    # Send off an e-mail
    
    #datestamp_tag = giveLastMonthDate()
    #email_subject = email_subject + " - " + datestamp_tag

    f = open(outfile, "r")
    reportbody = f.readlines()
    
    body = ""
    
    for line in reportbody:
        body = body + line
    
    f.close()
    
    send_email(email_from,email_to,email_subject + " - " + giveLastMonthDate(),body)
    
if __name__ == '__main__':
    main()
