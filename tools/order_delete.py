#!/usr/bin/env python
#
#
#
# Name: order_delete.py
#
# Description: 
#
# Author: Adam Dosch
#
# Date: 12-24-2013
#
##########################################################################################
# Change        Date            Author              Description
##########################################################################################
#  001          12-24-2013      Adam Dosch          Initial release, quick-and-dirty POS.
#                                                   This totally needs to be class-ified
#
##########################################################################################
#
#

__author__ = "Adam Dosch"

import os
import sys

import MySQLdb

import platform

import re

def getDBCreds(credLocation):
    """
    Get DB credentials from db creds file
    """
    # Lets check and see if our dbcreds file exists?
    # This is where we will know what DB environment to update
    credFile = "%s/.dbnfo" % credLocation
    
    creds = {}
    
    if os.path.isfile(credFile):
        try:
            # open and read data from creds file
            f = open(credFile, "r")
        
            data = f.readlines()
            
            f.close()
            
        except Exception, e:
            return False
        
        # stuff creds in a dict
        for line in data:
            (k, v) = line.split("=")
            creds[k] = v.strip("\n")
        
        return creds
    else:
        return False

def connect_db(host, user, password, db, port=3306):
    """
    Connect to MySQL database
    """
    try:
        return MySQLdb.connect(host=host, port=port, user=user, passwd=password, db=db)
    except MySQLdb.Error, e:
        print "Could not connect to MySQL database"
        
    return False

def print_usage():
    print
    print " Usage: " + sys.argv[0] + "  <email_address>"
    print
    print "   Will remove ESPA orders from system.  Useful for test orders for benchmarking or troubleshooting."
    print
    sys.exit(1)

#=========================================================================================================
#               START OF SCRIPT --- DO NOT EDIT BELOW UNLESS YOU KNOW WHAT YOU ARE DOING
#=========================================================================================================

def main():
    
    if len(sys.argv) -1 == 1:
        
        # Get homedir location of user running this to look for db creds
        if os.environ.has_key('HOME'):
            credLocation = os.environ['HOME']
        else:
            # Defaulting to 'something' vs exception out --- stupid, but whatever
            credLocation = "/tmp"
    
        # Try and fetch DB credentials
        creds = getDBCreds(credLocation)
        
        if not creds:
            raise Exception("Problem fetching or getting database credentials!")
    
        # Create DB connetion
        db_conn = connect_db(host=creds["h"], user=creds["u"], password=creds["p"], db=creds["d"])
    
        if not db_conn:
            raise Exception("Cound not establish connection to the DB on %s!  Either the username, password or database options are muffed or you're locked out." % (platform.node()))

        # Create DB cursor
        cursor = db_conn.cursor()
        
        # Lets fetch all the orders matching our e-mail (we aren't even validating e-mail.... so beware of injection)
        rows = tuple()
        
        try:
            cursor.execute("select id, orderid, email from ordering_order where email REGEXP '(" + sys.argv[1] + ")' order by order_date desc")
                
            rows = rows + cursor.fetchall()
        
        except Exception, e:
            print "Exception: ", e
            sys.exit(1)
        
        # Do we have enough rows?
        if len(rows) > 0:
            # Do some bullshit element injection/re-ordering to start at one, not zero
            lrows = list(rows)
            lrows.insert(0,tuple(("","","")))
            
            rows = tuple(lrows)
            
            # Print out the rows
            print
            print "Here's the following orders:"
            print
            
            for i, row in enumerate(rows):
                if row[0] <> "":
                    print " %s) %s" % (i, row[1])
            
            print
            
            # Choosing which order result(s) to delete
            try:
                
                answer = None
    
                while not answer:
                    answer = raw_input(" What order(s) do you want to delete (e.g. 1,2,3,n): ")
                    
                    if re.search("([0-9]+\,?)", answer):
                        break
                    else:
                        answer = None
                
                deleterows = answer
            except KeyboardInterrupt:
                print
                sys.exit(2)
            
            # Better ask if we're sure before we delete them
            try:
                answer = None
                
                while not answer:
                    answer = raw_input(" Are you sure? (y/n) ")
                    
                    if answer.lower() in ['y','n']:
                        if answer.lower() == 'n':
                            print
                            print "Bailing!"
                            sys.exit(1)
                    else:
                        answer = None
            except KeyboardInterrupt:
                print
                sys.exit(2)
            
            print "Going to remove: ", deleterows
            
            # Nuking the orders sequentialy from how they were chosen
            for order in deleterows.split(","):
                
                # Delete scenes related to order in ordering_scene
                try:
                    cursor.execute("delete from ordering_scene where order_id = '%s'" % rows[int(order)][0])
                    #print("delete from ordering_scene where order_id = '%s'" % rows[int(order)][0])
                    
                    db_conn.commit()
                except Exception, e:
                    db_conn.rollback()
                    print "Exception trying to remove scenes from ordering_scene for order_id %s: %s" % (rows[int(order)][0], e)

                # Delete order from ordering_order
                try:
                    cursor.execute("delete from ordering_order where id = '%s'" % rows[int(order)][0])
                    #print("delete from ordering_order where id = '%s'" % rows[int(order)][0])
                    
                    db_conn.commit()
                except Exception, e:
                    db_conn.rollback()
                    print "Exception trying to remove order entry from ordering_order for id %s: %s" % (rows[int(order)][0], e)          
            
            # Notify user to purge online cache area manually (for now)
               
            # Close DB connection
            db_conn.close()
        else:
            print
            print "Sorry, no order matches for: %s" % sys.argv[1]
            print
            sys.exit(1)
    else:
        print_usage()

if __name__ == '__main__':
    main()