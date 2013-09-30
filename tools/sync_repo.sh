#!/bin/bash
#
#
#
#
#
# Name: sync_repo.sh
#
# Description: Sync/mirror google code repository locally for backup in case we ever have a shit from cloud services
#
# Author: Adam Dosch
#
# Date: 07-12-2013
#
#######################################################################################################################
# Change	Date			Author				Description
#######################################################################################################################
#  001		07-12-2013		Adam Dosch			Initial Release
#  002		09-17-2013		Adam Dosch			Adding --loglocation parameter for external 
# 									logging
#  003		09-25-2013		Adam Dosch			Removing timestamp from logfile generation
#									and letting logrotate handle this
#									Adding zero parameter check to print_usage
#
#######################################################################################################################


declare INIT

declare SYNC

declare REPO2SYNC

declare DESTREPO

declare -r SVNADMIN=$( which svnadmin )

declare -r SVNSYNC=$( which svnsync )

declare LOGFILE=/dev/null

declare TIMESTAMP=$(date +'%Y-%m-%d')

function print_usage
{
   echo
   echo " Usage: $0 [--init|--sync] --repo2sync=<repo> --syncrepodestination=<repo> [--loglocation=/path/to/create/log]"
   echo
   echo " This script will sync a remote repository to a local filesystem location, for back-up purposes."
   echo " Don't try to use it for anything other than that.  If you do, hack accordingly."
   echo

   exit 1
}

# Checking parameters
if [ $# -gt 4 -o $# -eq 0 ]; then
   print_usage
else
   # Validating parameters
   for param in $@
   do
      case $param in
         --init)
            if [ -z "$SYNC" ]; then
               INIT=yes
            else
               echo "You cannot specify '--init' and '--sync' at the same time"
               print_usage
            fi
            ;;

         --sync)
            if [ -z "$INIT" ]; then
               SYNC=yes
            else
               echo "You cannot specify '--init' and '--sync' at the same time"
               print_usage
            fi
            ;;

         --loglocation=*)
            LOGFILE=$(echo $param | cut -d= -f2)
            LOGFILE="$LOGFILE/syncrepo.log"
            ;;

         --repo2sync=*)
            REPO2SYNC=$( echo $param | cut -d= -f2 )
            ;;

         --syncrepodestination=*)
            DESTREPO=$( echo $param | cut -d= -f2 )
            ;;

         *)
            echo "Invalid parameter --- $param"
            print_usage
            ;;
      esac
   done

   echo "------ $DESTREPO INIT=${INIT} SYNC=${SYNC} ------" >> $LOGFILE 2>&1

   # Initialize repository --- this should only be run once to set all of this up
   if [ ! -z "${INIT}" ]; then

      # Does our destination repo exist locally?
      if [ ! -d $DESTREPO ]; then
         # It doesn't, lets create it and initialize it
         mkdir -p $DESTREPO

         # Create repo since it clearly doesn't exist
         ${SVNADMIN} create $DESTREPO >> $LOGFILE 2>&1
         if [ $? -eq 0 ]; then
            # Since we first initialized, lets set up  pre-revprop-change hooks
            echo -e "#\041/bin/sh\nexit 0" >> ${DESTREPO}/hooks/pre-revprop-change && chmod 755 ${DESTREPO}/hooks/pre-revprop-change
         else
            #echo "Error initializing repo ${DESTREPO} to sync against ${REPO2SYNC}!"
            exit 1
         fi
      fi

      # lets initialize it
      ${SVNSYNC} init file://${DESTREPO} ${REPO2SYNC} >> $LOGFILE 2>&1
   fi

   # Sync that repo up --- assuming we've already initialized it
   if [ ! -z "${SYNC}" ]; then
      ${SVNSYNC} sync file://${DESTREPO} >> $LOGFILE 2>&1
   fi
fi
