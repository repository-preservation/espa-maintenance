#!/bin/bash
#
#
#
#
# Name: deploy_install.sh
#
# Description: Software deployment installation script for ESPA/Hadoop architecture
#
# Author: Adam Dosch
#
# Creation Date: 06-21-2011
#
#
#############################################################################################################################
# Change	Date		Author			Description
#############################################################################################################################
#  001		06-21-2011	Adam Dosch		Initial Release
#  002		01-17-2013	Adam Dosch		Updating deployment script 
#  003		07-22-2013	Adam Dosch		Adding parameter functionality for tag release, dot-file config file
#							to override anything hard coded, and insertion of database creds 
#							from dotfile in homedir (yish but will work for now)
#  004		07-31-2013	Adam Dosch		Added local clean-up for DB_CRED_SCRIPT cleanup
#
#############################################################################################################################

declare -r PINGBIN="/bin/ping"

declare -r SSHBIN="/usr/bin/ssh -q"

declare -r SCPBIN="/usr/bin/scp -q"

declare -r SVNBIN="/usr/bin/svn"

STAMP=$( date +'%m%d%y-%H%M%S' )

ESPA_SVN_WORKING_DIR=${HOME}/tmp

DB_SETTINGS_FILES="/orderservice/settings.py"

SVN_HOST="http://espa.googlecode.com"

SVN_BASE="/svn"

declare RELEASE

declare REMOTEHOST

declare DB_CRED_FILE=${HOME}/.dbnfo

declare DB_CRED_SCRIPT=${HOME}/cred.regex

declare VERBOSE=1

declare DELETE_PRIOR_RELEASES=1

function print_usage
{
   echo
   echo " Usage: $0 --remotehost=<hostname> --release=<espa-n.n.n-release> [-v|--verbose] [-d|--delete-prior-releases]"
   echo
   echo "   NOTE: For sequential automation over a list of hosts, do:"
   echo
   echo "        for s in host1 host2; do $0 --remotehost=\$s --release=espa-9.9.9-release; done"
   echo

   exit 1
}

function release_validation
{
   # $1 - release from parameter
   
   # Let's make sure it exists in SVN or bail out too
   for tag in $( ${SVNBIN} list ${SVN_HOST}${SVN_BASE}/tags )
   do
      if [ "$tag" == "$1/" ]; then
         echo $1
      fi
   done

   # Always return false
   #return 1

# Optional code --- does regex validation first --- overkill
#   # $1 - release from parameter
#   
#   echo "$1" | egrep "espa-[0-9].[0-9].[0-9]-release" &> /dev/null
#
#   if [ $? -eq 0 ]; then
#      # Let's make sure it exists in SVN or bail out too
#      for tag in $( $SVNBIN list $SVN_HOST/$SVN_BASE/tags )
#      do
#         if [ "$tag" == "$1/" ]; then
#            return 0
#         fi
#      done    
#   else
#      return $?
#   fi
#
#   # Always return false
#   return 1
}

function gen_cred_regex_script
{
   # Generating hack-ass script to do remote regex --- better than escaping all that shit for remote SSH command
   cat > $DB_CRED_SCRIPT <<END
#!/bin/bash
. $DB_CRED_FILE
sed -i.backup.u -r -e "s~^(.*'USER': ).*(\#.*)~\1'\$u',\2~g" ${ESPA_SVN_WORKING_DIR}/${DB_SETTINGS_FILES}
sed -i.backup.d -r -e "s~^(.*'NAME': ).*(\#.*)~\1'\$d',\2~g" ${ESPA_SVN_WORKING_DIR}/${DB_SETTINGS_FILES}
sed -i.backup.p -r -e "s~^(.*'PASSWORD': ).*(\#.*)~\1'\$p',\2~g" ${ESPA_SVN_WORKING_DIR}/${DB_SETTINGS_FILES}
sed -i.backup.h -r -e "s~^(.*'HOST': ).*(\#.*)~\1'\$h',\2~g" ${ESPA_SVN_WORKING_DIR}/${DB_SETTINGS_FILES}
[[ -f \$0 ]] && rm -rf \$0
END

chmod +x $DB_CRED_SCRIPT

}


##############################################################################################################################
#                         START OF SCRIPT - DO NOT EDIT BELOW UNLESS YOU KNOW WHAT YOU ARE DOING
##############################################################################################################################

if [ $# -ge 2 -a $# -le 4 ]; then

   for param in $@
   do
      case $param in
         -v|--verbose)
            VERBOSE=0
            [[ $VERBOSE -eq 0 ]] && echo "Verbose mode enabled"
            ;;
         --remotehost=*)
            REMOTEHOST=$( echo $param | cut -d= -f2 | sed -r -e "s/[\"\']//g" | tr A-Z a-z )
            ;;
         --release=*)
            RELEASE=$( echo $param | cut -d= -f2 | sed -r -e "s/[\"\']//g" | tr A-Z a-z )

            response=$( release_validation "$RELEASE" )

            if [ -z "$response" ]; then
               echo -e "\nInvalid release: $RELEASE -- Either invalid format or doesn't exist in SVN repo"
               print_usage
            fi
            ;;
         -d|--delete-prior-releases)
            DELETE_PRIOR_RELEASES=0
            ;;
         *)
            echo
            echo "Invalid option: $param"
            echo
            print_usage
            ;;
      esac
   done

   # Hack to check for mandatory options
   if [ -z "$REMOTEHOST" -o -z "$RELEASE" ]; then
      print_usage
   else
      [[ $VERBOSE -eq 0 ]] && echo "Passed mandatory parameter check.  We have everything to continue deployment."
   fi

# Must be espa or espadev
#if [ "${USER}" == "espa" -o "${USER}" == "espadev" ]; then

   for server in $REMOTEHOST
   do
      [[ $VERBOSE -eq 0 ]] && echo "Starting deployment for: $server"

      # Is host up? (Crude input santization might be needed? This should catch malformed FQDN or invalid hosts)
      [[ $VERBOSE -eq 0 ]] && echo -n "Pinging $server ... "

      ${PINGBIN} -q -c2 ${server} &> /dev/null

      if [ $? -eq 0 ]; then
         [[ $VERBOSE -eq 0 ]] && echo "alive, continuing."

         # If we've chosen to remove releases, let's do that first, since we auto-backup prior to release below in the deployment
         [[ $DELETE_PRIOR_RELEASES -eq 0 ]] && echo "Removing all prior code deployments matching: ${ESPA_SVN_WORKING_DIR}.deploy-*" && ${SSHBIN} -t ${server} "rm -rf ${ESPA_SVN_WORKING_DIR}.deploy-*" &> /dev/null

         # Do code deployment on server with SSH
         [[ $VERBOSE -eq 0 ]] && echo "Deploying ESPA release $RELEASE to $ESPA_SVN_WORKING_DIR on $server"

         ${SSHBIN} -t ${server} "mv $ESPA_SVN_WORKING_DIR ${ESPA_SVN_WORKING_DIR}.deploy-${STAMP}; mkdir -p $ESPA_SVN_WORKING_DIR; cd $ESPA_SVN_WORKING_DIR; svn co ${SVN_HOST}${SVN_BASE}/tags/${RELEASE} .; find $ESPA_SVN_WORKING_DIR -type f -name \"*.pyc\" -exec rm -rf '{}' \;" &> /dev/null

         [[ $VERBOSE -eq 0 ]] && echo -n "Are we on a ESPA web-front-end server... "

         # If on the web front-end hosts, set up the DB credentials
         if [ "${server}" == "l8srlscp03" -o "${server}" == "l8srlscp12" ]; then
            [[ $VERBOSE -eq 0 ]] && echo "yes, continuing"

            [[ $VERBOSE -eq 0 ]] && echo -n "Checking to see if $DB_CRED_FILE exists on $server... "

            # If db cred file exists on remote web host
            response=$( ${SSHBIN} ${server} "[[ -f $DB_CRED_FILE ]] && echo yes || echo no" )

            if [ "$response" == "yes" ]; then
               [[ $VERBOSE -eq 0 ]] && echo "yes, continuing"

               # Create db cred script
               gen_cred_regex_script

               [[ $VERBOSE -eq 0 ]] && echo "Generating credential regex script ($DB_CRED_SCRIPT), pushing it out to $server and running that sucka..."

               ${SCPBIN} -q $DB_CRED_SCRIPT ${server}:${HOME} &> /dev/null
               ${SSHBIN} ${server} "$DB_CRED_SCRIPT" &> /dev/null

               # Clean up locally
               if [[ -f $DB_CRED_SCRIPT ]]; then
                  \rm -rf $DB_CRED_SCRIPT
               fi
            else
               # Couldn't find db creds, print out a warning to do it manually
               if [ $VERBOSE -eq 0 ]; then
                  [[ $VERBOSE -eq 0 ]] && echo "failed, db credentials file does not exist.  Do DB credentials manually."
               else
                  echo "Warning: db credentials file does not exist on $server --- database credentials were NOT input into settings.py.  Do it manually."
               fi
            fi
         fi

         [[ $VERBOSE -eq 0 ]] && echo "Deployment complete for $server"
      else
         # Ping failed on host, bail out!
         [[ $VERBOSE -eq 0 ]] && echo "failed, exiting."
         exit 1
      fi
   done

else
   print_usage
fi
