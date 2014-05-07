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
#  005		04-29-2014	Adam Dosch		Rewrite of deployment script to add '--mode' to specify 'devel|prod'
#							Adding '--tier' to specify 'processing|web|app|maintenance' to work
#							with new app deployment with uwsgi
#							Adding new function for stdout writing when VERBOSE is enabled
#							Removing --remotehost flag
#
#############################################################################################################################

declare -r PINGBIN="/bin/ping"

declare -r SSHBIN="/usr/bin/ssh -q"

declare -r SCPBIN="/usr/bin/scp -q"

declare -r SVNBIN="/usr/bin/svn"

STAMP=$( date +'%m%d%y-%H%M%S' )

SVN_WORKING_DIR=${HOME}/tmp

SETTINGS_FILES="/web/espa_web/espa-uwsgi.ini"

SVN_HOST="http://espa.googlecode.com"

SVN_BASE="/svn"

declare RELEASE

declare TIER="all"

declare TIERS="app maintenance processing"

declare MODE

declare MODES="test devel prod"

declare DB_CRED_FILE=${HOME}/.dbnfo

declare DB_CRED_SCRIPT=${HOME}/cred.regex

declare VERBOSE=1

declare DELETE_PRIOR_RELEASES=1

function print_usage
{
   echo
   echo " Usage: $0 --mode=[prod|devel] --tier=[app|maintenance|processing|all]  --release=<espa-n.n.n-release> [-v|--verbose] [-d|--delete-prior-releases]"
   echo

   exit 1
}

function mode_validation
{
   # $1 - mode from parameter

   for valid_mode in $MODES
   do
      if [ "$valid_mode" == "$1" ]; then
         echo $1
         break
      fi
   done
}

function release_validation
{
   # $1 - release from parameter
   
   # Let's make sure it exists in SVN or bail out too
   for valid_tag in $( ${SVNBIN} list ${SVN_HOST}${SVN_BASE}/tags )
   do
      if [ "$valid_tag" == "$1/" ]; then
         echo $1
         break
      fi
   done
}

function tier_validation
{
   #$1 - tier from parameter

   for valid_tier in $TIERS all
   do
      if [ "$valid_tier" == "$1" ]; then
         echo $1
         break
      fi
   done
}

function gen_cred_regex_script
{
   # Generating hack-ass script to do remote regex --- better than escaping all that shit for remote SSH command
   cat > $DB_CRED_SCRIPT <<END
#!/bin/bash
. $DB_CRED_FILE

sed -i.backup.h -r -e "s~^(.*'HOST': ).*(\#.*)~\1'\$h',\2~g" ${SVN_WORKING_DIR}/${SETTINGS_FILES}
[[ -f \$0 ]] && rm -rf \$0
END

chmod +x $DB_CRED_SCRIPT

}

function write_stdout
{
   # $1 -> mode
   # $2 -> message body
   TIMESTAMP=$(  date +'%b %d %H:%M:%S' )

   echo "${TIMESTAMP} deployment $1: $2"   
}

function deploy_tier
{
   #$1 - tier
   #$2 - mode

   mode=$2

   declare -A tierhosts

   tierhosts[test-app]="l8srlscp16"
   tierhosts[test-maintenance]="l8srlscp16"
   tierhosts[test-processing]="l8srlscp16"

   tierhosts[devel-app]="l8srlscp13"
   tierhosts[devel-maintenance]="l8srlscp01"
   tierhosts[devel-processing]="l8srlscp08"
   
   tierhosts[prod-app]="l8srlscp14"
   tierhosts[prod-maintenance]="l8srlscp01"
   tierhosts[prod-processing]="l8srlscp05"

   if [ "$1" == "all" ]; then
      tiers="$TIERS"
   else
      tiers=$1
   fi
 
   for tier in $tiers
   do
      lookup="${mode}-${tier}"
      for server in ${tierhosts[${lookup}]}
      do
         continue
####         [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "Starting deployment for: $server"
####   
####         # Is host up? (Crude input santization might be needed? This should catch malformed FQDN or invalid hosts)
####         [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "Pinging $server ... "
####   
####         ${PINGBIN} -q -c2 ${server} &> /dev/null
####   
####         if [ $? -eq 0 ]; then
####            [[ $VERBOSE -eq 0 ]] && echo "alive, continuing."
####   
####            # If we've chosen to remove releases, let's do that first, since we auto-backup prior to release below in the deployment
####            [[ $DELETE_PRIOR_RELEASES -eq 0 ]] && echo "Removing all prior code deployments matching: ${SVN_WORKING_DIR}.deploy-*" && ${SSHBIN} -t ${server} "rm -rf ${SVN_WORKING_DIR}.deploy-*" &> /dev/null
####   
####            # Do code deployment on server with SSH
####            [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "Deploying ESPA release $RELEASE to $SVN_WORKING_DIR on $server"
####   
####            ${SSHBIN} -t ${server} "mv $SVN_WORKING_DIR ${SVN_WORKING_DIR}.deploy-${STAMP}; mkdir -p $SVN_WORKING_DIR; cd $SVN_WORKING_DIR; svn co ${SVN_HOST}${SVN_BASE}/tags/${RELEASE} .; find $SVN_WORKING_DIR -type f -name \"*.pyc\" -exec rm -rf '{}' \;" &> /dev/null
####   
####            [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "Are we on a ESPA web-front-end server... "
####   
####            # If on the web front-end hosts, set up the DB credentials
####            if [ "${server}" == "l8srlscp03" -o "${server}" == "l8srlscp12" ]; then
####               [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "yes, continuing"
####   
####               [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "Checking to see if $DB_CRED_FILE exists on $server... "
####   
####               # If db cred file exists on remote web host
####               response=$( ${SSHBIN} ${server} "[[ -f $DB_CRED_FILE ]] && echo yes || echo no" )
####   
####               if [ "$response" == "yes" ]; then
####                  [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "yes, continuing"
####   
####                  # Create db cred script
####                  gen_cred_regex_script
####   
####                  [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "Generating credential regex script ($DB_CRED_SCRIPT), pushing it out to $server and running that sucka..."
####   
####                  ${SCPBIN} -q $DB_CRED_SCRIPT ${server}:${HOME} &> /dev/null
####                  ${SSHBIN} ${server} "$DB_CRED_SCRIPT" &> /dev/null
####   
####                  # Clean up locally
####                  if [[ -f $DB_CRED_SCRIPT ]]; then
####                     \rm -rf $DB_CRED_SCRIPT
####                  fi
####               else
####                  # Couldn't find db creds, print out a warning to do it manually
####                  if [ $VERBOSE -eq 0 ]; then
####                     [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "failed, db credentials file does not exist.  Do DB credentials manually."
####                  else
####                     echo "Warning: db credentials file does not exist on $server --- database credentials were NOT input into settings.py.  Do it manually."
####                  fi
####               fi
####            fi
####   
####            [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "Deployment complete for $server"
####         else
####            # Ping failed on host, bail out!
####            [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "failed, exiting."
####            exit 1
####         fi
      done
   done

}



##############################################################################################################################
#                         START OF SCRIPT - DO NOT EDIT BELOW UNLESS YOU KNOW WHAT YOU ARE DOING
##############################################################################################################################

if [ $# -ge 2 -a $# -le 4 ]; then

   for param in $@
   do
      case $param in
         --mode=*)
            MODE=$( echo $param | cut -d= -f2 | sed -r -e "s/[\"\']//g" | tr A-Z a-z )

            response=$( mode_validation "$MODE" )

            if [ -z "$response" ]; then
               echo -e "\nInvalid mode: $MODE -- provide correct mode to continue"
               print_usage
            fi
            ;;
         --tier=*)
            TIER=$( echo $param | cut -d= -f2 | sed -r -e "s/[\"\']//g" | tr A-Z a-z )

            response=$( tier_validation "$TIER" )
            if [ -z "$response" ]; then
               echo -e "\nInvalid tier: $TIER -- provide corect tier to continue or use 'all'"
               print_usage
            fi
            ;;
         -v|--verbose)
            VERBOSE=0
            [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "Verbose mode enabled"
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
   if [ -z "$MODE" -o -z "$RELEASE" ]; then
      print_usage
   else
      [[ $VERBOSE -eq 0 ]] && write_stdout "$MODE" "Passed mandatory parameter check.  We have everything to continue deployment."
   fi

   # Deploy tier
   deploy_tier "$TIER" "$MODE" 

else
   print_usage
fi
