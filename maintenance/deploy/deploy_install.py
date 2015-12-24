#!/usr/bin/env python

import paramiko
import argparse
import datetime

try:
    import deployment_settings as settings
except Exception, e:
    s = '''tiers = ['espa-web', 'espa-maintenance', 'espa-production', 'all']

environments = {
    'dev': {
        'user': 'dev username',
        'tiers': {
            'espa-web': {'host': 'dev webapp hostname',
                         'repo': 'repository url'},
            'espa-maintenance': {'host': 'dev maint hostname',
                                 'repo': 'repository url'},
            'espa-production': {'host': 'dev production hostname',
                                'repo': 'repository url'}
        }
    },
    'tst': {
        'user': 'tst username',
        'tiers': {
            'espa-web': {'host': 'tst webapp hostname',
                         'repo': 'repository url'},
            'espa-maintenance': {'host': 'tst maint hostname',
                                 'repo': 'repository url'},
            'espa-production': {'host': 'tst production hostname',
                                'repo': 'repository url'}
        }
    },
    'ops': {
        'user': 'ops username',
        'tiers': {
            'espa-web': {'host': 'ops webapp hostname',
                         'repo': 'repository url'},
            'espa-maintenance': {'host': 'ops maint hostname',
                                 'repo': 'repository url'},
            'espa-production': {'host': 'ops production hostname',
                                'repo': 'repository url'}
        }
    }

}
'''

    print("deployment_settings.py could not be imported.")
    print("Module must contain site values as specified:")
    print(s)
    print("Tier hosts must also be accessible via passwordless ssh")
    print("Exiting...")

    raise e


class RemoteHost(object):
    '''Duplicated from espa_common/sshcmd.py, included here for convienience
    Runs a command on a remote host.  If no password is supplied, assumes
    you have passwordless ssh set up '''

    client = None

    def __init__(self, host, user, pw=None, debug=False):
        self.host = host
        self.user = user
        self.pw = pw
        self.debug = debug

    def __repr__(self):
        if self.pw:
            s = "RemoteHost(host=%s, user=%s, pw=xxx, debug=%s)" % (self.host,
                                                                    self.user,
                                                                    self.debug)
        else:
            s = "RemoteHost(host=%s, user=%s, debug=%s)" % (self.host,
                                                            self.user,
                                                            self.debug)
        return s

    def execute(self, command, expected_exit_status=0):
        try:
            if self.debug is True:
                print("Attempting to run [%s] on %s as %s" % (command,
                                                              self.host,
                                                              self.user))

            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if self.pw is not None:
                self.client.connect(self.host,
                                    username=self.user,
                                    password=self.pw)
            else:
                self.client.connect(self.host, username=self.user)

            stdin, stdout, stderr = self.client.exec_command(command)
            stdin.close()

            result = {'stdout': stdout.readlines(),
                      'stderr': stderr.readlines(),
                      'exit_status': stdout.channel.recv_exit_status()}

            if result['exit_status'] is not expected_exit_status:
                msg = "Error running %s." % command
                msg = "\n".join([msg, "Error:%s" % result['stderr']])
                msg = "\n".join([msg,
                                 "Exit status:%s" % result['exit_status']])
                raise Exception(msg)
            else:
                if self.debug is True:
                    out = 'None'
                    if ('stdout' in result and
                        result['stdout'] is not None and
                        len(result['stdout']) is not 0):
                        out = result['stdout']

                    print("stdout:%s" % out)

                return result

        finally:
            if self.client is not None:
                self.client.close()
                self.client = None


class Deployer(object):

    def __init__(self, branch_or_tag, environment, tier, debug=False):
        
        # tier is defined in settings
        if tier not in settings.tiers:
            raise ValueError("%s not found in deployment_settings.tiers"
                             % tier)
        self.tier = tier
        
        # branch or tagname must exist in git
        self.branch_or_tag = branch_or_tag

        # environment is defined in settings.  
        if environment not in settings.environments.keys():
            raise ValueError("%s not found in deployment_settings"
                             % environment)
                             
        # This is the name of the environment, not the object
        self.environment = environment
        
        # The username for the remote host ('espadev', etc)
        self.user = settings.environments[environment]['user']
        
        # The url of the git repo
        self.repo = settings.environments[environment]['tiers'][tier]['repo']
        
        # Get the host we should deploy to
        env = settings.environments[self.environment]
        self.host = env['tiers'][self.tier]['host']
        
        # Instantiate a remote client to the remote host
        self.remote_client = RemoteHost(self.host, self.user, debug=debug)
        

    def __pre_initialize__(self, *args, **kwargs):
        ''' Hook to perform actions before to initialization '''
        pass

    def __post_initialize__(self, *args, **kwargs):
        ''' Hook to perform actions after initialization '''
        pass

    def __pre_git__(self, *args, **kwargs):
        ''' Hook to perform actions before git command '''
        pass

    def __post_git__(self, *args, **kwargs):
        ''' Hook to perform actions after git command '''
        pass

    def __pre_delete_old__(self, *args, **kwargs):
        ''' Hook to perform actions before deleting old deployments '''
        pass

    def __post_delete_old__(self, *args, **kwargs):
        ''' Hook to perform actions after deleting old deployments '''
        pass

    def __pre_move__(self, *args, **kwargs):
        ''' Hook to perform actions before moving code from staging to 
        deployments'''
        pass

    def __post_move__(self, *args, **kwargs):
        ''' Hook to perform actions after moving code from staging to
        deployments'''
        pass

    def __pre_relink__(self, *args, **kwargs):
        ''' Hook to perform actions before relinking espa-site '''
        pass

    def __post_relink__(self, *args, **kwargs):
        ''' Hook to perform actions after relinking espa-site '''
        pass

    def __pre_cleanup__(self, *args, **kwargs):
        ''' Hook to perform actions before deleting staging directory '''
        pass

    def __post_cleanup__(self, *args, **kwargs):
        ''' Hook to perform actions after deleting staging directory '''
        pass

    def deploy(self,
               delete_previous_releases=False,
               verbose=False,
               *args,
               **kwargs):
        ''' Master deployment logic '''
        
        self.verbose = verbose
        self.delete_previous_releases = delete_previous_releases
        
        now = datetime.datetime.now()
        self.deployment_name = "%s-%s%s%s-%s%s%s" % (self.branch_or_tag,
                                                     str(now.month).zfill(2),
                                                     str(now.day).zfill(2),
                                                     str(now.year).zfill(2),
                                                     str(now.hour).zfill(2),
                                                     str(now.minute).zfill(2),
                                                     str(now.second).zfill(2))
                                                     
        # Initilize the target deployment directory
        self.initialize = ('rm -rf ~/staging;'
                           'mkdir ~/staging;'
                           'mkdir -p ~/deployments')
        
        # Pull the project from git
        git = 'git clone --depth 1 --branch {0} {1} {2}'
        git = git.format(self.branch_or_tag, self.repo, self.tier)
        self.git = 'cd ~/staging;{0}'.format(git)

        # Remove previous deployments
        self.delete_old = 'rm -rf ~/deployments/*'

        self.deployment_location = ('~/deployments/{0}'
                                   .format(self.deployment_name))
        
        # Move staged code to deployments
        self.move = 'mv ~/staging/{0} {1}'.format(self.tier,
                                                  self.deployment_location)
                                                                                            
        # Relink espa-site to point to the new deployment        
        self.relink = ('rm ~/espa-site; '
                       'ln -s ~/deployments/{0} ~/espa-site'
                       .format(self.deployment_name))

        # Clean up staging dir again.  Already done once in initialize                 
        self.cleanup = 'rm -rf ~/staging'

        if verbose is True:
            print('Deploying %s to %s' % (self.branch_or_tag,
                                          self.remote_client))
            print('Calling pre-initialize hook...')

        # call preinitialize hook
        self.__pre_initialize__(*args, **kwargs)

        if verbose is True:
            print('Initializing...')

        # initialize a clean staging directory
        self.remote_client.execute(command=self.initialize,
                                   expected_exit_status=0)

        if verbose is True:
            print('Calling post-initialize hook...')

        # call postinitialize hook
        self.__post_initialize__(*args, **kwargs)

        if verbose is True:
            print('Calling pre-git hook ...')

        self.__pre_git__(*args, **kwargs)

        if verbose is True:
            print("Pulling from Git...")

        # pull the code down from git
        self.remote_client.execute(command=self.git, expected_exit_status=0)

        if verbose is True:
            print('Calling post-git-hook ...')

        self.__post_git__(*args, **kwargs)

        # wipe out old deployments if requested
        if delete_previous_releases is True:

            if verbose is True:
                print('Calling pre-delete-old hook...')

            self.__pre_delete_old__(*args, **kwargs)

            if verbose is True:
                print("Deleting previous releases...")

            self.remote_client.execute(command=self.delete_old,
                                       expected_exit_status=0)

            if verbose is True:
                print('Calling post-delete-old hook...')

            self.__post_delete_old__(*args, **kwargs)

        if verbose is True:
            print('Calling pre-move hook...')

        self.__pre_move__(*args, **kwargs)

        if verbose is True:
            print("Deploying code...")

        # move code to deployment dir and rename
        self.remote_client.execute(command=self.move, expected_exit_status=0)

        if verbose is True:
            print('Calling post-move hook...')

        self.__post_move__(*args, **kwargs)

        if verbose is True:
            print('Calling pre-relink hook...')

        self.__pre_relink__(*args, **kwargs)

        if verbose is True:
            print("Relinking directories...")

        # reset the espa-site link
        self.remote_client.execute(command=self.relink, expected_exit_status=0)

        if verbose is True:
            print('Calling post-relink hook...')

        self.__post_relink__(*args, **kwargs)

        if verbose is True:
            print("Tier customization...")

        # run tier specific customizations
        #self.deployers[tier](remote_host, delete_previous_releases)

        if verbose is True:
            print("Calling pre-cleanup hook...")

        self.__pre_cleanup__(*args, **kwargs)

        if verbose is True:
            print("Cleaning up...")

        self.remote_client.execute(command=self.cleanup,
                                   expected_exit_status=0)

        if verbose is True:
            print("Calling post-cleanup hook...")

        self.__post_cleanup__(*args, **kwargs)

        if verbose is True:
            print("%s sucessfully deployed to %s" % (self.branch_or_tag,
                                                     self.environment))

    '''
     def __maintenance(self, remote_host, delete_previous=False, verbose=False):
        if verbose is True:
            print("Maintenance customizations...")
        move_script = 'cd ~; cp espa-site/deploy/deploy_install.py deploy_install.py'

        print('Moving new deploy_install.py to home directory...')

        # reset the espa-site link
        remote_host.execute(command=move_script, expected_exit_status=0)


        if verbose is True:
            print("Maintenance customizations complete")
    '''


class WebappDeployer(Deployer):
    ''' Deploys the espa-web project '''
    def __init__(self, *args, **kwargs):
        super(WebappDeployer, self).__init__(*args, **kwargs)
        
    def __post_move__(self, *args, **kwargs):
        # create the virtualenv after the code has been put into 
        # the deploy directory
        super(WebappDeployer, self).__post_move__(*args, **kwargs)
        
        virtual_env = 'cd {0}; virtualenv .'.format(self.deployment_location)
        print('Creating virtualenv at {0}'.format(self.deployment_location))
        self.remote_client.execute(command=virtual_env,
                                   expected_exit_status=0)
                                   
        pip_install = ('cd {0}; '
                       '. bin/activate; '
                       'pip install -r requirements.txt'
                      .format(self.deployment_location))
        print('Installing requirements')
        self.remote_client.execute(command=pip_install, expected_exit_status=0)


class ProductionDeployer(Deployer):
    ''' Deploys the espa-production project '''
    def __init__(self, *args, **kwargs):
        super(ProductionDeployer, self).__init__(*args, **kwargs)


class MaintenanceDeployer(Deployer):
    ''' Deploys the espa-maintenance project '''
    def __init__(self, *args, **kwargs):
        super(MaintenanceDeployer, self).__init__(*args, **kwargs)

    def __post_relink__(self, *args, **kwargs):
        ''' Update the maintenance tier after the new code is deployed '''

        super(MaintenanceDeployer, self).__post_relink__(*args, **kwargs)

        mv_script = ('cd ~; '
                     'cp espa-site/deploy/deploy_install.py deploy_install.py')

        print('Moving new deploy_install.py to home directory...')

        # reset the espa-site link
        self.remote_client.execute(command=mv_script,
                                   expected_exit_status=0)


''' Module level method to support deploying projects to the espa system.
    If in doubt, you should be calling this method rather than any of the 
    classes directly '''
def deploy(branch_or_tag,
           environment,
           tier,
           delete_previous_releases,
           verbose,
           debug):

    deployer = None

    if tier == 'espa-web':
        deployer = WebappDeployer(branch_or_tag=branch_or_tag,
                                  environment=environment,
                                  tier=tier,
                                  debug=debug)
    elif tier == 'espa-production':
        deployer = ProductionDeployer(branch_or_tag=branch_or_tag,
                                      environment=environment,
                                      tier=tier,
                                      debug=debug)
    elif tier == 'espa-maintenance':
        deployer = MaintenanceDeployer(branch_or_tag=branch_or_tag,
                                       environment=environment,
                                       tier=tier,
                                       debug=debug)
    else:
        raise TypeError('{0} is not a recognized tier... exiting'.format(tier))

    if deployer is not None:
        deployer.deploy(delete_previous_releases, verbose)
    else:
        print('deployer was None... exiting')


if __name__ == '__main__':

    description = "Deploys & installs ESPA projects into the named environment"

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument("--tier",
                        choices=settings.tiers,
                        required=True,
                        help="Project to deploy.")

    parser.add_argument("--environment",
                        required=True,
                        choices=settings.environments.keys(),
                        help="The environment to deploy to")

    parser.add_argument("--delete_previous_releases",
                        action="store_true",
                        help="Remove prior releases from deploy directories")

    parser.add_argument("--verbose",
                        action="store_true",
                        help="Print intermediate progress messages")

    parser.add_argument("--debug",
                        action="store_true",
                        help="Prints debugging info")

    parser.add_argument("--branch_or_tagname",
                        required=True,
                        help="Name of branch or tag to deploy")

    args = parser.parse_args()

    if args.debug is True:
        args.verbose = True

    deploy(args.branch_or_tagname,
           args.environment,
           args.tier,
           args.delete_previous_releases,
           args.verbose,
           args.debug)
