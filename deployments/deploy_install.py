#!/usr/bin/env python

import paramiko
import argparse
import datetime

try:
    import deployment_settings as settings
except Exception, e:
    s = '''tiers = ['webapp', 'maintenance', 'production', 'all']

environments = {
    'dev': {
        'user': 'dev username',
        'tiers': {
            'webapp': 'dev webapp hostname',
            'maintenance': 'dev maint hostname',
            'production': 'dev production hostname'
        }
    },
    'tst': {
        'user': 'tst username',
        'tiers': {
            'webapp': 'tst webapp hostname',
            'maintenance': 'tst maint hostname',
            'production': 'tst production hostname'
        }
    },
    'ops': {
        'user': 'ops username',
        'tiers': {
            'webapp': 'ops webapp hostname',
            'maintenance': 'ops maint hostname',
            'production': 'ops production hostname'
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
    '''Duplicated from espa_common/sshcmd.py, included here for convienience'''

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

    url = 'https://github.com/USGS-EROS/espa.git'

    def __init__(self, branch_or_tag, environment):
        self.branch_or_tag = branch_or_tag

        if environment not in settings.environments.keys():
            raise ValueError("%s not found in deployment_settings"
                             % environment)

        self.environment = environment

        self.git_cmd = 'git clone --depth 1 --branch %s %s'
        self.git_cmd = self.git_cmd % (branch_or_tag, self.url)

        self.user = settings.environments[self.environment]['user']

        self.deployers = {}
        self.deployers['webapp'] = self.__webapp
        self.deployers['production'] = self.__production
        self.deployers['maintenance'] = self.__maintenance

    def deploy(self,
               tier,
               delete_previous_releases=False,
               verbose=False,
               debug=False):

        now = datetime.datetime.now()
        deployment_name = "%s-%s%s%s-%s%s%s" % (self.branch_or_tag,
                                                str(now.month).zfill(2),
                                                str(now.day).zfill(2),
                                                str(now.year).zfill(2),
                                                str(now.hour).zfill(2),
                                                str(now.minute).zfill(2),
                                                str(now.second).zfill(2))

        init = 'rm -rf ~/staging; mkdir ~/staging; mkdir -p ~/deployments'
        git = 'cd ~/staging;%s' % self.git_cmd
        delete_old = 'rm -rf ~/deployments/*'
        move = 'mv ~/staging/espa ~/deployments/%s' % deployment_name
        relink = 'rm ~/espa-site; ln -s ~/deployments/%s ~/espa-site' \
                 % deployment_name
        cleanup = 'rm -rf ~/staging'

        if tier not in settings.tiers:
            raise ValueError("%s not found in deployment_settings.tiers"
                             % tier)

        host = settings.environments[self.environment]['tiers'][tier]

        remote_host = RemoteHost(host, self.user, debug=debug)

        if verbose is True:
            print("Deploying %s to %s" % (self.branch_or_tag, remote_host))
            print("Initializing...")

        # initialize a clean staging directory
        remote_host.execute(command=init, expected_exit_status=0)

        if verbose is True:
            print("Pulling from Git...")

        # pull the code down from git
        remote_host.execute(command=git, expected_exit_status=0)

        # wipe out old deployments if requested
        if delete_previous_releases is True:

            if verbose is True:
                print("Deleting previous releases...")

            remote_host.execute(command=delete_old, expected_exit_status=0)

        if verbose is True:
            print("Deploying code...")

        # move code to deployment dir and rename
        remote_host.execute(command=move, expected_exit_status=0)

        if verbose is True:
            print("Relinking directories...")

        # reset the espa-site link
        remote_host.execute(command=relink, expected_exit_status=0)

        if verbose is True:
            print("Tier customization...")

        # run tier specific customizations
        self.deployers[tier](remote_host, delete_previous_releases)

        if verbose is True:
            print("Cleaning up...")

        remote_host.execute(command=cleanup, expected_exit_status=0)

        if verbose is True:
            print("%s sucessfully deployed to %s" % (self.branch_or_tag,
                                                     self.environment))

    def __webapp(self, remote_host, delete_previous=False, verbose=False):
        if verbose is True:
            print("Webapp customizations...")

        if verbose is True:
            print("Webapp customizations complete")

    def __production(self, remote_host, delete_previous=False, verbose=False):
        if verbose is True:
            print("Production customizations...")

        if verbose is True:
            print("Production customizations complete")

    def __maintenance(self, remote_host, delete_previous=False, verbose=False):
        if verbose is True:
            print("Maintenance customizations...")

        if verbose is True:
            print("Maintenance customizations complete")

if __name__ == '__main__':

    description = "Deploys and installs ESPA into the named environment"

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument("--tier",
                        choices=settings.tiers,
                        help="Portion of system to deploy.  If blank, code \
                        will deploy to all available tiers")

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

    deployer = Deployer(args.branch_or_tagname, args.environment)

    if args.debug is True:
        args.verbose = True

    if args.tier:
        deployer.deploy(args.tier,
                        args.delete_previous_releases,
                        args.verbose,
                        args.debug)
    else:
        for tier in settings.tiers:
            deployer.deploy(tier,
                            args.delete_previous_releases,
                            args.verbose,
                            args.debug)
