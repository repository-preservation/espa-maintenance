import paramiko
import argparse

try:
    import deployment_settings as settings
except Exception, e:
    s = '''tiers = ['webapp', 'maintenance', 'production', 'all'],

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

    def execute(self, command):
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

            return {'stdout': stdout.readlines(), 'stderr': stderr.readlines()}

        finally:
            if self.client is not None:
                self.client.close()
                self.client = None


class Deployer(object):

    url = 'https://github.com/USGS-EROS/espa.git'

    def __init__(self, branch_or_tag, environment):
        self.branch_or_tag = branch_or_tag

        if not environment in settings.environments.keys():
            raise ValueError("%s not found in deployment_settings"
                             % environment)

        self.environment = environment

        self.git_cmd = 'git clone --depth 1 --branch %s %s'
        self.git_cmd = self.git_cmd % (branch_or_tag, self.url)

        self.user = settings.environments[self.environment]['user']

        self.deployers = {}
        self.deployers['webapp'] = self.__deploy_webapp
        self.deployers['production'] = self.__deploy_production
        self.deployers['maintenance'] = self.__deploy_maintenance

    def deploy(self, tier, delete_previous_releases=False, verbose=False):
        init = 'rm -rf ~/staging; mkdir ~/staging; mkdir ~/deployments'

        git = 'cd ~/staging;%s' % self.git_cmd

        move =  'mv ~/staging/espa ~/deployments/%s' % self.branch_or_tag

        relink = 'rm ~/espa-site; ln -s ~/deployments/%s ~/espa-site'

        if not tier in settings.tiers:
            raise ValueError("%s not found in deployment_settings.tiers"
                             % tier)

        hosts = settings.environments[self.environment]['tiers'][tier]

        remote_host = RemoteHost(hosts[tier], self.user)

        #initialize a clean staging directory
        out, err = remote_host.execute(init)
        if len(err) > 0:
            msg = "Error running %s.  Error:%s" % (init, err)
            raise Exception(msg)

        #pull the code down from git
        out, err = remote_host.execute(git)
        if len(err) > 0:
            msg = "Error running %s.  Error:%s" % (git, err)
            raise Exception(msg)

        #move code to deployment dir and rename
        out, err = remote_host.execute(move)
        if len(err) > 0:
            msg = "Error running %s.  Error:%s" % (move, err)
            raise Exception(err)

        #reset the espa-site link
        out, err = remote_host.execute(relink)
        if len(err) > 0:
            msg = "Error running %s.  Error:%s" % (relink, err)
            raise Exception(err)

        self.deployers[tier](remote_host, delete_previous_releases)


    def __deploy_webapp(self, remote_host, delete_previous=False):
        pass

    def __deploy_production(self, remote_host, delete_previous=False):
        pass

    def __deploy_maintenance(self, remote_host, delete_previous=False):
        pass


if __name__ == '__main__':

    epilog = "Deploys and installs ESPA into the named environment\n\n"

    formatter = argparse.RawDescriptionHelpFormatter

    parser = argparse.ArgumentParser(epilog=epilog,
                                     formatter_class=formatter)

    parser.add_argument("--tier",
                        choices=settings.tiers,
                        help="Portion of system to deploy")

    parser.add_argument("--environment",
                        required=True,
                        choices=settings.environments.keys(),
                        help="The environment to deploy to")

    parser.add_argument("--delete_previous_releases",
                        action="store_true",
                        help="Delete prior releases from deploy directories")

    parser.add_argument("--verbose",
                        action="store_true",
                        help="Print intermediate progress messages")

    parser.add_argument("--branch_or_tagname",
                        required=True,
                        help="Name of branch or tag to deploy")

    args = parser.parse_args()

    deployer = Deployer(args.branch_or_tagname, args.environment)

    if args.tier:
        deployer.deploy(args.tier, args.delete_previous_releases, args.verbose)
    else:
        for tier in settings.tiers:
            deployer.deploy(tier, args.delete_previous_releases, args.verbose)
