import paramiko
import argparse

try:
    import deployment_settings
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
    
    def __init__(self, branch_or_tag):
        self.git_cmd = 'git clone --depth 1 --branch %s %s'
        self.git_cmd = self.git_cmd % (branch_or_tag, self.url)
        
    def deploy(self):
        pass
                                                                                      
                                                                                      
class WebappDeployer(Deployer):
    def deploy(self):
        super(WebappDeployer, self).deploy()


class ProductionDeployer(Deployer):
    def deploy(self):
        super(WebappDeployer, self).deploy()


class MaintenanceDeployer(Deployer):
    def deploy(self):
        super(WebappDeployer, self).deploy()
    
    
if __name__ == '__main__':

    epilog = "Deploys and installs ESPA into the named environment\n\n"

    formatter = argparse.RawDescriptionHelpFormatter
    
    parser = argparse.ArgumentParser(epilog=epilog,
                                     formatter_class=formatter)

    parser.add_argument("--tier",
                        required=True,
                        choices=deployment_settings.tiers,
                        help="Portion of system to deploy")

    parser.add_argument("--environment",
                        required=True,
                        choices=deployment_settings.environments.keys(),
                        help="The environment to deploy to")
                        
    parser.add_argument("--delete-previous-releases",
                        action="store_true",
                        help="Delete prior releases from deploy directories")

    parser.add_argument("--branch_or_tagname",
                        required=True,
                        help="Name of branch or tag to deploy")
    

    args = parser.parse_args()
    #args.environment
