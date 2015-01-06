import paramiko
import requests
from bs4 import BeautifulSoup
from argparse import ArgumentParser
import pprint


class Hadoop(object):
    '''Class that holds all the functionality we'll use for interacting with
    our Hadoop cluster.  The machine this is run on must have passwordless
    ssh access to the Hadoop master node.'''

    def __init__(self, master_node_host, username):
        self.master_node = master_node_host
        self.master_node_username = username
        self.job_tracker_url = ('http://%s:50030/jobtracker.jsp'
                                % self.master_node)

    def run_ssh(self, command):
        '''Utility to execute remote ssh commands
        Keyword args:
        command - The command to execute on the remote host

        Returns:
        The result from standard out on the remote host
        '''

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(self.master_node, username=self.master_node_username)
            stdin, stdout, stderr = ssh.exec_command(command)
            return stdout.readlines()
        except Exception, e:
            print("Exception running:%s" % command)
            print(e)
            return None
        finally:
            if ssh:
                ssh.close()
            ssh = None

    def kill_job(self, jobid):
        '''Kills a hadoop job on the master node
        Keyword args:
        jobid - The id of the Hadoop job to kill

        Returns:
        The results of the hadoop job -kill command
        '''

        results = self.run_ssh('hadoop job -kill %s' % jobid)
        return results

    def list_jobs(self):
        '''Lists all active hadoop jobs on the master node
        Returns:
        A dictionary:
            jobid = {name, total maps, complete maps, running maps}
        '''
        response = requests.get(self.job_tracker_url)
        soup = BeautifulSoup(response.content)
        response.close()
        target = soup.find(id='running_jobs')

        while(target.name != 'table'):
            target = target.next_element

        trs = target.find_all('tr')

        results = {}
        for tr in trs:
            tds = tr.find_all('td')
            if 'id' in tds[0].attrs:
                jobid = tds[0].text
                name = tds[4].text
                if 'cellspacing' in tds[7].attrs:
                    total = tds[8].text
                    complete = tds[9].text
                    running = tds[14].text.split(' ')[0]
                else:
                    total = tds[7].text
                    complete = tds[8].text
                    running = tds[13].text.split(' ')[0]
                results[jobid] = {'name': name, 'total': total,
                                  'complete': complete, 'running': running}

        return results

if __name__ == '__main__':
    parser = ArgumentParser(description='Lists and kills Hadoop jobs',
                            prog='hadoop_job_control')

    parser.add_argument('-m', '--master_host', dest='master_host', required=True,
                        help='the hadoop master node')

    parser.add_argument('-u', '--user', dest='username', required=True,
                        help='hadoop master node username')

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('-l', '--list', action='store_true',
                       help='list active Hadoop jobs')

    group.add_argument('-k', '--kill', dest='kill_job',
                       help='Kill a Hadoop job')
    args = parser.parse_args()


    hadoop = Hadoop(args.master_host, username=args.username)
    printer = pprint.PrettyPrinter(indent=4)
    
    if args.list:
        jobs = hadoop.list_jobs()
        if not jobs:
            jobs = 'No jobs found'
        printer.pprint(jobs)       
        
    elif args.kill_job:
        result = hadoop.kill_job(args.kill_job)
        printer.pprint(result)

    #hadoop = Hadoop('l8srlscp05.cr.usgs.gov', username='espa')
    #jobs = hadoop.list_jobs()
    #if jobs:
    #    printer = pprint.PrettyPrinter(indent=4)
    #    printer.pprint(jobs)
