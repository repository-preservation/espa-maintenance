#!/usr/bin/env python

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

    def is_running(self):
        '''Checks to see if Hadoop is running on the master node
        Returns:
        True if running, False if not
        '''

        results = self.run_ssh('hadoop job -list')
        if len(results) > 0 and 'jobs currently running' in results[0].lower():
            return True
        else:
            return False

    def stop(self):
        ''' Stops the hadoop cluster.  If its already stopped does nothing.

        Returns:
        True if the cluster stopped, False if not.
        '''

        if self.is_running():
            self.run_ssh('stop-all.sh')
            if not self.is_running():
                return True
            else:
                return False
        return True

    def start(self):
        ''' Starts the hadoop cluster.  Does nothing if its already running

        Returns:
        True if the cluster started, False if not.
        '''

        if not self.is_running():
            self.run_ssh('start-all.sh')
            if self.is_running():
                return True
            else:
                return False
        return True

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
        response = requests.get(self.job_tracker_url, timeout=5)
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

def kill_job(master_node, username, jobid):
    '''Kills Hadoop jobid on the master_node'''
    return Hadoop(master_node, username).kill_job(jobid)

def list_jobs(master_node):
    '''Lists all active Hadoop jobs on the master node'''
    return Hadoop(master_node, None).list_jobs()

def is_running(master_node, username):
    '''Returns True if Hadoop is running on master node, False if not'''
    return Hadoop(master_node, username).is_running()

def stop(master_node, username):
    '''Stops the hadoop cluster'''
    return Hadoop(master_node, username).stop()

def start(master_node, username):
    '''Starts the hadoop cluster.  If its already running, does nothing'''
    return Hadoop(master_node, username).start()

if __name__ == '__main__':
    parser = ArgumentParser(description='Hadoop cluster controller')

    parser.add_argument('--master_host', dest='master_host',
                        required=True, help='the hadoop master node')

    parser.add_argument('--user', dest='username', required=True,
                        help='hadoop master node username')

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('--list', action='store_true',
                       help='list active Hadoop jobs')

    group.add_argument('--kill', dest='kill_job',
                       help='Kill a Hadoop job')

    group.add_argument('--start', action='store_true',
                       help='Start hadoop cluster')

    group.add_argument('--stop', action='store_true',
                       help='Stop hadoop cluster')

    group.add_argument('--is_running', action='store_true',
                       help='Checks if Hadoop is running')

    args = parser.parse_args()

    hadoop = Hadoop(args.master_host, username=args.username)

    printer = pprint.PrettyPrinter(indent=4)

    if args.list:
        jobs = hadoop.list_jobs()
        if not jobs:
            jobs = 'No jobs found'
        printer.pprint(jobs)
    elif args.kill_job:
        printer.pprint(hadoop.kill_job(args.kill_job))
    elif args.start:
        printer.pprint(hadoop.start)
    elif args.stop:
        printer.pprint(hadoop.stop)
    elif args.is_running:
        printer.pprint(hadoop.is_running())
