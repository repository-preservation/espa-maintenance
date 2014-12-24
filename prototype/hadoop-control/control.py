import paramiko
import requests
from bs4 import BeautifulSoup

def run_ssh(command):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect('l8srlscp05.cr.usgs.gov', username='espa')
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
            
def get_status():
    '''this is  nice to give us the job id and the running tasks, but it doesnt
    give us the job name, which is what we need to cross reference scenes in 
    out db... probably need to scrape the html page for it'''
    
    results = run_ssh('hadoop job -list')
    
    output = {}
        
    '''['2 jobs currently running\n',
        'JobId\tState\tStartTime\tUserName\tPriority\tSchedulingInfo\n',
        'job_201412220946_0132\t1\t1419446902589\tespa\tNORMAL\t253 running map tasks using 253 map slots. 0 additional slots reserved. 0 running reduce tasks using 0 reduce slots. 0 additional slots reserved.\n',
        'job_201412220946_0133\t1\t1419447125492\tespa\tNORMAL\t3 running map tasks using 3 map slots. 0 additional slots reserved. 0 running reduce tasks using 0 reduce slots. 0 additional slots reserved.\n'
    ]'''
            
    for line in results:            
        if line.lower().startswith('job_'):
            fields = line.split('\t')
            count = fields[5].split(' ')[0]
            output[fields[0]] = count
       
    return output

def get_http_status():
    url = 'http://l8srlscp05.cr.usgs.gov:50030/jobtracker.jsp'
    response = requests.get(url)
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
            total = tds[7].text
            complete = tds[8].text
            running = tds[13].text.split(' ')[0]
            results[jobid] = {'name': name, 'total': total,
                      'complete': complete, 'running': running}
                        
    return results            
            
        
        
    
    
    
    
   
    
    #print soup
    
        
if __name__ == '__main__':
    status = get_http_status()
    if status:
        print(status)

