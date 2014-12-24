import paramiko


def get_status():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print("connecting...")
    ssh.connect('l8srlscp24.cr.usgs.gov', username='espadev')
    print("executing command...")
    try:
        stdin, stdout, stderr = ssh.exec_command('hadoop job -list')
        
        errs = stderr.readlines()

        if len(errs) > 0:
            raise Exception(errs)

        results = {}
        output = stdout.readlines()
        for line in output:
            
            #'0 jobs currently running\n', 'JobId\tState\tStartTime\tUserName\tPriority\tSchedulingInfo\n'
                    
        return stdout.readlines()
    except:
        print("The connection was closed... aborting")
    finally:    
        ssh.close()
        ssh = None
        
if __name__ == '__main__':
    print(get_status())

