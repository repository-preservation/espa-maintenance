#!/usr/bin/env python
import paramiko
import os
import argparse
import sys
import time
import memcache
import datetime

__author__ = "David V. Hill"

class SceneCache(object):

    #file used to indicate that a scene_cache update process is running
    lock_file = '/tmp/scene_cache.lock'
    
    def __init__(self):
        pass
    
    def __del__(self):
        pass
        
    def __get_memcache_client(self):
        c = memcache.Client(['127.0.0.1:11211'], debug=1)
        if c is None:
            raise Exception("Could not create memcached client...")
        return c
    
    def __get_ssh_client(self):
        '''Returns an open ssh client to the online cache'''    
        
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect('edclpdsftp.cr.usgs.gov', username='espa')
        return ssh

    def __run_command(self,ssh, command):
        '''Runs a specified system command against the supplied ssh connection and
        returns the result as a list, err as a string'''
        
        try:
            stdin,stdout,stderr = ssh.exec_command(command)
            out = stdout.readlines()
            err = stderr.read()
            return out,err
        except Exception, e:
            print "an exception occurred"
            print e

    def set_lock_file(self):
        '''Sets a lock file in self.lock_file so the scene_cache can prevent running if another
           instance of it is already in progress.  Raises an exception if a lock_file is already
           in place'''
        if self.is_lock_file_set():
            raise Exception("Cannot set lock file: Lock file %s already exists" % self.lock_file)
        else:
            h = open(self.lock_file, 'wb+')
            h.write(str(datetime.datetime.now()))
            h.write("\n")
            h.flush()
            h.close()
        return True
        

    def is_lock_file_set(self):
        '''Returns True if a lock file is already in place, False otherwise'''
        return (os.path.exists(self.lock_file) and os.path.isfile(self.lock_file))

    def clear_lock_file(self):
        '''Clear the lock file set at self.lock_file'''
        return os.unlink(self.lock_file)
                

    def get_scene_cache(self):
        '''retrieves a listing of all tm, etm and nlaps scenes currently available
        on the online cache'''
        results = {}
        results['scenes'] = list()
        results['nlaps'] = list()
    
        ssh = None
        try:
            attempts = 0
            while ssh is None and attempts <= 3:
                try:
                    attempts = attempts + 1
                    ssh = self.__get_ssh_client()
                except Exception, e:
                    print ("Error getting ssh connection , retrying")
                    continue
        
            if ssh is None:
                print ("Could not get ssh connection after %i attempts") % attempts
                return
            
            etm_out,etm_err = self.__run_command(ssh, 'ls -R /data/standard_l1t/etm |grep gz')
            tm_out,tm_err = self.__run_command(ssh, 'ls -R /data/standard_l1t/tm |grep gz')
            nlaps_out,nlaps_err = self.__run_command(ssh, 'ls -R /data/standard_l1t/nlaps/tm |grep gz')
    
            for e in etm_out:
                if len(e) > 3 and e.startswith('L'):
                    e = e.replace('.tar.gz', '')
                    e = e.replace('\n', '')
                    results['scenes'].append(e)
        
            etm_out = None
            etm_err = None
        
            for e in tm_out:
                if len(e) > 3 and e.startswith('L'):
                    e = e.replace('.tar.gz', '')
                    e = e.replace('\n', '')
                    results['scenes'].append(e)
    
            tm_out = None
            tm_err = None
        
            for e in nlaps_out:
                if len(e) > 3 and e.startswith('L'):
                    e = e.replace('.tar.gz', '')
                    e = e.replace('\n', '')
                    results['nlaps'].append(e)
                        
            nlaps_out = None
            nlaps_err = None
        
            return results
    
        except Exception, e:
            print ("An exception occurred in get_scene_cache():%s") % e
        finally:
            if ssh is not None:
                ssh.close()
                ssh = None
    
    
    def load_cache(self):

        try:
            self.set_lock_file()
        except Exception,e:
            print ("An error occurred in scene_cache.load_cache():")
            print e
            return False

        
        results = self.get_scene_cache()
        cache = self.__get_memcache_client()

        #Tell clear_cache not to bother with the lock file
        #as we already are handling it in this method
        self.clear_cache(honor_lock_file=False)
        
        for r in results['scenes']:
            cache.set(r, True)                
        cache.set('nlaps', results['nlaps'])

        cache.set('last_updated', time.time())

        cache.disconnect_all()   
        cache = None 
        results = None

        self.clear_lock_file()
        

    def clear_cache(self, honor_lock_file=True):

        if honor_lock_file:
            try:
                self.set_lock_file()
            except Exception,e:
                print ("An error occurred in scene_cache.clear_cache():")
                print e
                return False
        
        cache = self.__get_memcache_client()
        cache.flush_all()
        cache.disconnect_all()

        if honor_lock_file:
            self.clear_lock_file()
        
        
    def has_scenes(self,scene_names):
        '''returns a list of the scene names available in the cache.  Unavailable scenes supplied in the input parameter
        are not included in the result'''        
        cache = self.__get_memcache_client()
        result = cache.get_multi(scene_names).keys()
        cache.disconnect_all()
        return result

    def last_updated(self):
        cache = self.__get_memcache_client()
        result = cache.get('last_updated')
        cache.disconnect_all()
        return result

    def is_nlaps(self, scene_names):
        cache = self.__get_memcache_client()
        nlaps = set(cache.get('nlaps'))
        results = nlaps.intersection(set(scene_names))
        cache.disconnect_all()
        return list(results)        
        
        
                
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Manage the scene cache')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--reload', action="store_true", help='reload the scene cache')
    group.add_argument('--clear', action="store_true", help='clear the scene cache')
    group.add_argument('--listscenes', dest='scenes', metavar='scenename', nargs='*', help='scenes to search for in the cache')
    group.add_argument('--is_nlaps', dest='is_nlaps', metavar='scenename', nargs='*', help='scenes to search the nlaps inventory for')
    group.add_argument('--last_updated', action="store_true", help="returns timestamp (seconds since epoch) cache last populated.  No result if empty")
    args = parser.parse_args()
    

    if args.reload:
        SceneCache().load_cache()
    elif args.clear:
        SceneCache().clear_cache()
    elif args.scenes:
        results = SceneCache().has_scenes(list(args.scenes))
        for r in results:
            print r
    elif args.is_nlaps:
        results = SceneCache().is_nlaps(list(args.scenes))
        for r in results:
            print r
    elif args.last_updated:
        print SceneCache().last_updated()
    else:
        print parser.print_usage()
        
    sys.exit(0)
