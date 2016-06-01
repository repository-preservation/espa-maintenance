## espa-maintenance
This is a project to hold scripts needed to deploy our code, 
changes system password, generate daily reports, etc.

Code must be installed to a node that has passwordless ssh 
enabled for the account to the master hadoop nodes, 
uwsgi servers and itself.

This is operationally deployed to the espa dev account to manage 
the dev environment, the espa tst account to manage test and 
the espa account to manage ops, with deployment_settings.py 
constructed appropriately.

## Changlog

###### Version 1.1.0 (May 2016)
* Support for the new log rotation

###### Version 1.0.7 (April 2016)
* deployment changes to support API and web changes

###### Version 1.0.6 (April 2016)
* Added download statistics by product

###### Version 1.0.5 (February 2016)
* Minor boiler plate text change

###### Version 1.0.4 (December 2015)
* Re-written lsrd_stats.py and change_credentials.py
* Removed need for pig scripts

###### Version 1.0.3 (October 2015)
* Updates for postgres db vs mysql (October 2015)
