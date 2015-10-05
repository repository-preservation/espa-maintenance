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
Version 1.03
Updates for postgres db vs mysql (October 2015)
