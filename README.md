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


#### Support Information

This project is unsupported software provided by the U.S. Geological Survey (USGS) Earth Resources Observation and Science (EROS) Land Satellite Data Systems (LSDS) Project. For questions regarding products produced by this source code, please contact the [Landsat Contact Us][2] page and specify USGS Level-2 in the "Regarding" section.

#### Disclaimer

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.


[2]: https://landsat.usgs.gov/contact
    
