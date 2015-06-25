# Migrate ESPA data from the mysql database to postgres
# 
# Migration steps:
#  1. Set your PYTHONPATH to include the 'espa' and 'espa/web' directories
#  2. Add postgres configuration to .cfgnfo
#  3. Use manage.py to create the ESPA schema in the postgres database
#       - from the epsa/web directory
#       - python manage.py syncdb --pythonpath=../ --database=postgres
#  4. Run the migration script
#  5. Have the DBAs reset the sequence for each primary key to the table's max value

import os
import sys

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'espa_web.settings')

from django.db import transaction

#from django.contrib.contenttypes.models import ContentType
#from django.contrib.auth.models import Permission
from django.contrib.auth.models import Group, User
from ordering.models import UserProfile, Order, Scene, Configuration, DownloadSection, Download, Tag, DataPoint


def migrate_table(table, limit=500):
    print 'Starting migration of {} table'.format(table)

    count = table.objects.all().count()
    if count == 0:
        print '  - No records to migrate'
        return

    for i in xrange(count / limit + 1):
        start = i * limit
        end = (i + 1) * limit
        end = end if end < count else count

        print '  - migrating records {} - {}'.format(start, end)
        for item in table.objects.all()[start:end]:
            item.save(using='postgres')


@transaction.atomic
def migrate(limit=500):
    #migrate_table(ContentType, limit)
    #migrate_table(Permission, limit)
    migrate_table(Group, limit)
    migrate_table(User, limit)

    # ESPA Objects
    migrate_table(UserProfile, limit)
    migrate_table(Order, limit)
    migrate_table(Scene, limit)
    migrate_table(Configuration, limit)
    migrate_table(DownloadSection, limit)
    migrate_table(Download, limit)
    migrate_table(Tag, limit)
    migrate_table(DataPoint, limit)


if __name__ == '__main__':
    migrate()
