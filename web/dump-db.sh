#!/usr/bin/env bash

export PYTHONPATH=$PYTHONPATH:../
export DJANGO_SETTINGS_MODULE=espa_web.settings

django-admin.py dumpdata >> /home/espa/opsdb.dump
cd /home/espa
gzip opsdb.dump

