#!/usr/bin/env bash

export PYTHONPATH=$PYTHONPATH:../
export DJANGO_SETTINGS_MODULE=espa_web.settings

django-admin.py cleanup

