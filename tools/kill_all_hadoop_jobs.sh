#!/usr/bin/env bash

for x in `hadoop job -list|grep job_|awk '{print $1}'`;do hadoop job -kill $x;done
