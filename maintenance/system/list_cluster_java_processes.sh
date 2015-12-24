#!/usr/bin/env bash

for x in `cat ~/bin/hadoop/conf/slaves`;do echo ""; echo "Java Processes for $x"; ssh -q $x jps; done

