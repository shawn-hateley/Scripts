#!/bin/bash
#dir=$1/*/
#echo $dir
for f in $1/*/
  do
     [ -d $f ] && cd "$f" && echo Entering into $f
     ls -f . | wc -l
  done;
