#!/bin/bash
for f in $1/*/
  do
    [ -d $f ] && cd "$f"  # && echo Entering into $f and installing packages
    for x in *.JPG; do
      t=$(date -r $x "+%Y-%m-%d %H:%M:%S")
      echo \"$t\"","$x
    done
done;
