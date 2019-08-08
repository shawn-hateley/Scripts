#!/bin/bash
shopt -s nocaseglob

for f in $1*/

  do
    echo $f
    [ -d $f ] && cd "$f" # && echo Entering into $f and installing packages
    for x in *.JPG; do
      t=$(date -r $x "+%Y%m%d_%H%M%S")
      echo $x to $2"_"$t".JPG"
      #mv $x $2"_"$t".JPG"
    done
done;
