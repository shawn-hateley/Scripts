 #!/bin/bash
DATE="$1"
FILE="$2"
FDATE=$(ls -l $FILE | awk '{ print $7 " " $6}')
ECHO $DATE
ECHO $FILE
ECHO $FDATE
#if [ "$DATE" == "$FDATE" ];then
#vi $FILE
#fi
 