#!/bin/bash
for f in *.JPG; do 
	mv "$f" "$(ls $f | awk -F " " '{print substr($1,1,4)"0"substr($1,5,4)}')".JPG"";
done