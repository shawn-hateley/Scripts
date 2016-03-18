#!/bin/bash
for f in *.JPG; do 
	mv "$f" "$(GetFileInfo -m $f | awk -F"/" '{print $1"_"$2"_"substr($3,1,4)}')"_"$f"; 
done