#!/bin/bash
for f in *.JPG; do 
	mv "$f" "$(ls $f | awk -F"_" '{print $1"_1"$2.JPG}')"; 
done