#!/bin/bash
################################################################################
# This script is used to export raw salt dose data to the Hecate web server
#
# Created by: Ray Brunsting (ray@hakai.org)
# Created on: September 21, 2015
################################################################################

scriptRunningMarker=/tmp/exportDoseData.running
if [ -e "$scriptRunningMarker" ]; then
	echo `date` "${0} already running, skipping!"
	exit 0
fi
touch "$scriptRunningMarker"

sourceFolder="hakai@hecate.hakai.org:/data/LoggerNet/CalvertData/"
destinationFolder="/Users/shawnhateley/Projects/Test_Data"

rsyncParameters="-av"
rsyncParameters="$rsyncParameters --include=*/"
rsyncParameters="$rsyncParameters --include=KCBuoy_SeaologySamples.dat"
rsyncParameters="$rsyncParameters --exclude=*"
rsyncParameters="$rsyncParameters --prune-empty-dirs"

echo `date` Synchronizing salt dosage data from $sourceFolder to $destinationFolder
rsync $rsyncParameters "$sourceFolder" "$destinationFolder"

for datFilename in `find "$destinationFolder" -name *.dat -o -name *.backup`; do
	csvFilename="${datFilename}.csv"
	mv "$datFilename" "$csvFilename"
done

rm -f "$scriptRunningMarker"

#
# End of export-dose-data.bash
#