#!/bin/bash
################################################################################
# This script is used to organize and synchronize the webcam images
# captured on Quadra
#
# Created by: Ray Brunsting (ray@hakai.org)
# Created on: December 17, 2013
################################################################################

scriptRunningMarker=/tmp/organizeAndSynchronizeWebcamImages.running
if [ -e "${scriptRunningMarker}" ]; then
	echo `date` "${0} already running, skipping!"
	exit 0
fi
touch "${scriptRunningMarker}"

imageMoveList="/tmp/imageMove.list"
imageSyncList="/tmp/imageSync.list"

remoteUser="hakai"
remoteHost="hecate2.hakai.org"

if [ -d "/Volumes/Sensor_Network/Incoming_Done" ]; then
	webcamImageFolder="/Volumes/Sensor_Network/Incoming_Done"
#elif [ -d "/data/webcams" ]; then
#	webcamImageFolder="/data/webcams"
#elif [ -d "/share/CACHEDEV3_DATA/webcams/" ]; then
#	webcamImageFolder="/share/CACHEDEV3_DATA/webcams/"
#elif [ -d "/share/CACHEDEV2_DATA/webcams" ]; then
#	webcamImageFolder="/share/CACHEDEV2_DATA/webcams"
else
	echo `date` "ERROR: failed to find webcam image folder!"
	rm -f "${scriptRunningMarker}" "${imageMoveList}" "${imageSyncList}"
	exit -1
fi

hostname=`hostname`
if [ "${hostname}" == "Hakai-562-Shawn.local" ]; then
	remoteDataFolder="${remoteUser}@${remoteHost}:/data/webcams/calvert"
#elif [ "${hostname}" == "calvert2.hakai.org" ]; then
#	remoteDataFolder="${remoteUser}@${remoteHost}:/data/webcams/calvert"
#elif [ "${hostname}" == "Calvert-NAS" ]; then
#		remoteDataFolder="${remoteUser}@${remoteHost}:/data/webcams/calvert"
#elif [ "${hostname}" == "quadra.hakai.org" ]; then
#	remoteDataFolder="${remoteUser}@${remoteHost}:/data/webcams/quadra"
#elif [ "${hostname}" == "quadra2.hakai.org" ]; then
#	remoteDataFolder="${remoteUser}@${remoteHost}:/data/webcams/quadra"
#elif [ "${hostname}" == "Quadra-NAS" ]; then
#	remoteDataFolder="${remoteUser}@${remoteHost}:/data/webcams/quadra"
else
	echo `date` "ERROR: unrecognized hostname (${hostname})!"
	rm -f "${scriptRunningMarker}" "${imageMoveList}" "${imageSyncList}"
	exit -1
fi

cd "${webcamImageFolder}"
find . -not -path '*/.*' -maxdepth 3 -type f -mmin +1 -name "*.JPG" > "${imageMoveList}"

cat "${imageMoveList}" | while read webcamImage; do
	basename=`basename "${webcamImage}"`
	datename=`echo "${basename}" | sed -e "s/.*\(20[0-9][0-9][0-9][0-9][0-9][0-9]\).*/\1/"`

	if [ "${datename}" != "" ]; then
		dirname=`dirname "${webcamImage}"`
		destfolder="${dirname}/${datename}"

		echo `date` moving ${webcamImage} to ${destfolder}
		mkdir -p "${destfolder}"
		mv "${webcamImage}" "${destfolder}"
	fi
done

# Synchronize all images created in the last three days
#echo `date` Synchronizing ${webcamImageFolder} to ${remoteHost}
#find . -maxdepth 3 -type f -mtime -5 -name "*.jpg" > "${imageSyncList}"
#rsync -av --chmod=Du=rwx,Dgo=rx,Fu=rw,Fog=r --files-from="${imageSyncList}" --prune-empty-dirs . ${remoteDataFolder}
#echo `date` Finished synchronizing ${webcamImageFolder} to ${remoteHost}

rm -f "${scriptRunningMarker}" #"${imageMoveList}" "${imageSyncList}"

#
# End of organize-and-synchronized-webcam-images.bash
#
