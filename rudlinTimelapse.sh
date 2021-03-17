  #!/bin/bash  
dd=`date  +%Y-%m-%d -d "-1 day"`
cd /media/usbstick/webcam/$dd/
ffmpeg -framerate 5 -pattern_type glob -i '*.jpg' -c:v libx264 -r 30 /media/usbstick/timelapse/$dd.mp4