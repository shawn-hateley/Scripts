  #!/bin/bash  
dd=`date -v -1d '+20%y-%m-%d'`
timelapse  -v -b 8 -o ~/Pictures/EyeFi/TimeLapse/$dd.mp4 ~/Pictures/EyeFi/$dd
python upload_video.py --file=$dd.mp4 --title=$dd --description="Hakai Magazine 3rd Floor TimeLapse" 
