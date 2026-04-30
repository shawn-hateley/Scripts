#!/usr/bin/Rscript
##-----------------------------------------------------------------------------------------------
# Created by: Margot Vore 
# May 2021
# 
# This code reads in data from new sensor calibration field sheets in google drive and writes it to the database. 
# Before running this code, make sure all sensors and barrel fills are up to date as it will rely on this
# information to input the correct data into the database.
#
# This code enters data into the following database tables:
# Calibration Events
# Calibration Results
#
# Abbreviations:
# EC --> Electrical Conductivity
# CF  -->  Calibration Factor



##-----------------------------------------------------------------------------------------------
## ---------------------------Setting up the workspace------------------------------------------
##-----------------------------------------------------------------------------------------------
#setting up the environment
#readRenviron('/home/autosalt/AutoSaltDilution/other/.Renviron')
# readRenviron('C:/Program Files/R/R-3.6.2/.Renviron')
#options(java.parameters = "-Xmx8g")
#options(warn = - 1)

#libraries
#suppressMessages(library(googledrive))
#suppressMessages(library(DBI))
suppressMessages(library(openxlsx))
suppressMessages(library(lubridate))
suppressMessages(library(stringi))
#suppressMessages(library(prodlim))
suppressMessages(library(broom))

# #connect to database and google drive
#con <- dbConnect(RPostgres::Postgres(), dbname=Sys.getenv('dbname'),host=Sys.getenv('host'),user=Sys.getenv('user'),password=Sys.getenv('password'))
#drive_auth(path="/home/autosalt/AutoSaltDilution/other/Oauth.json")

# ##-----------------------------------------------------------------------------------
# ##-------------- Finding CF field sheets that are new to the drive------------------
# ##----------------------------------------------------------------------------------
#print statements for log
cat("\n")
print('----------------------------------------------------')
print('----------------------------------------------------')
print(sprintf("Date and Time:%s", Sys.time()))

#select all entries from the googledriveid table
#query <- sprintf("SELECT * FROM chrl.googledriveid")
#Old_CF_Events <- dbGetQuery(con, query)

#find which sheet in the google drive are new
#Drive_Sheets <- drive_ls("AutoSalt_Hakai_Project/CF_Measurements")
#New_Events <- Drive_Sheets[!(Drive_Sheets$id %in% Old_CF_Events$driveid), ]

#if there is no new events,  stop script
#if (nrow(New_Events)<1){
#  print('There are no new CF events to upload')
## quit()
#}

##-----------------------------------------------------------------------------------------
##------------------ Transferring data to database-----------------------------------------
##-----------------------------------------------------------------------------------------
CF_Summary <- data.frame(); Sensor_Summary <- data.frame(); Events_added <- data.frame()
Num <- 1


#for (x in c(1:nrow(New_Events))){ 
  #New_Events[x,'id_char'] = as.character(New_Events[x,'id'])
  CF_File <- '/Users/shawnhateley/Downloads/drive-download-20260202T190842Z-3-001/WTS703-20250505-Mid2.xlsx'
  # CF_File <- 'working_directory/NewCF.xlsx'
  #EA <- data.frame(name= New_Events[x,'name'],Googleid=New_Events[x,'id_char'], added= Sys.Date(), Num= Num)
  #Events_added <- rbind(Events_added,EA)
  
  # Downloads file locally and reads it into R
  #drive_download(file=sprintf("AutoSalt_Hakai_Project/CF_Measurements/%s",New_Events[x,'name']), path= CF_File, overwrite = T)
  workbook <- read.xlsx(CF_File, sheet='Calibration',skipEmptyRows = F,skipEmptyCols = F)

  # select metadata from the excel sheet
  SiteID <- as.integer(workbook[2,2])
  Date <- as.Date(as.integer(workbook[3,2]),origin='1899-12-30')
  PMP <- workbook[6,2]
  
  
  ##---------------------------------------------------------------------------
  ##------------------- Determining the barrel period--------------------------
  ##---------------------------------------------------------------------------

  # if (PMP=='Mid'){
  #   query <- sprintf("SELECT * FROM chrl.barrel_periods WHERE (Starting_Date <= '%s') AND (Ending_Date >= '%s') AND (SiteID=%s)",Date,Date,SiteID)
  #   Periods <- dbGetQuery(con, query)
    
  #   # If end period is NA,  indicates the event happened in the current barrel period
  #   if (nrow(Periods)==0){
  #     query <- sprintf("SELECT * FROM chrl.barrel_periods WHERE (Starting_Date <= '%s') AND (Ending_Date IS NULL) AND (SiteID='%s')",Date,SiteID)
  #     Periods <- dbGetQuery(con, query)
  #   }
    
    
  # } else if (PMP=='Pre'){
  #     query <- sprintf("SELECT * FROM chrl.barrel_periods WHERE (Starting_Date <= '%s') AND (Ending_Date >= '%s') AND (SiteID=%s)",Date,Date,SiteID)
  #     Periods <- dbGetQuery(con, query)
      
      
  # } else if (PMP=='Post'){
  #    query <- sprintf("SELECT * FROM chrl.barrel_periods WHERE (Starting_Date <= '%s') AND (Ending_Date IS NULL) AND (SiteID='%s')",Date,SiteID)
  #    Periods <- dbGetQuery(con, query)
  # }
  
  # # if none of the previous scenarios produce a PeriodID, check if it falls in the first barrel period (for historical data uploads)
  # if (nrow(Periods)==0){
  #   query <- sprintf("SELECT * FROM chrl.barrel_periods WHERE (Starting_Date IS NULL) AND (Ending_Date >= '%s') AND (SiteID='%s')",Date,SiteID)
  #   Periods <- dbGetQuery(con, query)
  # }
  
  # Period_ID <- as.numeric(Periods$periodid[1])
  
  ##------------------------------------------------------------------------------------
  ##-------------------- Extracting data from the Calibration sheet---------------------
  ##------------------------------------------------------------------------------------
  #get the link to google drive sheet
  #Link <- drive_link(sprintf("AutoSalt_Hakai_Project/CF_Measurements/%s", New_Events[x,'name']))
  
  #on site or lab
  Location <- workbook[7,2]
  
  
  SensorInfo <- data.frame()
  for (Sen in c(1:4)){
    
    # get sensor info
    SenType <- workbook[(Sen+1),5]
    SerialNum <- workbook[(Sen+1),6]
    
    
    if (is.na(SerialNum) == TRUE & is.na(SenType)==FALSE){
      stop(sprintf("Sensor serial numbers were not entered for file %s",New_Events[x,'name']))
    }
      
    if (is.na(SenType)==TRUE & is.na(SerialNum)==TRUE){
      next()
    }
    if (stri_sub(SerialNum,-2,-1 )=='E9'){
      SerialNum <- as.character(as.numeric(SerialNum))
    }
    
    if (SenType=='Threcs'){
      SenType <- 'THRECS'
    }
    
    #location of event
    Loc <- workbook[(Sen+1),7]
    
    #notes about event
    Notes <- workbook[(Sen+1),8]
    
    #average temperature for CF event
    if (Sen==1){
      Temp <- mean(as.numeric(workbook[13:18,4]),na.rm=TRUE)
    } else if (Sen==2){
      Temp <- mean(as.numeric(workbook[22:27,4]),na.rm=TRUE)
    } else if (Sen==3){
      Temp <- mean(as.numeric(workbook[31:36,4]),na.rm=TRUE)
    } else if (Sen==4){
      Temp <- mean(as.numeric(workbook[40:45,4]),na.rm=TRUE)
    }
    
    #summarize the data
    S <- data.frame(ProbeNum=Sen, Type=SenType, SerialNum= SerialNum, StreamLoc= Loc, Notes=Notes, Temp= Temp, Num=Num)
    SensorInfo <- rbind(SensorInfo,S)
  }
  
  ##-----------------------------------------------------------------------------------------------------
  ##--------------- Extracting data from Uncertainty CF sheet-------------------------------------------
  ##----------------------------------------------------------------------------------------------------
  workbook <- read.xlsx(CF_File, sheet='Uncertainty CF',skipEmptyRows = F,skipEmptyCols = F)
  
  #extract CF and Error information 
  for (Sen in c(1:nrow(SensorInfo))){
    if (SensorInfo$ProbeNum[Sen]==1){
       Salt_conc= as.numeric(workbook[15:32,9])
       EC = as.numeric(workbook[15:32,8])
       reg = tidy(lm(Salt_conc ~ EC))
       CF = reg[2,2]*10^6
       Err = (reg[2,3]*2)/reg[2,2] *100
    } else if (SensorInfo$ProbeNum[Sen]==2){
      Salt_conc= as.numeric(workbook[43:60,9])
      EC = as.numeric(workbook[43:60,8])
      reg = tidy(lm(Salt_conc ~ EC))
      CF = reg[2,2]*10^6
      Err = (reg[2,3]*2)/reg[2,2] *100
      
    } else if (SensorInfo$ProbeNum[Sen]==3){
      Salt_conc= as.numeric(workbook[71:88,9])
      EC = as.numeric(workbook[71:88,8])
      reg = tidy(lm(Salt_conc ~ EC))
      CF = reg[2,2]*10^6
      Err = (reg[2,3]*2)/reg[2,2] *100
      
    } else if (SensorInfo$ProbeNum[Sen]==4){
      
      Salt_conc= as.numeric(workbook[99:116,9])
      EC = as.numeric(workbook[99:116,8])
      reg = tidy(lm(Salt_conc ~ EC))
      CF = reg[2,2]*10^6
      Err = (reg[2,3]*2)/reg[2,2] *100
    }
    
    
    # Determine if the CF value is too high or low to use
    SensorInfo[Sen,'CF'] <- CF
    SensorInfo[Sen,'Err'] <- Err
    if (CF <2.20){
      Flag <- 'L'
    } else if (CF > 2.9){
      Flag <- 'H'
    } else {
      Flag <- NA
    }
    SensorInfo[Sen,'Flag'] <- Flag
  }
  
  # Stores summary of each sensor and event
  C <- data.frame(SiteID= SiteID, Date=Date, PMP=PMP, Loc= Location, PeriodID= Period_ID, Num= Num, Link=Link)
  CF_Summary <- rbind(CF_Summary,C)  
  Sensor_Summary <- rbind(Sensor_Summary, SensorInfo)
  Num <- Num+1
#}

##----------------------------------------------------------------------------------
##------- Enter data into the calibration events table------------------------------
##----------------------------------------------------------------------------------
# Select data needed for upload to database
# CF_Summary_cut <- subset(CF_Summary,select=-c(Num,Link))

# # Determine how many CF trials happened during the CF event
# Uni <-  unique(CF_Summary_cut)
# for (x in c(1:nrow(Uni))){
#   R <- CF_Summary_cut[which(CF_Summary_cut$SiteID==Uni[x,'SiteID']& CF_Summary_cut$Date==Uni[x,'Date'] & CF_Summary_cut$PMP==Uni[x,'PMP']& CF_Summary_cut$Loc==Uni[x,'Loc'] & CF_Summary_cut$PeriodID==Uni[x,'PeriodID']),]
#   Uni[x,'Trials'] <- nrow(R)
# }


# # Insert the data into the database
# for (r in c(1:nrow(Uni))){
  
#   # Check if there is already data from this event in database
#   query= sprintf("SELECT * FROM chrl.calibration_events WHERE SiteID=%s AND Date='%s'AND PMP='%s' AND Location='%s' AND PeriodID=%s",
#     Uni[r,'SiteID'], Uni[r,'Date'],Uni[r,'PMP'],Uni[r,'Loc'],Uni[r,'PeriodID'])
#   H= dbGetQuery(con,query)
  
#   #if there are not already events for this site and date
#   if (nrow(H)==0){
#     print(sprintf("Adding New Event: WTS%s-%s-%s",Uni[r,"SiteID"],Uni[r,"PMP"],Uni[r,"Date"]))
#     query <- sprintf("INSERT INTO chrl.calibration_events (PeriodID, SiteID, Date, PMP, Trial, Location) VALUES (%s,%s,'%s','%s',%s,'%s')",
#                      Uni[r,'PeriodID'],
#                      Uni[r,"SiteID"],
#                      Uni[r,"Date"],
#                      Uni[r,"PMP"],
#                      Uni[r,"Trials"],
#                      Uni[r,"Loc"])
#     query <- gsub("\\n\\s+", " ", query)
#     dbSendQuery(con, query)
    
#     # summarize results
#     Num= CF_Summary[CF_Summary$PMP==Uni[r,"PMP"] & CF_Summary$PeriodID==Uni[r,'PeriodID'] &
#                       CF_Summary$SiteID==Uni[r,"SiteID"] & CF_Summary$Date==Uni[r,"Date"] & 
#                       CF_Summary$Loc==Uni[r,"Loc"],]
    
#     #specify the entries are new
#     for (N in Num$Num){
#       Sensor_Summary[Sensor_Summary$Num==N,'Addition']='New'
#     }
    
#     #case where a CF event already exists for the site and date
#   } else {
#     for (h in c(1:nrow(H))){
#       ID= H[h,"caleventid"]
      
#       #select the row that already exists in the database
#       query= sprintf("SELECT * FROM chrl.calibration_results WHERE CalEventID=%s", ID)
#       duplicate=dbGetQuery(con,query)
      
#       # how many trials have already been entered into database
#       Num_Trials_Before= max(duplicate$trial_number)
      
#       # select New CF events that match the data already in the database
#       Num= CF_Summary[CF_Summary$PMP==H[h,'pmp'] & CF_Summary$PeriodID==H[h,'periodid'] &
#                    CF_Summary$SiteID==H[h,'siteid'] & CF_Summary$Date==H[h,'date'] & 
#                    CF_Summary$Loc==H[h,'location'],'Num']
      
#       Trials= Uni[Uni$PMP==H[h,'pmp'] & Uni$PeriodID==H[h,'periodid'] &
#                         Uni$SiteID==H[h,'siteid'] & Uni$Date==H[h,'date'] & 
#                         Uni$Loc==H[h,'location'],'Trials']
      
#       # for all the new entries
#       for (N in Num){
#         Val= Sensor_Summary[Sensor_Summary$Num==N,]
        
#         #determine if the added values are duplicates or new
#         if (all(round(Val$CF,5) %in% duplicate$cf_value)== TRUE){
#           Sensor_Summary[Sensor_Summary$Num==N,'Addition']='Duplicate'
#         } else {
#           Sensor_Summary[Sensor_Summary$Num==N,'Addition']='Adding'
          
#           #if the data is new update the trial number in the database
#           query <- sprintf("UPDATE chrl.calibration_events SET Trial= %s WHERE  caleventid=%s ", (Num_Trials_Before+Trials), ID)
#           dbSendQuery(con,query)
#         }
#       }
      
#     }
#   }
# }
 
# # select all added CF values 
# Sensor_Summary=Sensor_Summary[which(Sensor_Summary$Addition!="Duplicate"),]


# ##----------------------------------------------------------------------------------------------
# ##------------------- Enter data into the calibration results table----------------------------
# ##---------------------------------------------------------------------------------------------

# # Pull the Calibration Event ID from the newly created records
# for (x in c(1:nrow(CF_Summary))){
#   query <- sprintf("SELECT CalEventID,Trial from chrl.calibration_events WHERE SiteID=%s AND Date='%s' AND PMP='%s' AND Location='%s'",
#                  CF_Summary[x,'SiteID'], CF_Summary[x,'Date'],CF_Summary[x,'PMP'],CF_Summary[x,'Loc'])
#   V <- dbGetQuery(con, query)
#   CF_Summary[x,'CalEventID'] <- V$caleventid
#   CF_Summary[x,'Trials']<- V$trial
# }
# CF_Summary$Addition=unique(Sensor_Summary$Addition)

# # Extract the sensor ID for each calibrated sensor
# for (R in c(1:nrow(Sensor_Summary))){
#   Number <- Sensor_Summary[R,'Num']
#   query <- sprintf("SELECT SensorID FROM chrl.sensors WHERE SiteID=%s AND Probe_Number=%s AND Sensor_Type='%s' AND Serial_Number='%s' and River_Loc= '%s'",
#                 CF_Summary[which(CF_Summary$Num==Number), 'SiteID'],
#                 Sensor_Summary[R,'ProbeNum'],
#                 Sensor_Summary[R,'Type'],
#                 Sensor_Summary[R,'SerialNum'],
#                 Sensor_Summary[R,'StreamLoc'])
#   SensorID <- dbGetQuery(con, query)
  
#   # check if a matching sensor exists. If not print an error statement
#   if (nrow(SensorID)==0){
#     SensorID <- NA
#     print(sprintf('ERROR: No matching sensor info in database for CF Event at site %s on %s',
#                   CF_Summary[which(CF_Summary$Num==Number), 'SiteID'],CF_Summary[which(CF_Summary$Num==Number), 'Date']))
#   } else if (nrow(SensorID)> 1){
#     SensorID <- NA
#   }
#   Sensor_Summary[R,'SensorID'] <- SensorID
# }

# # Update the googledriveid table that records which Google drive documents have been added
# for (R in c(1:nrow(Events_added))){
#   Number <- Events_added[R,'Num']
  
#   # if all sheets are duplicates of those already added
#   if (all(Sensor_Summary[Sensor_Summary$Num==Number, 'Addition']=='Duplicate')==TRUE){
#     next()
#   }
  
#   #insert google drive links
#   query <- sprintf("INSERT INTO chrl.googledriveid (file_name,driveid,date_added,caleventid) VALUES ('%s','%s','%s',%s)",
#             Events_added[R,'name'],
#             Events_added[R,'id_char'],
#             Events_added[R,'added'],
#             CF_Summary[which(CF_Summary$Num==Number),'CalEventID'])
#   query <- gsub("\\n\\s+", " ", query)
#   dbSendQuery(con, query)
# }

# # Assigning a trial number to each result
# Trial_assignment <- CF_Summary[,c('Num','CalEventID','Addition','Trials')]
# Ca=c(0)
# for (f in c(1:nrow(Trial_assignment))){
#   ID <- Trial_assignment[f,'CalEventID'] 
#   assign <- sum(Ca==ID)+1
  
#   if (Trial_assignment[f,"Addition"]=="Adding"){
#     query=sprintf("SELECT trial_number from chrl.calibration_results WHERE CalEventID=%s",ID)
#     trials_thus_far= unique(dbGetQuery(con,query)$trial_number)
#     Trial_assignment[f,'TrialNum']  <- max(trials_thus_far)+1
#   } else {
#     Trial_assignment[f,'TrialNum'] <- assign
#   }
#   Ca <- append(Ca,ID)
# }


# # Insert data into the calibration results table
# for (R in c(1:nrow(Sensor_Summary))){
#   Number <- Sensor_Summary[R,'Num']
  
#   #don't added data if it a duplicate
#   if (Sensor_Summary[R,'Addition']=='Duplicate'){
#     next()
#   }
  
#   # if no flags, enter flags as NA
#   if (is.null(Sensor_Summary[R,'Flags'])==TRUE){
#     Sensor_Summary[R,'Flags'] <- NA
#   }
  
#   # enter calibration results
#   query <- sprintf("Insert INTO chrl.calibration_results (CalEventID,SiteID,SensorID,trial_number,Temp,CF_value,Per_Err,Flags,Notes,Link)
#                  VALUES (%s,%s,%s,%s,%s,%s,%s,'%s','%s','%s')",
#                  CF_Summary[which(CF_Summary$Num==Number),'CalEventID'],
#                  CF_Summary[which(CF_Summary$Num==Number),'SiteID'],
#                  Sensor_Summary[R,'SensorID'],
#                  Trial_assignment[which(Trial_assignment$Num==Number),'TrialNum'],
#                  Sensor_Summary[R,'Temp'],
#                  Sensor_Summary[R,'CF'],
#                  Sensor_Summary[R,'Err'],
#                  Sensor_Summary[R,'Flags'],
#                  Sensor_Summary[R,'Notes'],
#                  sprintf("<a href=%s>%s</a>",CF_Summary[which(CF_Summary$Num==Number),'Link'], Events_added[which(Events_added$Num==Number),'name'])
#   )
#   # query=  query[1]
#   query <- gsub("\\n\\s+", " ", query)
#   query <- gsub('NA',"NULL", query)
#   query <- gsub("'NULL'","NULL",query)
#   dbSendQuery(con, query)
# }


# # Assign average temperature to calibration event table
# CalE= unique(CF_Summary$CalEventID)
# CalResults=data.frame()
# for (E in CalE){
  
#   #extract all temperatures
#   query= sprintf("SELECT Temp from chrl.calibration_results WHERE CalEventID=%s",E)
#   Tmps= dbGetQuery(con,query)
  
#   # average temps
#   T_mean= mean(Tmps$temp,na.rm=TRUE)
  
#   #update database
#   query=sprintf("UPDATE chrl.calibration_events SET Temp=%s WHERE CalEventID= %s",T_mean,E)
#   dbSendQuery(con,query)
  
#   #select all new calibration results 
#   query= sprintf("SELECT CalResultsID, SiteID,Temp FROM chrl.calibration_results WHERE CalEventID=%s",E)
#   CR=dbGetQuery(con,query)
  
#   CR$PeriodID=unique(CF_Summary[CF_Summary$CalEventID==E,'PeriodID'])
#   CR$Date=unique(CF_Summary[CF_Summary$CalEventID==E,'Date'])
  
#   CalResults= rbind(CalResults, CR)
# }

# ##-------------------------------------------------------------------
# ##-------Delete discharges that should include new CF values---------
# ##-------------------------------------------------------------------
# Unique_periods= unique(CalResults$PeriodID)

# # select autosalt events in the barrel period of CF measurements
# for (P in Unique_periods){
#   query= sprintf("SELECT EventID, SiteID, Date, Temp FROM chrl.autosalt_summary WHERE PeriodID=%s", P)
#   Events= dbGetQuery(con,query)
  
#   working= CalResults[CalResults$PeriodID==P,]
#   unique_date= unique(as.Date(working$Date))
  
#   # Select dump events that happened within in the previous 30 days of CF measurement
#   for (D in unique_date){
#     D=as.Date(D,origin="1970-01-01")
#     sub_events= Events[as.Date(Events$date)>(D-30),]
#     sub_working= working[working$Date==D,]
    
#     if (nrow(sub_events)==0){
#       next()
#     }
#     # check if dump event and CF measurement temps correspond
#     for (SD in c(1:nrow(sub_events))){
#       TP= sub_events[SD,'temp']
#       Apply= sub_working[sub_working$temp< (TP+5) & sub_working$temp>(TP-5),]
      
#       # delete dump events from database to be recalculated if new CF values
#       # should be incorporated in calculation
#       if (nrow(Apply)> 0){
#         print(sprintf("Dump event %s at %s need to be recalculated with new CF values and has temporarily been deleted",
#                       sub_events[SD,'eventid'], sub_events[SD,'siteid']))
#         query= sprintf("DELETE FROM chrl.autosalt_summary WHERE EventID=%s and SiteID=%s"
#                        ,sub_events[SD,'eventid'],sub_events[SD,'siteid'])
#         dbSendQuery(con,query)
#       }
#     }
#   }
# }

# dbDisconnect(con)
# options(warn = 0)


