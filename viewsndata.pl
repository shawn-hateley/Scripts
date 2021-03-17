#!/usr/bin/perl
################################################################################
# Copyright (c) 2016, Tula Foundation, and individual contributors.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the <organization> nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
################################################################################
# This script is used to view Hakai sensor network data in a quick-and-dirty
# way for specific measurements
#
# Created by: Ray Brunsting (ray@hakai.org)
# Created on: August 15, 2016
################################################################################

use CGI qw(:standard);
use Log::Log4perl;
use Log::Log4perl::Layout;
use Log::Log4perl::Level;
use HTML::Entities;
use DateTime;
use Time::HiRes qw( time );
use JSON;
use DBI;
use Encode;
use utf8;

################################################################################
# Force screen output to be UTF8. This was documented at
# http://stackoverflow.com/questions/627661/how-can-i-output-utf-8-from-perl
binmode( STDOUT, ":utf8" );

################################################################################
# Time how long it takes to process each request
my $startTime = time();

################################################################################
# Parse settings from the current environment and Hakai configuration file,
# if a configuration file has been specified and exists
for my $var ( sort keys %ENV ) {
    $myConfig{$var} = $ENV{$var};
}

my $myConfigFilename = $myConfig{SN_CONFIG_FILENAME};
if ( length $myConfigFilename && -f $myConfigFilename ) {

    open( my $configFH, '<:encoding(UTF-8)', $myConfigFilename )
      or die "Could not open file '$myConfigFilename' $!";

    while ( my $configLine = <$configFH> ) {
        chomp $configLine;

        next if index( $configLine, "#" ) == 0 || index( $configLine, "=" ) < 0;

        my ( $parameterName, $parameterValue ) = split /=/, $configLine;
        $parameterName =~ s/^\s+//;
        $parameterName =~ s/\s+$//;
        $parameterValue =~ s/^\s+//;
        $parameterValue =~ s/\s+$//;
        next unless length $parameterName && length $parameterValue;

        $myConfig{$parameterName} = $parameterValue;
    }
}

################################################################################
# Parse parameters passed in via the URL, using the CGI module
foreach my $parameterName ( param() ) {
    $myParameter{$parameterName} = param($parameterName);
}

my $hakaiHome   = $myConfig{hakaiHome};
my $logFilename = $myConfig{hakaiLogFilename};
if ( !length $logFilename && length $hakaiHome ) {
    my $logFolder = "$hakaiHome/logs";
    $logFilename = "$logFolder/hakai-sndata-www.log" if -d $logFolder;
}

################################################################################
# Initialize the logger
my $log       = Log::Log4perl->get_logger("");
my $logLayout = Log::Log4perl::Layout::PatternLayout->new("%d %m%n");
if ( defined $logFilename ) {
    my $logFileAppender = Log::Log4perl::Appender->new(
        "Log::Log4perl::Appender::File",
        name     => "filelog",
        filename => $logFilename
    );
    $logFileAppender->layout($logLayout);
    $log->add_appender($logFileAppender);
    $log->level($INFO);
}
else {
    my $screenLogAppender = Log::Log4perl::Appender->new( "Log::Log4perl::Appender::Screen", name => "screenlog" );
    $screenLogAppender->layout($logLayout);
    $log->add_appender($screenLogAppender);
    $log->level($ERROR);
}

my $logLevel = $myConfig{hakaiLogLevel};
if ( length $logLevel ) {
    $log->level($INFO)  if uc $logLevel eq "INFO";
    $log->level($DEBUG) if uc $logLevel eq "DEBUG";
    $log->level($TRACE) if uc $logLevel eq "TRACE";
}

################################################################################
# Print out the current configuration settings and script parameters
foreach my $parameterName ( sort keys %myConfig ) {
    my $parameterValue = $myConfig{$parameterName};
    $log->debug("$parameterName=\"$parameterValue\"");
}

foreach my $parameterName ( sort keys %myParameter ) {
    my $parameterValue = $myParameter{$parameterName};
    $log->debug("$parameterName=\"$parameterValue\"");
}

################################################################################
# Globals, contannts, defaults
my $oneMinuteTable  = "1minuteSamples";
my $fiveMinuteTable = "5minuteSamples";
my $oneHourTable    = "1hourSamples";
my $oneDayTable     = "1daySamples";

################################################################################
# Common header for generated html files
my $htmlHeader = "Content-type: text/html; charset=UTF-8\n\n";

# Header stuff, according to Bootstrap example
$htmlHeader .= "<!doctype html>\n";
$htmlHeader .= "<html lang=\"en\">\n";
$htmlHeader .= "<head>\n";
$htmlHeader .= "<meta name=\"robots\" content=\"noindex\">\n";

# Required meta tags, according to Bootstrap example
$htmlHeader .= "<meta charset=\"utf-8\">\n";
$htmlHeader .= "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1, shrink-to-fit=no\">\n";

# Bootstrap CSS
$htmlHeader .=
    "<link rel=\"stylesheet\" href=\"https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css\""
  . " integrity=\"sha384-Vkoo8x4CGsO3+Hhxv8T/Q5PaXtkKtu6ug5TOeNV6gBiFeWPGFN9MuhOf23Q9Ifjh\" crossorigin=\"anonymous\">\n";

$htmlHeader .= "<title>Hakai Sensor Network</title>\n";
$htmlHeader .= "<style type=\"text/css\">\n";
$htmlHeader .= ".myTable {font-size:small; background-color:#eee; border-collapse:collapse}\n";
$htmlHeader .= ".myTable thead, .myTable tfoot {background-color:#000066; color:white; vertical-align:bottom}\n";
$htmlHeader .= ".myTable tr:nth-child(even) {background: #FFF}\n";
$htmlHeader .=
"div.alert {background:#ffffcc; border:3px solid red; color:#000000; font-weight:bolder; margin:12px auto; padding:3px; width:500px; text-align:center;}\n";
$htmlHeader .= "</style>\n";
$htmlHeader .= "<script type=\"text/javascript\">\n";
$htmlHeader .= "var _gaq = _gaq || [];\n";
$htmlHeader .= "_gaq.push(['_setAccount', 'UA-46971905-1']);\n";
$htmlHeader .= "_gaq.push(['_trackPageview']);\n";
$htmlHeader .= "(function() {\n";
$htmlHeader .= "var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;\n";
$htmlHeader .=
  "ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';\n";
$htmlHeader .= "var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);\n";
$htmlHeader .= "})();\n";
$htmlHeader .= "</script>\n";

# Optional JavaScript, according to Bootstrap example
# jQuery first, then Popper.js, then Bootstrap JS
my $bsBody =
"<script src=\"https://code.jquery.com/jquery-3.4.1.slim.min.js\" integrity=\"sha384-J6qa4849blE2+poT4WnyKhv5vZF5SrPo0iEjwBvKU7imGFAV0wwj1yYfoRSJoZ+n\" crossorigin=\"anonymous\"></script>\n";
$bsBody .=
"<script src=\"https://cdn.jsdelivr.net/npm/popper.js\@1.16.0/dist/umd/popper.min.js\" integrity=\"sha384-Q6E9RHvbIyZFJoft+2mJbHaEWldlvI9IOYy5n3zV9zzTtmI3UksdQRVvoxMfooAo\" crossorigin=\"anonymous\"></script>\n";
$bsBody .=
"<script src=\"https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/js/bootstrap.min.js\" integrity=\"sha384-wfSDF2E50Y2D1uUdj0O3uMBJnjuUD4Ih7YwaYd1iqfktj0Uod8GCExl3Og8ifwB6\" crossorigin=\"anonymous\"></script>\n";

sub printNavigationHeader {
    my ($currentPageDescription) = @_;

    my @breadcrumbs =
      ( "<a href=\"https://www.hakai.org\">Hakai</a>", "<a href=\"/\">Data</a>", "<a href=\"/sn\">Sensor Network</a>" );
    push @breadcrumbs, $currentPageDescription if $currentPageDescription;

    print "<font size=\"-1\"><table width=\"100%\"><tr>";
    print "<td align=\"left\">" . join( " &gt; ", @breadcrumbs ) . "</td>";
    print "<td align=\"right\"><a href=\"http://hakai.org/\">Hakai</a>"
      . ", a <a href=\"http://tula.org/\">Tula Foundation</a> program</td>";
    print "</tr></table></font>\n";
}

sub errorOut {
    my ($errorString) = @_;

    $errorString = "[99] unknown internal error" unless length $errorString;
    $log->error($errorString);

    print $htmlHeader;

    print "</head>\n";
    print "<body>\n";

    print $bsBody;    # Add in Bootstrap stuff

    printNavigationHeader("<b>Error</b>");

    print "<hr>\n";

    print "<div id=\"errorMessage\" align=\"center\">\n";
    print "<b>ERROR:</b> $errorString";
    print "</div>\n";

    print "</body>\n";
    print "</html>\n";

    exit(1);
}

################################################################################
# Initialize a bunch of variables based on configuration settings or parameters
my $remoteHost = $myConfig{REMOTE_ADDR};
my $remoteUser = $myConfig{REMOTE_USER};
$remoteUser = "guest" unless defined $remoteUser;
my $requestURI = $myConfig{REQUEST_URI};
my $scriptName = $myConfig{SCRIPT_NAME};
my $homeURL    = $scriptName;
errorOut("Internal configuration error")
  unless length $remoteHost && length $remoteUser && length $requestURI;

################################################################################
# Filter parameters and defaults
my $dataTable              = $myParameter{dataTable};
my $visualizedMeasurements = $myParameter{measurements};
my $dateRange              = $myParameter{dateRange};
my $downloadFlag           = $myParameter{download};
my $originalFlag           = $myParameter{original};
my $firstMeasurementTime   = $myParameter{firstMeasurementTime};
my $lastMeasurementTime    = $myParameter{lastMeasurementTime};

undef $visualizedMeasurements if defined $visualizedMeasurements && length($visualizedMeasurements) == 0;

my $customizeFlag;
$customizeFlag = 1 if !defined $visualizedMeasurements || exists $myParameter{customize};

$dataTable = $oneHourTable unless defined $dataTable;

my $sampleInterval = 60;
$sampleInterval = 1    if $dataTable eq $oneMinuteTable;
$sampleInterval = 5    if $dataTable eq $fiveMinuteTable;
$sampleInterval = 1440 if $dataTable eq $oneDayTable;

# Support viewing both original/raw and derived/processed measurements
my $dbSchema = "sn";
$dbSchema = "sn_original"
  if defined $originalFlag
  || $dataTable eq "Diagnostics";    # Diagnostics data is only stored in raw form

my $dateRangeParameters;
if ( defined $dateRange ) {
    $dateRangeParameters .= "&dateRange=$dateRange";
}
elsif ( defined $firstMeasurementTime ) {
    $dateRangeParameters .= "&firstMeasurementTime=$firstMeasurementTime";
    $dateRangeParameters .= "&lastMeasurementTime=$lastMeasurementTime"
      if defined $lastMeasurementTime;
}

$firstMeasurementTime = decode_entities($firstMeasurementTime) if defined $firstMeasurementTime;
$lastMeasurementTime  = decode_entities($lastMeasurementTime)  if defined $lastMeasurementTime;

if ( defined $dateRange ) {
    if ( index( uc($dateRange), "LAST" ) == 0 ) {
        my $tempDateTime = DateTime->today();
        $tempDateTime->set_hour(0);
        $tempDateTime->set_minute(0);
        $tempDateTime->set_second(0);

        my $numDates = $dateRange;
        $numDates =~ s/[^0-9\.]//g;
        if ( index( uc $dateRange, "MONTH" ) > 0 ) {
            $tempDateTime->subtract( months => $numDates );
        }
        elsif ( index( uc $dateRange, "WEEK" ) > 0 ) {
            $tempDateTime->subtract( weeks => $numDates );
        }
        elsif ( index( uc $dateRange, "DAY" ) > 0 ) {
            $tempDateTime->subtract( days => $numDates );
        }

        $firstMeasurementTime = $tempDateTime->ymd . " " . $tempDateTime->hms;
    }
    elsif ( uc($dateRange) eq "YTD" ) {
        my $tempDateTime = DateTime->today();
        $tempDateTime->set_month(1);
        $tempDateTime->set_day(1);
        $tempDateTime->set_hour(0);
        $tempDateTime->set_minute(1);

        $firstMeasurementTime = $tempDateTime->ymd . " " . $tempDateTime->hms;
    }
    elsif ( uc($dateRange) eq "MTD" ) {
        my $tempDateTime = DateTime->today();
        $tempDateTime->set_day(1);
        $tempDateTime->set_hour(0);
        $tempDateTime->set_minute(1);

        $firstMeasurementTime = $tempDateTime->ymd . " " . $tempDateTime->hms;
    }
    elsif ( length($dateRange) == 4 && $dateRange =~ /2[0-9][0-9][0-9]/ ) {
        my $queryYear    = $dateRange;
        my $tempDateTime = DateTime->new(
            year      => $queryYear,
            month     => 1,
            day       => 1,
            hour      => 0,
            minute    => 1,
            time_zone => 'UTC'
        );

        $firstMeasurementTime = $tempDateTime->ymd . " " . $tempDateTime->hms;

        $tempDateTime->set_year( $queryYear + 1 );
        $tempDateTime->set_minute(0);

        $lastMeasurementTime = $tempDateTime->ymd . " " . $tempDateTime->hms;
    }
    elsif ( length($dateRange) == 7 && $dateRange =~ /2[0-9][0-9][0-9]-[0-9][0-9]/ ) {
        my ( $queryYear, $queryMonth ) = split( /-/, $dateRange );

        my $tempDateTime = DateTime->new(
            year      => $queryYear,
            month     => $queryMonth,
            day       => 1,
            hour      => 0,
            minute    => 1,
            time_zone => 'UTC'
        );

        $firstMeasurementTime = $tempDateTime->ymd . " " . $tempDateTime->hms;

        if ( $queryMonth < 12 ) {
            $tempDateTime->set_month( $queryMonth + 1 );
        }
        else {
            $tempDateTime->set_year( $queryYear + 1 );
            $tempDateTime->set_month(1);
        }
        $tempDateTime->set_minute(0);

        $lastMeasurementTime = $tempDateTime->ymd . " " . $tempDateTime->hms;
    }
    elsif ( length($dateRange) == 10 && $dateRange =~ /2[0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/ ) {
        my ( $queryYear, $queryMonth, $queryDay ) = split( /-/, $dateRange );

        my $tempDateTime = DateTime->new(
            year      => $queryYear,
            month     => $queryMonth,
            day       => $queryDay,
            hour      => 0,
            minute    => 1,
            time_zone => 'UTC'
        );

        $firstMeasurementTime = $tempDateTime->ymd . " " . $tempDateTime->hms;

        $tempDateTime->add( days => 2 );
        $tempDateTime->set_minute(0);

        $lastMeasurementTime = $tempDateTime->ymd . " " . $tempDateTime->hms;
    }
}

################################################################################
# Utility function to get the base measurement/display name
sub getBaseName {
    my ($baseName) = @_;

    $baseName =~ s/_Med$//;
    $baseName =~ s/_Avg$//;
    $baseName =~ s/_Min$//;
    $baseName =~ s/_Max$//;
    $baseName =~ s/_Std$//;
    $baseName =~ s/_QL$//;
    $baseName =~ s/_UQL$//;
    $baseName =~ s/_QC$//;

    $baseName =~ s/_med$//;
    $baseName =~ s/_avg$//;
    $baseName =~ s/_min$//;
    $baseName =~ s/_max$//;
    $baseName =~ s/_std$//;
    $baseName =~ s/_ql$//;
    $baseName =~ s/_uql$//;
    $baseName =~ s/_qc$//;

    return $baseName;
}

################################################################################
# Utility functions
%previousDateCache;

sub getPreviousDate {
    my $baseDate = $_[0];

    # Cache the results, since calls to DateTime are relatively expensive
    $previousDateCache{$baseDate} = DateTime->new(
        year  => substr( $baseDate, 0, 4 ),
        month => substr( $baseDate, 5, 2 ),
        day   => substr( $baseDate, 8, 2 ),
        hour  => 12
    )->subtract( days => 1 )->ymd('-')
      unless exists $previousDateCache{$baseDate};

    return $previousDateCache{$baseDate};
}

################################################################################
# Connect to the database
my $pgHost     = $myConfig{hakaiDBHost};
my $pgName     = $myConfig{hakaiDBName};
my $pgUser     = $myConfig{hakaiDBUser};
my $pgPassword = $myConfig{hakaiDBPassword};
my $dbh;
{
    # Allow user to switch to a different PostgreSQL database via URL parameter
    if ( defined $myParameter{pgName} ) {
        $pgName = $myParameter{pgName};
    }
    elsif ( exists $myParameter{dev} || exists $myParameter{test} ) {
        $pgName = "hakaidev";
    }

    # Assume some defaults
    $pgHost = "db.hakai.org" unless length $pgHost;
    $pgName = "hakai"        unless length $pgName;

    errorOut("Missing database credentials") unless length $pgUser && length $pgPassword;

    $dbh = DBI->connect( "DBI:Pg:dbname=$pgName;host=$pgHost", $pgUser, $pgPassword );

    errorOut("Failed to connect to database") unless $dbh;
}

################################################################################
# Log/audit request to the log (info level) and to the database
sub auditDataAccess {
    my $elapsedTime = sprintf( "%.2fs", time() - $startTime );

    $log->info("[$remoteUser\@$remoteHost] Processed request $requestURI in $elapsedTime");

    my $now = DateTime->now( time_zone => 'UTC' );

    my $dtAccessed = $now;

    my %auditMetadata;
    $auditMetadata{remoteUser}  = $remoteUser;
    $auditMetadata{remoteHost}  = $remoteHost;
    $auditMetadata{requestURI}  = $requestURI;
    $auditMetadata{elapsedTime} = $elapsedTime;

    my $jsonMetadata = to_json( \%auditMetadata );

    my $auditDbh = $dbh;
    $auditDbh = DBI->connect( "DBI:Pg:dbname=hakai;host=$pgHost", $pgUser, $pgPassword ) if $pgName ne "hakai";

    $auditDbh->do( "INSERT INTO sn.audit_log (remote_host, remote_user, dt_accessed, url, metadata) VALUES (?,?,?,?,?)",
        undef, $remoteHost, $remoteUser, $dtAccessed, $requestURI, $jsonMetadata );

    $auditDbh->disconnect() if $pgName ne "hakai";
}

################################################################################
# Convert a date string to JavaScript Date data type
sub getDataTableDateTimeType {
    my ($currentDateTime) = @_;

    my $year   = substr( $currentDateTime, 0,  4 );
    my $month  = substr( $currentDateTime, 5,  2 ) - 1;
    my $day    = substr( $currentDateTime, 8,  2 );
    my $hour   = substr( $currentDateTime, 11, 2 );
    my $minute = substr( $currentDateTime, 14, 2 );

    # Convert to UTC manually, for performance reasons
    $hour += 8;
    if ( $hour > 23 ) {
        $day++;
        $hour -= 24;
        if (   $day > 31
            || ( $day > 30 && ( $month == 3 || $month == 5 || $month == 8 || $month == 10 ) )
            || ( $month == 1 && $day > 29 )
            || ( $month == 1 && $day > 28 && ( $year % 4 ) > 0 ) )
        {
            $month++;
            $day = 1;
            if ( $month == 12 ) {
                $year++;
                $month = 0;
            }
        }
    }

    return "Date(Date.UTC($year,$month,$day,$hour,$minute))";
}

sub getDataTableDateType {
    my ($currentDateTime) = @_;

    my $year  = substr( $currentDateTime, 0, 4 );
    my $month = substr( $currentDateTime, 5, 2 ) - 1;
    my $day   = substr( $currentDateTime, 8, 2 );

    return "Date(Date.UTC($year,$month,$day))";
}

################################################################################
# Build URL based on various input parameters
sub getBaseURL {
    my ( $dataTable, $visualizedMeasurements, $dateRangeParameters ) = @_;

    my $baseURL = "$homeURL?dataTable=$dataTable";
    $baseURL .= "&measurements=" . encode_entities($visualizedMeasurements) if defined $visualizedMeasurements;
    $baseURL .= $dateRangeParameters                                        if defined $dateRangeParameters;
    $baseURL .= "&pgName=" . $myParameter{pgName}                           if exists $myParameter{pgName};
    $baseURL .= "&dev"                                                      if exists $myParameter{dev};
    $baseURL .= "&test"                                                     if exists $myParameter{test};
    $baseURL .= "&noFlags"                                                  if exists $myParameter{noFlags};
    $baseURL .= "&original"                                                 if defined $originalFlag;

    return $baseURL;
}

################################################################################
# Contruct list of links to switch to other measurement intervals
my @otherSampleIntervalLinks;
push @otherSampleIntervalLinks,
  "<a href=\"" . getBaseURL( $oneDayTable, $visualizedMeasurements, $dateRangeParameters ) . "\">daily</a>"
  if ( $dataTable eq $oneHourTable || $dataTable eq $fiveMinuteTable ) && !defined $originalFlag;
push @otherSampleIntervalLinks, "daily" if $dataTable eq $oneDayTable;

push @otherSampleIntervalLinks,
  "<a href=\"" . getBaseURL( $oneHourTable, $visualizedMeasurements, $dateRangeParameters ) . "\">hourly</a>"
  if $dataTable eq $oneDayTable || $dataTable eq $fiveMinuteTable;
push @otherSampleIntervalLinks, "hourly" if $dataTable eq $oneHourTable;

push @otherSampleIntervalLinks,
  "<a href=\"" . getBaseURL( $fiveMinuteTable, $visualizedMeasurements, $dateRangeParameters ) . "\">5 minute</a>"
  if $dataTable eq $oneDayTable || $dataTable eq $oneHourTable;
push @otherSampleIntervalLinks, "5 minute" if $dataTable eq $fiveMinuteTable;

################################################################################
# Construct a normalized URL that matches the current content
my $currentURL = getBaseURL( $dataTable, $visualizedMeasurements, $dateRangeParameters );

################################################################################
# Parse the list of viewed measurements
my %allMeasurements;
my %currentMeasurements;
my %baseMeasurements;
my @viewedMeasurementList;
my %viewedMeasurements;
my %viewedFullMeasurementNames;
my %viewedSensorNodes;
my %viewedWatersheds;
my %viewedDisplayNames;

sub addMeasurement {
    my (
        $sensorNode,             $measurementName,     $displayName,
        $measurementUnits,       $sensorType,          $serialNumber,
        $sensorDescription,      $sensorDocumentation, $sensorComments,
        $measurementCalculation, $databaseTable,       $databaseColumn,
        $firstDatabaseMeasurementTime
    ) = @_;

    #$displayName      = decode( 'utf8', $displayName )      if defined $displayName;
    #$measurementUnits = decode( 'utf8', $measurementUnits ) if defined $measurementUnits;

    my $fullMeasurementName = "$sensorNode.$measurementName";
    my $baseMeasurementName = getBaseName($fullMeasurementName);

    my $measurementGroupName = $baseMeasurementName;
    $measurementGroupName =~ s/[0-9][0-9]hour//;

    $displayName = $measurementName unless defined $displayName && length $displayName;
    $displayName =~ s/_/ /g;

    my $measurementKey = lc $baseMeasurementName . "-" . $fullMeasurementName;
    $measurementKey =~ s/_med$/01/g;
    $measurementKey =~ s/_avg$/02/g;
    $measurementKey =~ s/_min$/03/g;
    $measurementKey =~ s/_max$/04/g;
    $measurementKey =~ s/_std$/05/g;

    my $commonMeasurementKey = lc $displayName . "-" . $measurementKey;

    push @{ $viewedMeasurements{$measurementName}{sensorNodes} }, $sensorNode
      if exists $viewedMeasurements{$measurementName};

    # Include all measurements associated with the same sensor node or watershed as
    # one or more of the measurements being viewed
    if ( !$customizeFlag && !exists $viewedSensorNodes{$sensorNode} ) {
        my $viewedWatershed;
        foreach my $viewedWatershedID ( keys %viewedWatersheds ) {
            $viewedWatershed = 1 if index( $sensorNode, $viewedWatershedID ) >= 0;
        }
        return unless defined $viewedWatershed;
    }

    $viewedDisplayNames{$displayName}{sensorNodes}{$sensorNode} = $sensorNode;

    $viewedMeasurements{$measurementName}{displayName} = $displayName
      if defined $viewedMeasurements{$measurementName}
      && !defined $viewedMeasurements{$measurementName}{displayName};

    $allMeasurements{$measurementKey}{sensorNode}             = $sensorNode;
    $allMeasurements{$measurementKey}{measurementName}        = $measurementName;
    $allMeasurements{$measurementKey}{displayName}            = $displayName;
    $allMeasurements{$measurementKey}{measurementUnits}       = $measurementUnits;
    $allMeasurements{$measurementKey}{sensorType}             = $sensorType;
    $allMeasurements{$measurementKey}{serialNumber}           = $serialNumber;
    $allMeasurements{$measurementKey}{sensorDescription}      = $sensorDescription;
    $allMeasurements{$measurementKey}{sensorDocumentation}    = $sensorDocumentation;
    $allMeasurements{$measurementKey}{sensorComments}         = $sensorComments;
    $allMeasurements{$measurementKey}{measurementCalculation} = $measurementCalculation;
    $allMeasurements{$measurementKey}{firstMeasurementTime}   = $firstDatabaseMeasurementTime;
    $allMeasurements{$measurementKey}{fullMeasurementName}    = $fullMeasurementName;
    $allMeasurements{$measurementKey}{baseMeasurementName}    = $baseMeasurementName;
    $allMeasurements{$measurementKey}{measurementGroupName}   = $measurementGroupName;
    $allMeasurements{$measurementKey}{databaseTable}          = $databaseTable;
    $allMeasurements{$measurementKey}{databaseColumn}         = $databaseColumn;

    return unless $customizeFlag || exists $baseMeasurements{$baseMeasurementName};

    $currentMeasurements{$measurementKey}{sensorNode}             = $sensorNode;
    $currentMeasurements{$measurementKey}{measurementName}        = $measurementName;
    $currentMeasurements{$measurementKey}{displayName}            = $displayName;
    $currentMeasurements{$measurementKey}{measurementUnits}       = $measurementUnits;
    $currentMeasurements{$measurementKey}{sensorType}             = $sensorType;
    $currentMeasurements{$measurementKey}{serialNumber}           = $serialNumber;
    $currentMeasurements{$measurementKey}{sensorDescription}      = $sensorDescription;
    $currentMeasurements{$measurementKey}{sensorDocumentation}    = $sensorDocumentation;
    $currentMeasurements{$measurementKey}{sensorComments}         = $sensorComments;
    $currentMeasurements{$measurementKey}{measurementCalculation} = $measurementCalculation;
    $currentMeasurements{$measurementKey}{firstMeasurementTime}   = $firstDatabaseMeasurementTime;
    $currentMeasurements{$measurementKey}{fullMeasurementName}    = $fullMeasurementName;
    $currentMeasurements{$measurementKey}{baseMeasurementName}    = $baseMeasurementName;
    $currentMeasurements{$measurementKey}{measurementGroupName}   = $measurementGroupName;
    $currentMeasurements{$measurementKey}{databaseTable}          = "$dbSchema.$databaseTable";

    $currentMeasurements{$measurementKey}{databaseColumn} = $databaseColumn;

    if ( index( $measurementName, "_QL" ) > 0 ) {
        $currentMeasurements{$measurementKey}{qlField} = 1;

        $baseMeasurements{$baseMeasurementName}{qlMeasurementKey} = $measurementKey;
    }
    elsif ( index( $measurementName, "_UQL" ) > 0 ) {
        $currentMeasurements{$measurementKey}{uqlField} = 1;

        $baseMeasurements{$baseMeasurementName}{uqlMeasurementKey} = $measurementKey;
    }
    elsif ( index( $measurementName, "_QC" ) > 0 ) {
        $currentMeasurements{$measurementKey}{qcFlag} = 1;

        $baseMeasurements{$baseMeasurementName}{qcMeasurementKey} = $measurementKey;
    }
    else {
        $baseMeasurements{$baseMeasurementName}{measurementsKeys}{$measurementKey} = 1;
    }
}

{
    my $viewedMeasurementIndex = 0;

    foreach my $visualizedMeasurement ( split( /,/, $visualizedMeasurements ) ) {
        my ( $fullMeasurementName, $viewOptions ) = split( /\?/, $visualizedMeasurement );

        # Allow data table name to be implicitly determined to match current sample interval
        my ( $sensorNode, $measurementName ) = split( /\./, $fullMeasurementName );
        next unless defined $sensorNode && defined $measurementName;

        my $baseMeasurementName = getBaseName($fullMeasurementName);

        my $measurementGroupName = $baseMeasurementName;
        $measurementGroupName =~ s/[0-9][0-9]hour//;

        my $measurementKey = lc $baseMeasurementName . "-" . $fullMeasurementName;
        $measurementKey =~ s/_med$/01/g;
        $measurementKey =~ s/_avg$/02/g;
        $measurementKey =~ s/_min$/03/g;
        $measurementKey =~ s/_max$/04/g;
        $measurementKey =~ s/_std$/05/g;

        $viewedMeasurementList[$viewedMeasurementIndex]{measurementKey}        = $measurementKey;
        $viewedMeasurementList[$viewedMeasurementIndex]{visualizedMeasurement} = $visualizedMeasurement;
        $viewedMeasurementList[$viewedMeasurementIndex]{fullMeasurementName}   = $fullMeasurementName;
        $viewedMeasurementList[$viewedMeasurementIndex]{viewOptions}           = $viewOptions;
        $viewedMeasurementIndex++;

        $currentMeasurements{$measurementKey}{sensorNode}           = $sensorNode;
        $currentMeasurements{$measurementKey}{measurementName}      = $measurementName;
        $currentMeasurements{$measurementKey}{fullMeasurementName}  = $fullMeasurementName;
        $currentMeasurements{$measurementKey}{baseMeasurementName}  = $baseMeasurementName;
        $currentMeasurements{$measurementKey}{measurementGroupName} = $measurementGroupName;
        $currentMeasurements{$measurementKey}{view}                 = 1;

        $baseMeasurements{$baseMeasurementName}{baseMeasurementName} = $baseMeasurementName;

        $viewedSensorNodes{$sensorNode}{numMeasurements}++;

        $viewedMeasurements{$measurementName}{measurementName}             = $measurementName;
        $viewedFullMeasurementNames{$fullMeasurementName}{sensorNode}      = $sensorNode;
        $viewedFullMeasurementNames{$fullMeasurementName}{measurementName} = $measurementName;
    }

    # Read in the current list of measurements from the database
    my $sql =
        "SELECT sensor_node, measurement_name, display_name, measurement_units"
      . ", sensor_type, serial_number, sensor_description, sensor_documentation, sensor_comments"
      . ", measurement_calculation, database_table, database_column, first_measurement_time at time zone 'PST'"
      . " FROM sn.measurements sm, information_schema.columns ic"
      . " WHERE sm.data_table=?"
      . " AND sm.database_table = ic.table_name"
      . " AND sm.database_column = ic.column_name"
      . " AND ic.table_schema=?";

    my $measurementsSth = $dbh->prepare($sql);

    $log->debug("[$pgName:$dbSchema] SQL: $sql");

    $measurementsSth->execute( $dataTable, $dbSchema );

    my (
        $sensorNode,             $measurementName,     $displayName,
        $measurementUnits,       $sensorType,          $serialNumber,
        $sensorDescription,      $sensorDocumentation, $sensorComments,
        $measurementCalculation, $databaseTable,       $databaseColumn,
        $firstDatabaseMeasurementTime
    );

    $measurementsSth->bind_columns(
        \$sensorNode,             \$measurementName,     \$displayName,
        \$measurementUnits,       \$sensorType,          \$serialNumber,
        \$sensorDescription,      \$sensorDocumentation, \$sensorComments,
        \$measurementCalculation, \$databaseTable,       \$databaseColumn,
        \$firstDatabaseMeasurementTime
    );

    foreach my $viewedSensorNode ( keys %viewedSensorNodes ) {
        foreach my $watershedID ( '1015', '626', '693', '703', '708', '819', '1015', '844' ) {
            $viewedWatersheds{$watershedID} = 1 if index( $viewedSensorNode, $watershedID ) >= 0;
        }
    }

    while ( $measurementsSth->fetch ) {

        addMeasurement(
            $sensorNode,             $measurementName,     $displayName,
            $measurementUnits,       $sensorType,          $serialNumber,
            $sensorDescription,      $sensorDocumentation, $sensorComments,
            $measurementCalculation, $databaseTable,       $databaseColumn,
            $firstDatabaseMeasurementTime
        );

        # Support aggregated measurements
        next
          unless $measurementName eq "Rain"
          || $measurementName eq "DischargeVolume"
          || ( $dataTable eq $oneDayTable && $measurementName eq "24hourRain" );

        $measurementName = "Rain" if $measurementName eq "24hourRain";

        # Add monthly, yearly and total aggregated rainfall measurements
        foreach my $aggType ( 'Agg', 'Mtd', 'Ytd' ) {

            my $aggMeasurementName = $measurementName . $aggType;
            my $aggDisplayName     = $displayName . $aggType;

            addMeasurement(
                $sensorNode,             $aggMeasurementName,  $aggDisplayName,
                $measurementUnits,       $sensorType,          $serialNumber,
                $sensorDescription,      $sensorDocumentation, $sensorComments,
                $measurementCalculation, $databaseTable,       $databaseColumn,
                $firstDatabaseMeasurementTime
            );

            next unless $measurementName eq "DischargeVolume";

            my $minAggMeasurementName = $measurementName . $aggType . "_Min";
            my $minAggDisplayName     = $displayName . $aggType . " Min (95% CI)";
            my $minAggDatabaseColumn  = $databaseColumn . "_min";

            addMeasurement(
                $sensorNode,             $minAggMeasurementName, $minAggDisplayName,
                $measurementUnits,       $sensorType,            $serialNumber,
                $sensorDescription,      $sensorDocumentation,   $sensorComments,
                $measurementCalculation, $databaseTable,         $minAggDatabaseColumn,
                $firstDatabaseMeasurementTime
            );

            my $maxAggMeasurementName = $measurementName . $aggType . "_Max";
            my $maxAggDisplayName     = $displayName . $aggType . " Max (95% CI)";
            my $maxAggDatabaseColumn  = $databaseColumn . "_max";

            addMeasurement(
                $sensorNode,             $maxAggMeasurementName, $maxAggDisplayName,
                $measurementUnits,       $sensorType,            $serialNumber,
                $sensorDescription,      $sensorDocumentation,   $sensorComments,
                $measurementCalculation, $databaseTable,         $maxAggDatabaseColumn,
                $firstDatabaseMeasurementTime
            );
        }
    }

    foreach my $measurementKey ( keys %allMeasurements ) {
        my $sensorNode  = $allMeasurements{$measurementKey}{sensorNode};
        my $displayName = $allMeasurements{$measurementKey}{displayName};

        my $columnName = $displayName;
        $columnName =~ s/_/ /g;
        $columnName .= " ($sensorNode)" if scalar( keys %{ $viewedDisplayNames{$displayName}{sensorNodes} } ) > 1;

        $allMeasurements{$measurementKey}{columnName} = $columnName;
        $currentMeasurements{$measurementKey}{columnName} = $columnName if exists $currentMeasurements{$measurementKey};
    }
}

if ( scalar( keys %viewedSensorNodes ) > 1 ) {
    my $sql = "SELECT sensor_node, dt_lastseen FROM sn.sensor_node_status";
    $sql .= " WHERE sensor_node in ('" . join( "','", keys %viewedSensorNodes ) . "')";

    $log->debug("[$pgName:$dbSchema] SQL: $sql");

    my $sensorStatusSth = $dbh->prepare($sql);
    $sensorStatusSth->execute();

    my ( $sensorNode, $dtLastseen );

    $sensorStatusSth->bind_columns( \$sensorNode, \$dtLastseen );

    while ( $sensorStatusSth->fetch ) {
        $viewedSensorNodes{$sensorNode}{dtLastseen} = $dtLastseen;
    }
}

################################################################################
# Support graph customization if no measurements have been provided or if
# the 'customize' command line option is included
if ($customizeFlag) {
    print $htmlHeader;

    print "<script type=\"text/javascript\" src=\"https://www.gstatic.com/charts/loader.js\"></script>\n";
    print "<script type=\"text/javascript\">\n";
    print "google.charts.load('visualization', 'current', {'packages':['controls']});\n";
    print "google.charts.setOnLoadCallback(drawMeasurementList);\n";
    print "function drawMeasurementList() {\n";

    print "var myMeasurements = new google.visualization.DataTable();\n";

    print "myMeasurements.addColumn('string', 'Viewed sensor node')\n";
    print "myMeasurements.addColumn('string', 'Sensor node')\n";
    print "myMeasurements.addColumn('string', 'Measurement')\n";
    print "myMeasurements.addColumn('string', 'Measurement type')\n";
    print "myMeasurements.addColumn('string', 'Units')\n";
    print "myMeasurements.addColumn('string', 'Smp')\n";
    print "myMeasurements.addColumn('string', 'Med')\n";
    print "myMeasurements.addColumn('string', 'Avg')\n";
    print "myMeasurements.addColumn('string', 'Min')\n";
    print "myMeasurements.addColumn('string', 'Max')\n";
    print "myMeasurements.addColumn('string', 'Std')\n";

    # Get a list of cases for all cases
    my @dataRows;

    my $currentToggleNum = 1;
    foreach my $baseMeasurementName ( sort keys %baseMeasurements ) {

        my $lastSensorNode;
        my $baseDisplayName;
        my $measurementType;
        my $lastMeasurementUnits;
        my %measurementLinks;
        my $numViewedMeasurement = 0;

        foreach my $measurementKey ( sort keys %{ $baseMeasurements{$baseMeasurementName}{measurementsKeys} } ) {

            my $sensorNode      = $allMeasurements{$measurementKey}{sensorNode};
            my $measurementName = $allMeasurements{$measurementKey}{measurementName};

            my $currentDateRangeParameters = $dateRangeParameters;
            undef $currentDateRangeParameters if index( $sensorNode, "SA_" ) == 0;

            next
              unless exists $allMeasurements{$measurementKey}{databaseTable}
              && exists $allMeasurements{$measurementKey}{databaseColumn};

            next if $measurementName eq "RECORD";

            my $displayName = $allMeasurements{$measurementKey}{displayName};

            if ( !defined $measurementType ) {
                if (   $measurementName eq "RainAgg"
                    || $measurementName eq "RainMtd"
                    || $measurementName eq "RainYtd"
                    || $measurementName eq "DischargeVolumeAgg"
                    || $measurementName eq "DischargeVolumeMtd"
                    || $measurementName eq "DischargeVolumeYtd"
                    || $measurementName eq "DischargeVolumeAgg_Min"
                    || $measurementName eq "DischargeVolumeMtd_Min"
                    || $measurementName eq "DischargeVolumeYtd_Min"
                    || $measurementName eq "DischargeVolumeAgg_Max"
                    || $measurementName eq "DischargeVolumeMtd_Max"
                    || $measurementName eq "DischargeVolumeYtd_Max"
                    || $measurementName eq "24hourRain"
                    || $measurementName =~ /AirTemp.*Avg$/
                    || $measurementName eq "WindSpd_Avg"
                    || $measurementName eq "RH_Avg"
                    || $measurementName eq "PLS_Lvl_Avg"
                    || $measurementName eq "PLS2_Lvl_Avg"
                    || ( $sensorNode =~ /SA_/ && $measurementName =~ /^WaterTemp/ ) )
                {
                    $measurementType = $measurementName;
                    $measurementType =~ s/_Avg//;
                    $measurementType =~ s/_Med//;

                    $measurementType = "Air temperature"            if $measurementName =~ /AirTemp/;
                    $measurementType = "Wind speed"                 if $measurementType =~ /WindSpd/;
                    $measurementType = "Aggregated preciptation"    if $measurementType =~ /RainAgg/;
                    $measurementType = "Month-to-date preciptation" if $measurementType =~ /RainMtd/;
                    $measurementType = "Year-to-date preciptation"  if $measurementType =~ /RainYtd/;
                    $measurementType = "Aggregated discharge"       if $measurementType =~ /DischargeVolumeAgg/;
                    $measurementType = "Month-to-date discharge"    if $measurementType =~ /DischargeVolumeMtd/;
                    $measurementType = "Year-to-date discharge"     if $measurementType =~ /DischargeVolumeYtd/;
                    $measurementType = "24 hour preciptation"       if $measurementType =~ /24hourRain/;
                    $measurementType = "Relative humidity"          if $measurementType =~ /RH/;
                    $measurementType = "Stream stage"               if $measurementType =~ /PLS/;
                    $measurementType = "Standalone water temperature"
                      if ( $sensorNode =~ /SA_/ && $measurementName =~ /^WaterTemp/ );
                }
                elsif ( $measurementName =~ /_wtd$/ ) {
                    $measurementType = "Water table depth";
                }
            }

            if ( !defined $baseDisplayName ) {
                $baseDisplayName = $displayName;
                $baseDisplayName =~ s/ Med$//;
                $baseDisplayName =~ s/ Avg$//;
                $baseDisplayName =~ s/ Min$//;
                $baseDisplayName =~ s/ Max$//;
                $baseDisplayName =~ s/ Std$//;
            }

            my $measurementType = $displayName;
            if ( $baseDisplayName eq $measurementType ) {
                $measurementType = "Smp";
            }
            elsif ( index( $measurementType, $baseDisplayName ) == 0 ) {
                $measurementType = substr( $measurementType, length($baseDisplayName) );
                $measurementType =~ s/^[ ]+//;
            }

            $lastSensorNode = $allMeasurements{$measurementKey}{sensorNode} unless defined $lastSensorNode;
            $lastMeasurementUnits = $allMeasurements{$measurementKey}{measurementUnits}
              unless defined $lastMeasurementUnits;

            my $fullMeasurementName =
              $allMeasurements{$measurementKey}{sensorNode} . "." . $allMeasurements{$measurementKey}{measurementName};

            # Function to toggle these from being viewed
            my $toggleCheckboxId = "tog" . $currentToggleNum++;

            my $measurementLink;
            if ( !exists $viewedFullMeasurementNames{$fullMeasurementName} ) {
                $measurementLink =
"<input type=\"checkbox\" id=\"$toggleCheckboxId\" onclick=\"updateSelected(this)\" value=\"$fullMeasurementName\">";
            }
            else {
                $measurementLink =
"<input type=\"checkbox\" id=\"$toggleCheckboxId\" checked=\"checked\" onclick=\"updateSelected(this)\" value=\"$fullMeasurementName\">";

                $numViewedMeasurement++;
            }

            $measurementLink .=
              " <a href=\"" . getBaseURL( $dataTable, $fullMeasurementName, $currentDateRangeParameters );
            $measurementLink .= "\">$measurementType</a>";

            $measurementLinks{ lc($measurementType) } = $measurementLink;
        }

        next unless %measurementLinks;

        if ($numViewedMeasurement) {
            $lastSensorNode       = "{v:'$lastSensorNode',f:'<b>$lastSensorNode</b>'}";
            $baseDisplayName      = "{v:'$baseDisplayName',f:'<b>$baseDisplayName</b>'}";
            $lastMeasurementUnits = "{v:'$lastMeasurementUnits',f:'<b>$lastMeasurementUnits</b>'}"
              if defined $lastMeasurementUnits;
        }
        else {
            $lastSensorNode       = "'$lastSensorNode'";
            $baseDisplayName      = "'$baseDisplayName'";
            $lastMeasurementUnits = "'$lastMeasurementUnits'" if defined $lastMeasurementUnits;
        }

        $lastMeasurementUnits = "null" unless defined $lastMeasurementUnits;

        if ( !defined $measurementType ) {
            $measurementType = "null";
        }
        else {
            $measurementType = "'$measurementType'";
        }

        my $viewedSensorNode = $lastSensorNode;
        $viewedSensorNode = "'Standalone'" if index( $viewedSensorNode, "SA_" ) >= 0;

        my @currentMeasurements;
        foreach my $measurementType ( 'smp', 'med', 'avg', 'min', 'max', 'std' ) {
            if ( exists $measurementLinks{$measurementType} ) {
                push @currentMeasurements, "'" . $measurementLinks{$measurementType} . "'";
            }
            else {
                push @currentMeasurements, "null";
            }
        }

        push @dataRows,
          "[$viewedSensorNode,$lastSensorNode,$baseDisplayName,$measurementType,$lastMeasurementUnits,"
          . join( ",", @currentMeasurements ) . "]";
    }

    print "myMeasurements.addRows([";
    print join( ",\n", @dataRows );
    print "]);\n";

    print
"var measurementsDashboard = new google.visualization.Dashboard(document.getElementById('measurementsDashboardDiv'));\n";

    print "var nodePicker = new google.visualization.ControlWrapper({\n";
    print "'controlType': 'CategoryFilter',\n";
    print "'containerId': 'nodePickerDiv',\n";
    print "'options': {'filterColumnIndex': 0,\n";
    print "'ui': {\n";
    print "'labelStacking': 'horizontal'";
    print ",'allowTyping': false, 'allowNone': true, 'allowMultiple': true";
    print ",'sortValues': true";
    print ",'caption': 'All'}";
    print "}\n";
    print "});\n";

    if (%viewedSensorNodes) {
        print "nodePicker.setState({'selectedValues': ['" . join( "','", sort keys %viewedSensorNodes ) . "']})\n";
    }

    print "var typePicker = new google.visualization.ControlWrapper({\n";
    print "'controlType': 'CategoryFilter',\n";
    print "'containerId': 'typePickerDiv',\n";
    print "'options': {'filterColumnIndex': 3,\n";
    print "'ui': {\n";
    print "'labelStacking': 'horizontal'";
    print ",'allowTyping': false, 'allowNone': true, 'allowMultiple': false";
    print ",'sortValues': true";
    print ",'caption': 'All'}";
    print "}\n";
    print "});\n";

    print "var measurementsTable = new google.visualization.ChartWrapper({\n";
    print "'chartType': 'Table'";
    print ",'containerId': 'measurementsTableDiv'";
    print ",options: {allowHtml: true}\n";
    print ",view: {'columns': [1,2,4,5,6,7,8,9,10]}\n";
    print "});\n";

    print "measurementsDashboard.bind([nodePicker,typePicker], measurementsTable);\n";

    print "measurementsDashboard.draw(myMeasurements);\n";

    print "}\n";

    # Create and update the list of selected measurements based on events
    if ($visualizedMeasurements) {
        print "var selectedMeasurements=[\"" . join( "\",\"", split( /,/, $visualizedMeasurements ) ) . "\"]\n";
    }
    else {
        print "var selectedMeasurements=[]\n";
    }

    print "function updateSelected(elmt) {";

    print "if(elmt.checked){\n";
    print "selectedMeasurements.push(elmt.value);\n";

    # print "console.log(\"Added \"+elmt.value);\n";
    print "} else {";
    print "for (var i = 0; i < selectedMeasurements.length; i++) {\n";
    print "if (selectedMeasurements[i]==elmt.value) {\n";
    print "selectedMeasurements.splice(i,1);\n";

    #print "console.log(\"Removed \"+elmt.value);\n";
    print "}\n";
    print "}\n";

    print "}\n";

    # print "console.log(selectedMeasurements);\n";

    print
"var customizeSelectedMeasurementsHtml=\"<font size=-1>You can now toggle on/off multiple measurements</font>\";\n";

    print "if (selectedMeasurements.length>0){\n";
    print "var customizeMeasurementsURL=\"https://hecate.hakai.org"
      . getBaseURL($dataTable)
      . "\"+\"&measurements=\"+selectedMeasurements.join()";
    print "+encodeURI(\"$dateRangeParameters\")" if defined $dateRangeParameters;
    print ";\n";
    print "customizeSelectedMeasurementsHtml=\"<b><a href=\"+customizeMeasurementsURL+\">"
      . "Here</a> is a link that includes the \"+selectedMeasurements.length+\" selected measurements\";\n";
    print "};\n";

    print "document.getElementById('customizeSelectedMeasurementsDiv').innerHTML=customizeSelectedMeasurementsHtml;\n";
    print "};\n";

    print "</script>\n";

    print "</head>\n";
    print "<body>\n";

    print $bsBody;    # Add in Bootstrap stuff

    printNavigationHeader("Customize");

    print "<hr>\n";
    print "<center>\n";

    print "<div id=\"customizeSelectedMeasurementsDiv\">You can now toggle on/off viewed measurements</div><br>";

    print "<div id=\"measurementsDashboardDiv\">\n";
    print "<div id=\"nodePickerDiv\"></div>\n";
    print "<div id=\"typePickerDiv\"></div>\n";

    # Allow the user to switch to a different measurement interval.  Not all measurements
    # are available for each measurement interval
    if (@otherSampleIntervalLinks) {
        my $otherSampleIntervalLinkList = join( ", ", @otherSampleIntervalLinks );
        $otherSampleIntervalLinkList =~ s/\">/&customize\">/g;

        print "<font size=\"-1\"><b>Frequency:</b> $otherSampleIntervalLinkList measurements</font><br>";
    }

    print "<div id=\"measurementsTableDiv\"></div>\n";
    print "</div>\n";
    print "</center>\n";

    print "</body>\n";
    print "</html>\n";

    # Log and audit all access
    auditDataAccess();

    return;
}

my $firstDatabaseTable;
my $firstDatabaseMeasurementTime;
my $lastDatabaseMeasurementTime;
my @databaseColumns;
my $currentColumnNum = 1;
my %databaseTables;
my @aggregatedMeasurementKeys;

foreach my $measurementKey ( sort keys %currentMeasurements ) {
    my $sensorNode           = $currentMeasurements{$measurementKey}{sensorNode};
    my $measurementName      = $currentMeasurements{$measurementKey}{measurementName};
    my $databaseTable        = $currentMeasurements{$measurementKey}{databaseTable};
    my $databaseColumn       = $currentMeasurements{$measurementKey}{databaseColumn};
    my $firstMeasurementTime = $currentMeasurements{$measurementKey}{firstMeasurementTime};
    my $lastMeasurementTime  = $viewedSensorNodes{$sensorNode}{dtLastseen};
    $lastMeasurementTime = $firstMeasurementTime unless defined $lastMeasurementTime;

    next unless defined $databaseTable && defined $databaseColumn;

    if ( defined $firstMeasurementTime ) {
        if (
               !defined $firstDatabaseTable
            || $lastMeasurementTime gt $lastDatabaseMeasurementTime
            || (   $lastMeasurementTime eq $lastDatabaseMeasurementTime
                && $firstMeasurementTime lt $firstDatabaseMeasurementTime )
          )
        {
            $log->debug( "[$databaseTable] firstMeasurementTime=$firstMeasurementTime"
                  . ",lastMeasurementTime=$lastMeasurementTime" );

            $firstDatabaseTable           = $databaseTable;
            $firstDatabaseMeasurementTime = $firstMeasurementTime;
            $lastDatabaseMeasurementTime  = $lastMeasurementTime;
        }
    }

    $databaseTables{$databaseTable}{columns}{$databaseColumn} = 1;

    my $dbColumnName = "$databaseTable.$databaseColumn";
    if ( exists $currentMeasurements{$measurementKey}{qlField} ) {
        $dbColumnName = "COALESCE($dbColumnName,1)";
    }
    elsif ( exists $currentMeasurements{$measurementKey}{uqlField} ) {
        $dbColumnName = "COALESCE($dbColumnName,2)";
    }

    push @databaseColumns, $dbColumnName;

    $currentMeasurements{$measurementKey}{columnNum}     = $currentColumnNum++;
    $currentMeasurements{$measurementKey}{aggregateData} = 1 if $measurementName =~ /Agg/;
    $currentMeasurements{$measurementKey}{aggregateMtd}  = 1 if $measurementName =~ /Mtd/;
    $currentMeasurements{$measurementKey}{aggregateYtd}  = 1 if $measurementName =~ /Ytd/;

    push @aggregatedMeasurementKeys, $measurementKey
      if $currentMeasurements{$measurementKey}{aggregateData}
      || $currentMeasurements{$measurementKey}{aggregateMtd}
      || $currentMeasurements{$measurementKey}{aggregateYtd};
}

my @leftOuterJoins;
foreach my $databaseTable ( sort keys %databaseTables ) {
    next if $databaseTable eq $firstDatabaseTable;

    push @leftOuterJoins,
      " LEFT OUTER JOIN $databaseTable on ($databaseTable.measurement_time=$firstDatabaseTable.measurement_time )";
}

my $sql = "SELECT $firstDatabaseTable.measurement_time at time zone 'PST',";
$sql .= join( ",", @databaseColumns );
$sql .= " FROM $firstDatabaseTable";
$sql .= join( "", @leftOuterJoins ) if @leftOuterJoins;
$sql .= " WHERE $firstDatabaseTable.measurement_time at time zone 'PST'>='$firstMeasurementTime'"
  if $firstMeasurementTime;
$sql .= " AND $firstDatabaseTable.measurement_time at time zone 'PST'<='$lastMeasurementTime'" if $lastMeasurementTime;
$sql .= " ORDER BY 1";

$log->debug("[$pgName:$dbSchema] SQL: $sql");

my $dataSth = $dbh->prepare($sql);
$dataSth->execute();

################################################################################
# Support downloading data as a CVS file
if ( defined $downloadFlag ) {
    my $csvFilename = DateTime->today()->ymd . ".$dataTable";
    $csvFilename .= ".$dateRange" if defined $dateRange;
    $csvFilename .= "-raw"        if defined $originalFlag;
    $csvFilename .= ".csv";

    print "Content-Type:application/x-download\n";
    print "Content-Disposition:attachment;filename=$csvFilename\n\n";

    my @downloadedSensorNodes;
    my @downloadedMeasurementNames;
    my @downloadedDisplayNames;
    my @downloadedMeasurementUnits = ("PST");

    if ( $dataTable eq $oneDayTable ) {
        push @downloadedSensorNodes,      ("date");
        push @downloadedMeasurementNames, ("date");
        push @downloadedDisplayNames,     ("Date");
    }
    else {
        push @downloadedSensorNodes,      ("measurementTime");
        push @downloadedMeasurementNames, ("measurementTime");
        push @downloadedDisplayNames,     ("Measurement time");
    }

    push @downloadedSensorNodes,      ( "Year", "Month", "WaterYear" );
    push @downloadedMeasurementNames, ( "year", "month", "waterYear" );
    push @downloadedDisplayNames,     ( "Year", "Month", "WaterYear" );
    push @downloadedMeasurementUnits, ( "Year", "Month", "WaterYear" );

    my @downloadedColumns = (0);
    my %qcColumnNums;
    my %aggregatedColumnNums;
    my %aggregatedMtdColumnNums;
    my %aggregatedYtdColumnNums;

    foreach my $viewedMeasurement (@viewedMeasurementList) {

        my $viewedMeasurementKey = $viewedMeasurement->{measurementKey};
        next unless $currentMeasurements{$viewedMeasurementKey}{columnNum};

        my $baseMeasurementName = $currentMeasurements{$viewedMeasurementKey}{baseMeasurementName};

        if ( defined $currentMeasurements{$viewedMeasurementKey}{aggregateData} ) {
            $aggregatedColumnNums{ $currentMeasurements{$viewedMeasurementKey}{columnNum} } = 1;
        }
        elsif ( defined $currentMeasurements{$viewedMeasurementKey}{aggregateMtd} ) {
            $aggregatedMtdColumnNums{ $currentMeasurements{$viewedMeasurementKey}{columnNum} } = 1;
        }
        elsif ( defined $currentMeasurements{$viewedMeasurementKey}{aggregateYtd} ) {
            $aggregatedYtdColumnNums{ $currentMeasurements{$viewedMeasurementKey}{columnNum} } = 1;
        }

        next if defined $baseMeasurements{$baseMeasurementName}{downloadSetup};
        $baseMeasurements{$baseMeasurementName}{downloadSetup} = 1;

        my $qlMeasurementKey = $baseMeasurements{$baseMeasurementName}{qlMeasurementKey};
        if ( defined $qlMeasurementKey ) {
            my $displayName = $currentMeasurements{$qlMeasurementKey}{displayName};
            $displayName =~ s/([a-z]+)\s+(?=[A-Z])/$1/g;
            $displayName =~ s/\s+/_/g;

            push @downloadedSensorNodes,      $currentMeasurements{$qlMeasurementKey}{sensorNode};
            push @downloadedMeasurementNames, $currentMeasurements{$qlMeasurementKey}{measurementName};
            push @downloadedDisplayNames,     $displayName;
            push @downloadedMeasurementUnits, $currentMeasurements{$qlMeasurementKey}{measurementUnits};
            push @downloadedColumns,          $currentMeasurements{$qlMeasurementKey}{columnNum};
        }

        my $qcMeasurementKey = $baseMeasurements{$baseMeasurementName}{qcMeasurementKey};
        if ( defined $qcMeasurementKey ) {
            my $displayName = $currentMeasurements{$qcMeasurementKey}{displayName};
            $displayName =~ s/([a-z]+)\s+(?=[A-Z])/$1/g;
            $displayName =~ s/\s+/_/g;

            push @downloadedSensorNodes,      $currentMeasurements{$qcMeasurementKey}{sensorNode};
            push @downloadedMeasurementNames, $currentMeasurements{$qcMeasurementKey}{measurementName};
            push @downloadedDisplayNames,     $displayName;
            push @downloadedMeasurementUnits, $currentMeasurements{$qcMeasurementKey}{measurementUnits};
            push @downloadedColumns,          $currentMeasurements{$qcMeasurementKey}{columnNum};

            $qcColumnNums{ $currentMeasurements{$qcMeasurementKey}{columnNum} } = 1;
        }

        my $uqlMeasurementKey = $baseMeasurements{$baseMeasurementName}{uqlMeasurementKey};
        if ( defined $uqlMeasurementKey ) {
            my $displayName = $currentMeasurements{$uqlMeasurementKey}{displayName};
            $displayName =~ s/([a-z]+)\s+(?=[A-Z])/$1/g;
            $displayName =~ s/\s+/_/g;

            push @downloadedSensorNodes,      $currentMeasurements{$uqlMeasurementKey}{sensorNode};
            push @downloadedMeasurementNames, $currentMeasurements{$uqlMeasurementKey}{measurementName};
            push @downloadedDisplayNames,     $displayName;
            push @downloadedMeasurementUnits, $currentMeasurements{$uqlMeasurementKey}{measurementUnits};
            push @downloadedColumns,          $currentMeasurements{$uqlMeasurementKey}{columnNum};
        }

        foreach my $measurementKey ( sort keys %{ $baseMeasurements{$baseMeasurementName}{measurementsKeys} } ) {

            my $displayName = $currentMeasurements{$measurementKey}{displayName};
            $displayName =~ s/([a-z]+)\s+(?=[A-Z])/$1/g;
            $displayName =~ s/\s+/_/g;

            push @downloadedSensorNodes,      $currentMeasurements{$measurementKey}{sensorNode};
            push @downloadedMeasurementNames, $currentMeasurements{$measurementKey}{measurementName};
            push @downloadedDisplayNames,     $displayName;
            push @downloadedMeasurementUnits, $currentMeasurements{$measurementKey}{measurementUnits};
            push @downloadedColumns,          $currentMeasurements{$measurementKey}{columnNum};
        }
    }

    print join( ",", @downloadedDisplayNames ) . "\r\n";
    print join( ",", @downloadedMeasurementUnits ) . "\r\n";
    print join( ",", @downloadedSensorNodes ) . "\r\n";
    print join( ",", @downloadedMeasurementNames ) . "\r\n";

    my @monthAbbreviations =
      ( 'xxx', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec' );

    my %aggDataValues;
    my %mtdDataValues;
    my %ytdDataValues;

    my $previousWaterYear;
    my $previousMeasurementMonth;

    my $rows = [];    # cache for batches of rows
    while (
        my $row = (
            shift(@$rows) ||    # get row from cache, or reload cache:
              shift( @{ $rows = $dataSth->fetchall_arrayref( undef, 10000 ) || [] } )
        )
      )
    {
        foreach my $columnNum (@downloadedColumns) {
            print "," if $columnNum;

            if ( defined $row->[$columnNum] ) {
                if ( exists $qcColumnNums{$columnNum} ) {
                    print "\"$row->[$columnNum]\"";
                }
                elsif ( exists $aggregatedColumnNums{$columnNum} ) {
                    $aggDataValues{$columnNum} += $row->[$columnNum];

                    print $aggDataValues{$columnNum};
                }
                elsif ( exists $aggregatedMtdColumnNums{$columnNum} ) {
                    $mtdDataValues{$columnNum} += $row->[$columnNum];

                    print $mtdDataValues{$columnNum};
                }
                elsif ( exists $aggregatedYtdColumnNums{$columnNum} ) {
                    $ytdDataValues{$columnNum} += $row->[$columnNum];

                    print $ytdDataValues{$columnNum};
                }
                elsif ( $columnNum > 0 ) {
                    print $row->[$columnNum];
                }
            }

            if ( $columnNum == 0 ) {
                my $measurementYear  = substr $row->[0], 0,  4;
                my $measurementMonth = substr $row->[0], 5,  2;
                my $measurementDay   = substr $row->[0], 8,  2;
                my $measurementTime  = substr $row->[0], 11, 8;

                if ( $measurementTime eq "00:00:00" ) {
                    $measurementDay--;
                    $measurementTime = "24:00:00";
                    if ( $measurementDay == 0 ) {
                        $measurementMonth--;
                        $measurementDay = 31;
                        if ( $measurementMonth == 0 ) {
                            $measurementYear--;
                            $measurementMonth = 12;
                        }
                    }
                }

                my $waterYearStart = $measurementYear;
                $waterYearStart-- if $measurementMonth < 10;

                if ( $dataTable eq $oneDayTable ) {
                    print &getPreviousDate( $row->[0] );
                }
                else {
                    print $row->[0];
                }

                print ",$measurementYear,"
                  . $monthAbbreviations[$measurementMonth]
                  . ",$waterYearStart-"
                  . ( $waterYearStart + 1 );

                # Reset the month-to-date and year-to-date accumulated values

                if ( !defined $previousMeasurementMonth || $previousMeasurementMonth != $measurementMonth ) {
                    undef %mtdDataValues;

                    $previousMeasurementMonth = $measurementMonth;
                }

                if ( !defined $previousWaterYear || $previousWaterYear != $waterYearStart ) {
                    undef %ytdDataValues;

                    $previousWaterYear = $waterYearStart;
                }
            }
        }

        print "\r\n";
    }

    # Log and audit all access
    auditDataAccess();

    return;
}

my $rows = [];    # cache for batches of rows
my @dataRows;
undef $firstMeasurementTime;
my %lastAnnotation;
my %lastQualityLevel;
my %aggDataValues;
my %mtdDataValues;
my %ytdDataValues;
my $previousWaterYear;
my $previousMeasurementMonth;
my $numEstimatedIntervals;
my $numIntervals = 0;
my $numRowsToSkip;
my $numSkippedRows;

while (
    my $row = (
        shift(@$rows) ||    # get row from cache, or reload cache:
          shift( @{ $rows = $dataSth->fetchall_arrayref( undef, 10000 ) || [] } )
    )
  )
{
    $numIntervals++;

    $measurementTime = $row->[0];

    {
        my $measurementYear  = substr $measurementTime, 0,  4;
        my $measurementMonth = substr $measurementTime, 5,  2;
        my $measurementDay   = substr $measurementTime, 8,  2;
        my $measurementHms   = substr $measurementTime, 11, 8;

        if ( $measurementHms eq "00:00:00" ) {
            $measurementDay--;
            $measurementHms = "24:00:00";
            if ( $measurementDay == 0 ) {
                $measurementMonth--;
                $measurementDay = 31;
                if ( $measurementMonth == 0 ) {
                    $measurementYear--;
                    $measurementMonth = 12;
                }
            }
        }

        my $waterYear = $measurementYear;
        $waterYear-- if $measurementMonth < 10;

        # Reset the month-to-date and year-to-date accumulated values
        if ( !defined $previousMeasurementMonth || $previousMeasurementMonth != $measurementMonth ) {
            undef %mtdDataValues;

            $previousMeasurementMonth = $measurementMonth;
        }

        if ( !defined $previousWaterYear || $previousWaterYear != $waterYear ) {
            undef %ytdDataValues;

            $previousWaterYear = $waterYear;
        }
    }

    # Estimate the number of intervals to be found so we can potentially reduce the number
    # of data rows returned to the browser, and improve viewing performance
    if ( !defined $numEstimatedIntervals ) {

        my $startDateTime = DateTime->new(
            year  => substr( $measurementTime, 0, 4 ),
            month => substr( $measurementTime, 5, 2 ),
            day   => substr( $measurementTime, 8, 2 )
        );

        my $endDateTime;
        if ( defined $lastMeasurementTime ) {
            $endDateTime = DateTime->new(
                year  => substr( $lastMeasurementTime, 0, 4 ),
                month => substr( $lastMeasurementTime, 5, 2 ),
                day   => substr( $lastMeasurementTime, 8, 2 )
            );
        }
        else {
            $endDateTime = DateTime->today();
        }

        $numEstimatedIntervals =
          int( $endDateTime->delta_days($startDateTime)->in_units('days') * 1440 / $sampleInterval );

        # Calculate an target compression factor intended to have < 6000 rows of data returned
        my $targetCompressionFactor = int( $numEstimatedIntervals / 3000 );

        $numRowsToSkip = ( $targetCompressionFactor - 1 ) if $targetCompressionFactor >= 2;

        $log->info( "[$remoteUser\@$remoteHost] Estimated $numEstimatedIntervals intervals"
              . ", calculated targetCompressionFactor=$targetCompressionFactor" );

        undef $lastMeasurementTime;
    }

    $firstMeasurementTime = substr( $measurementTime, 0, 16 ) unless defined $firstMeasurementTime;
    $lastMeasurementTime = substr( $measurementTime, 0, 16 );

    foreach my $measurementKey ( keys %currentMeasurements ) {
        next unless defined $currentMeasurements{$measurementKey}{columnName};

        if ( defined $row->[ $currentMeasurements{$measurementKey}{columnNum} ] ) {
            $currentMeasurements{$measurementKey}{measurementsFound}++;
        }
        else {
            $currentMeasurements{$measurementKey}{measurementsMissing}++;
        }
    }

    my %qualityLevels;
    my %qcFlags;
    foreach my $baseMeasurementName ( sort keys %baseMeasurements ) {

        my $qlMeasurementKey = $baseMeasurements{$baseMeasurementName}{qlMeasurementKey};
        my $qcMeasurementKey = $baseMeasurements{$baseMeasurementName}{qcMeasurementKey};

        # Count the number of measurements at each quality level
        if ( defined $qlMeasurementKey && exists $currentMeasurements{$qlMeasurementKey}{columnNum} ) {
            my $qualityLevel = $row->[ $currentMeasurements{$qlMeasurementKey}{columnNum} ];
            $qualityLevel = 1 unless defined $qualityLevel;    # Automated QC flags and cleaning completed

            $qualityLevels{$baseMeasurementName} = $qualityLevel;

            $currentMeasurements{$qlMeasurementKey}{qualityLevelCounts}{$qualityLevel}++;
        }

        if ( defined $qcMeasurementKey && exists $currentMeasurements{$qcMeasurementKey}{columnNum} ) {
            my $qcFlag = $row->[ $currentMeasurements{$qcMeasurementKey}{columnNum} ];
            next unless defined $qcFlag;

            $qcFlag =~ s/'/&apos;/g;
            $qcFlags{$baseMeasurementName} = $qcFlag;

            $currentMeasurements{$qcMeasurementKey}{flaggedMeasurements}++ if index( $qcFlag, "AV" ) < 0;
        }
    }

    my $dataRow;
    if ( $dataTable eq $oneDayTable ) {
        $dataRow = "[new " . getDataTableDateType( &getPreviousDate($measurementTime) );
    }
    else {
        $dataRow = "[new " . getDataTableDateTimeType($measurementTime);
    }

    my $dontSkip;

    # Handle data aggregation
    foreach my $aggregatedMeasurementKey (@aggregatedMeasurementKeys) {

        my $dataValue = $row->[ $currentMeasurements{$aggregatedMeasurementKey}{columnNum} ];
        next unless defined $dataValue;

        if ( exists $currentMeasurements{$aggregatedMeasurementKey}{aggregateData} ) {
            $aggDataValues{$aggregatedMeasurementKey} += $dataValue;
        }
        elsif ( exists $currentMeasurements{$aggregatedMeasurementKey}{aggregateMtd} ) {
            $mtdDataValues{$aggregatedMeasurementKey} += $dataValue;
        }
        elsif ( exists $currentMeasurements{$aggregatedMeasurementKey}{aggregateYtd} ) {
            $ytdDataValues{$aggregatedMeasurementKey} += $dataValue;
        }
    }

    foreach my $viewedMeasurement (@viewedMeasurementList) {

        my $viewedMeasurementKey = $viewedMeasurement->{measurementKey};
        next unless $currentMeasurements{$viewedMeasurementKey}{columnNum};

        my $baseMeasurementName = $currentMeasurements{$viewedMeasurementKey}{baseMeasurementName};

        my $qualityLevel = $qualityLevels{$baseMeasurementName};
        my $qcFlag       = $qcFlags{$baseMeasurementName};

        my $tooltipText;

        if ( $dataTable eq $oneDayTable ) {
            $tooltipText = "<b>" . &getPreviousDate($measurementTime) . "</b>";
        }
        else {
            $tooltipText = "<b>$measurementTime PST</b>";
        }

        my $foundDataValue;
        foreach my $measurementKey ( sort keys %{ $baseMeasurements{$baseMeasurementName}{measurementsKeys} } ) {
            my $dataValue;
            if ( exists $currentMeasurements{$measurementKey}{aggregateData} ) {
                $dataValue = $aggDataValues{$measurementKey};
            }
            elsif ( exists $currentMeasurements{$measurementKey}{aggregateMtd} ) {
                $dataValue = $mtdDataValues{$measurementKey};
            }
            elsif ( exists $currentMeasurements{$measurementKey}{aggregateYtd} ) {
                $dataValue = $ytdDataValues{$measurementKey};
            }
            else {
                $dataValue = $row->[ $currentMeasurements{$measurementKey}{columnNum} ];
            }

            if ( defined $dataValue ) {
                if ( $measurementKey eq $viewedMeasurementKey ) {
                    $tooltipText .= "<br><b>" . $currentMeasurements{$measurementKey}{columnName} . "</b> = $dataValue";
                }
                else {
                    $tooltipText .= "<br>" . $currentMeasurements{$measurementKey}{columnName} . " = $dataValue";
                }
            }

            next unless $measurementKey eq $viewedMeasurementKey;

            if ( defined $dataValue ) {
                $foundDataValue = $dataRow .= ",$dataValue";
            }
            else {
                $dataRow .= ",null";
            }
        }

        my $certainty      = "true";
        my $annotation     = "null";
        my $annotationText = "null";

        $tooltipText .= "<br>Quality level = $qualityLevel" if defined $qualityLevel;
        $tooltipText .= "<br>Quality flag = $qcFlag"        if defined $qcFlag;

        if ( defined $qcFlag && index( $qcFlag, "AV" ) < 0 ) {

            $certainty = "false";

            $annotation = $qcFlag;
            $annotation =~ s/:.*//;
            $annotation = "'$annotation'";
        }

        if ( defined $lastAnnotation{$viewedMeasurement} && $annotation ne "null" ) {
            $lastAnnotation{$viewedMeasurement}{lastAnnotation} = $annotation;
            $lastAnnotation{$viewedMeasurement}{lastLevel}      = $qualityLevel;
            $lastAnnotation{$viewedMeasurement}{lastFlag}       = $qcFlag;
            $lastAnnotation{$viewedMeasurement}{endTime}        = $measurementTime;
            $lastAnnotation{$viewedMeasurement}{annotationCount}++;

            $annotation = "null";
        }
        elsif ( defined $lastAnnotation{$viewedMeasurement} && $annotation eq "null" && $foundDataValue ) {
            $annotation = $lastAnnotation{$viewedMeasurement}{lastAnnotation};

            if ( $lastAnnotation{$viewedMeasurement}{annotationCount} > 1 ) {
                $annotationText = "'<b>"
                  . $lastAnnotation{$viewedMeasurement}{startTime} . " - "
                  . $lastAnnotation{$viewedMeasurement}{endTime} . "</b>"
                  . "<br>Previous "
                  . $lastAnnotation{$viewedMeasurement}{annotationCount}
                  . " measurements have been flagged";
            }
            else {
                $annotationText = "'<b>Previous measurement was flagged</b>";
            }
            $annotationText .= "<br>Last quality level = " . $lastAnnotation{$viewedMeasurement}{lastLevel}
              if defined $lastAnnotation{$viewedMeasurement}{lastLevel};
            $annotationText .= "<br>Last quality flag = " . $lastAnnotation{$viewedMeasurement}{lastFlag} . "'"
              if defined $lastAnnotation{$viewedMeasurement}{lastFlag};

            $lastAnnotation{$viewedMeasurement} = undef;
        }
        elsif ( !defined $lastAnnotation{$viewedMeasurement} && $annotation ne "null" ) {
            $lastAnnotation{$viewedMeasurement}{lastAnnotation}  = $annotation;
            $lastAnnotation{$viewedMeasurement}{lastFlag}        = $qcFlag;
            $lastAnnotation{$viewedMeasurement}{startTime}       = $measurementTime;
            $lastAnnotation{$viewedMeasurement}{annotationCount} = 1;

            $annotation = "null";
        }

        if (   $annotation eq "null"
            && defined $qualityLevel
            && exists $lastQualityLevel{$viewedMeasurement}
            && $qualityLevel != $lastQualityLevel{$viewedMeasurement} )
        {
            $annotationText = "'<b>$measurementTime</b><br>";

            if ( $qualityLevel > $lastQualityLevel{$viewedMeasurement} ) {
                $annotation = "'QL+'";
                $annotationText .=
                  "Quality level increased from $lastQualityLevel{$viewedMeasurement} to $qualityLevel'";
            }
            else {
                $annotation = "'QL-'";
                $annotationText .=
                  "Quality level decreased from $lastQualityLevel{$viewedMeasurement} to $qualityLevel'";
            }
        }

        if ( !exists $myParameter{noFlags} ) {
            $dataRow .= ",$certainty,$annotation,$annotationText,'$tooltipText'";

            $dontSkip = 1 if $annotation ne "null";
        }
        else {
            $dataRow .= ",'$tooltipText'";
        }

        $lastQualityLevel{$viewedMeasurement} = $qualityLevel;
    }

    $dataRow .= "]";

    next if $numRowsToSkip && defined $numSkippedRows && $numSkippedRows++ < $numRowsToSkip && !defined $dontSkip;

    push @dataRows, $dataRow;
    $numSkippedRows = 0;
}

my $viewableMeasurements = 0;
{
    foreach my $viewedMeasurement (@viewedMeasurementList) {

        my $viewedMeasurementKey = $viewedMeasurement->{measurementKey};
        next
          unless exists $currentMeasurements{$viewedMeasurementKey}{columnNum}
          && exists $currentMeasurements{$viewedMeasurementKey}{measurementsFound};

        $viewableMeasurements++;
    }
}

errorOut("Failed to find any measurement data") unless @dataRows && $viewableMeasurements > 0;

$log->info("[$remoteUser\@$remoteHost] Found $numIntervals intervals (estimated $numEstimatedIntervals)");
$log->info( "[$remoteUser\@$remoteHost] Returned " . scalar(@dataRows) . " of $numIntervals intervals" )
  if defined $numRowsToSkip;

##############################################################################################
# Get default presentation options, including y-axis title, y-axis options and series options
my @defaultPresentationOptions = (
    {
        measurementName => "_Std\$",
        axisTitle       => "Standard deviation",
        seriesOptions   => "lineDashStyle:[2,2],lineWidth:1"
    },
    { measurementName => "ShuntCurrent",        axisTitle => "Current" },
    { measurementName => "ShuntAmpHours",       axisTitle => "Amp hours" },
    { measurementName => "Air.*Temp",           axisTitle => "Air temperature" },
    { measurementName => "Soil.*Temp",          axisTitle => "Soil temperature" },
    { displayName     => "TWtr",                axisTitle => "Water temperature" },
    { measurementName => "PLS.*Temp",           axisTitle => "Water temperature" },
    { measurementName => "EC.*Temp",            axisTitle => "Water temperature" },
    { measurementName => "Temp",                axisTitle => "Temperature" },
    { measurementName => "delta.*Pressure",     axisTitle => "Delta pressure" },
    { measurementName => "spanPressurePumpOn",  axisTitle => "Span pump pressure on/off" },
    { measurementName => "spanPressurePumpOff", axisTitle => "Span pump pressure on/off" },
    { measurementName => "AirPressure",         axisTitle => "Air pressure" },
    { measurementName => "RH",                  axisTitle => "Relative humidity" },
    { measurementName => "WindSpd",             axisTitle => "Wind speed" },
    { measurementName => "WindDir", axisTitle => "Wind direction", seriesOptions => "type:'scatter',pointSize:2" },
    {
        measurementName => "RainAgg",
        axisTitle       => "Aggregated rain",
        seriesOptions   => "type:'area',areaOpacity:'0.10',lineWidth:1"
    },
    {
        measurementName => "RainMtd",
        axisTitle       => "Month-to-date rain",
        seriesOptions   => "type:'area',areaOpacity:'0.10',lineWidth:1"
    },
    {
        measurementName => "RainYtd",
        axisTitle       => "Year-to-date rain",
        seriesOptions   => "type:'area',areaOpacity:'0.10',lineWidth:1"
    },
    {
        measurementName => "Rain",
        axisTitle       => "Rain",
        seriesOptions   => "type:'area',areaOpacity:'0.10',lineWidth:1"
    },
    {
        measurementName => "DischargeVolumeAgg",
        axisTitle       => "Aggregated discharge",
        seriesOptions   => "type:'area',areaOpacity:'0.10',lineWidth:1"
    },
    {
        measurementName => "DischargeVolumeMtd",
        axisTitle       => "Month-to-date discharge",
        seriesOptions   => "type:'area',areaOpacity:'0.10',lineWidth:1"
    },
    {
        measurementName => "DischargeVolumeYtd",
        axisTitle       => "Year-to-date discharge",
        seriesOptions   => "type:'area',areaOpacity:'0.10',lineWidth:1"
    },
    { measurementName => "cm.*water",       axisTitle => "Water depth" },
    { measurementName => "PLS.*Lvl",        axisTitle => "Water depth" },
    { measurementName => "WaterDepth",      axisTitle => "Water depth" },
    { measurementName => "SensorDepth",     axisTitle => "Sensor depth" },
    { measurementName => "SR50A.*Distance", axisTitle => "SR50 distance" },
    { measurementName => "SnowDepth",       axisTitle => "Snow depth" },
    { measurementName => "Well_PT",         axisTitle => "Water above well sensor" },
    { measurementName => "_wtd",         axisTitle => "Water table depth", axisOptions => "minValue:0,direction:-1" },
    { measurementName => "fDOM",         axisTitle => "fDOM" },
    { measurementName => "pCO2",         axisTitle => "pCO2" },
    { measurementName => "xCO2",         axisTitle => "xCO2" },
    { measurementName => "pH.*",         axisTitle => "pH" },
    { measurementName => "Turbidity",    axisTitle => "Turbidity" },
    { measurementName => "Solar",        axisTitle => "Solar" },
    { measurementName => "PAR",          axisTitle => "PAR" },
    { measurementName => "UVRad",        axisTitle => "UV radiation" },
    { measurementName => "Conductance",  axisTitle => "Conductivity" },
    { measurementName => "Conductivity", axisTitle => "Conductivity" },
    { measurementName => "_EC",          axisTitle => "Conductivity" },
    { measurementName => "salinity",     axisTitle => "Salinity" },
    { measurementName => "Salinity",     axisTitle => "Salinity" },
    { measurementName => "OxygenSat",    axisTitle => "Oxygen saturation" },
    { measurementName => "O2Concentration", axisTitle => "Oxygen concentration" },
    { measurementName => "WaterOxygen",     axisTitle => "Oxygen concentration" },
    { measurementName => "TideHeight",      axisTitle => "Tide height" },
    { measurementName => "DischargeVolume", axisTitle => "Discharge volume" },
    { measurementName => "Discharge",       axisTitle => "Discharge" },
    { measurementName => "AirFlow",         axisTitle => "Air flow" },
    { measurementName => "CO2Flow",         axisTitle => "CO2 flow" },
    { measurementName => "AirSetPoint",     axisTitle => "Air flow", measurementUnits => "lpm" },
    { measurementName => "CO2SetPoint",     axisTitle => "CO2 flow", measurementUnits => "lpm" },
    { measurementName => "AirVolumetric",   axisTitle => "Air flow" },
    { measurementName => "CO2Volumetric",   axisTitle => "CO2 flow" },
    { measurementName => "AirTemp",         axisTitle => "Air temperature" },
    { measurementName => "CO2Temp",         axisTitle => "CO2 temperature" },
    { measurementName => "AirAbsolute",     axisTitle => "Air pressure" },
    { measurementName => "CO2Absolute",     axisTitle => "CO2 pressure" }
);

sub getPresentationOptions {
    my ( $measurementName, $displayName, $measurementUnits, $presentationOptions ) = @_;

    foreach my $presentationOption (@defaultPresentationOptions) {
        if (
            (
                defined $presentationOption->{measurementName}
                && $measurementName =~ /$presentationOption->{measurementName}/
            )
            || (   defined $displayName
                && defined $presentationOption->{displayName}
                && $displayName =~ /$presentationOption->{displayName}/ )
          )
        {
            $presentationOptions->{axisTitle} = $presentationOption->{axisTitle}
              if defined $presentationOption->{axisTitle};
            $presentationOptions->{axisOptions} = $presentationOption->{axisOptions}
              if defined $presentationOption->{axisOptions};
            $presentationOptions->{seriesOptions} = $presentationOption->{seriesOptions}
              if defined $presentationOption->{seriesOptions};

            if ( defined $presentationOption->{measurementUnits} ) {
                $measurementUnits = $presentationOption->{measurementUnits};
                $presentationOptions->{measurementUnits} = $measurementUnits;
            }

            last;
        }
    }

    if ( !defined $presentationOptions->{axisTitle} ) {
        if ( defined $displayName ) {
            $presentationOptions->{axisTitle} = $displayName;
        }
        else {
            $presentationOptions->{axisTitle} = $measurementName;
        }
        $presentationOptions->{axisTitle} =~ s/ Med$//g;
        $presentationOptions->{axisTitle} =~ s/ Avg$//g;
        $presentationOptions->{axisTitle} =~ s/ Min$//g;
        $presentationOptions->{axisTitle} =~ s/ Max$//g;
        $presentationOptions->{axisTitle} =~ s/ Std$//g;
    }

    if ( index( $measurementName, "_Std" ) < 0 && defined $measurementUnits ) {
        $measurementUnits = "deg C" if lc($measurementUnits) eq "deg c";

        $presentationOptions->{axisTitle} .= " ($measurementUnits)";
        $presentationOptions->{measurementUnits} = "$measurementUnits";
    }
}

################################################################################
# Generate some date time strings
my $oneShiftBeforeStart;
my $twoShiftsBeforeStart;
my $oneShiftAfterStart;
my $twoShiftsAfterStart;
my $oneShiftBeforeEnd;
my $twoShiftsBeforeEnd;
my $oneShiftAfterEnd;
my $twoShiftsAfterEnd;
my $shiftDays;

if ( ( $numIntervals * $sampleInterval ) > ( 5 * 1440 ) ) {
    $shiftDays = int( ( $numIntervals * $sampleInterval ) / 1440 / 4 + 0.5 );

    my $tempStartDateTime = DateTime->new(
        year      => ( substr $firstMeasurementTime, 0,  4 ),
        month     => ( substr $firstMeasurementTime, 5,  2 ),
        day       => ( substr $firstMeasurementTime, 8,  2 ),
        hour      => ( substr $firstMeasurementTime, 11, 2 ),
        minute    => ( substr $firstMeasurementTime, 14, 2 ),
        time_zone => 'UTC'
    );

    $tempStartDateTime->subtract( days => $shiftDays );
    $oneShiftBeforeStart = $tempStartDateTime->ymd . "%20" . $tempStartDateTime->hms;
    $tempStartDateTime->subtract( days => $shiftDays );
    $twoShiftsBeforeStart = $tempStartDateTime->ymd . "%20" . $tempStartDateTime->hms;
    $tempStartDateTime->add( days => ( 3 * $shiftDays ) );
    $oneShiftAfterStart = $tempStartDateTime->ymd . "%20" . $tempStartDateTime->hms;
    $tempStartDateTime->add( days => $shiftDays );
    $twoShiftsAfterStart = $tempStartDateTime->ymd . "%20" . $tempStartDateTime->hms;

    my $tempEndDateTime = DateTime->new(
        year      => ( substr $lastMeasurementTime, 0,  4 ),
        month     => ( substr $lastMeasurementTime, 5,  2 ),
        day       => ( substr $lastMeasurementTime, 8,  2 ),
        hour      => ( substr $lastMeasurementTime, 11, 2 ),
        minute    => ( substr $lastMeasurementTime, 14, 2 ),
        time_zone => 'UTC'
    );

    $tempEndDateTime->subtract( days => $shiftDays );
    $oneShiftBeforeEnd = $tempEndDateTime->ymd . "%20" . $tempEndDateTime->hms;
    $tempEndDateTime->subtract( days => $shiftDays );
    $twoShiftsBeforeEnd = $tempEndDateTime->ymd . "%20" . $tempEndDateTime->hms;
    $tempEndDateTime->add( days => ( 3 * $shiftDays ) );
    $oneShiftAfterEnd = $tempEndDateTime->ymd . "%20" . $tempEndDateTime->hms;
    $tempEndDateTime->add( days => $shiftDays );
    $twoShiftsAfterEnd = $tempEndDateTime->ymd . "%20" . $tempEndDateTime->hms;
}

################################################################################
# Finish up the data file summary
print $htmlHeader;

print "<script type=\"text/javascript\" src=\"https://www.gstatic.com/charts/loader.js\"></script>\n";
print "<script type=\"text/javascript\">\n";
print "google.charts.load('current', {'packages':['corechart', 'controls']});\n";
print "google.charts.setOnLoadCallback(initializeDataView);\n";
print "var dataGraph;\n";
print "var dashboard;\n";
print "function initializeDataView() {\n";
print "var data = new google.visualization.DataTable();\n";
print "data.addColumn('datetime', 'Measurement time')\n";

my @controlColumns;
my @viewedColumns;
my @seriesOptions;
my $currentSeriesIndex = 0;
my $currentColumnNum   = 1;
my $currentToggleNum   = 1;
my @axisOptionsList;
my @viewedMeasurementsObjects;

foreach my $viewedMeasurement (@viewedMeasurementList) {

    my $viewedMeasurementKey = $viewedMeasurement->{measurementKey};
    next unless exists $currentMeasurements{$viewedMeasurementKey}{columnNum};

    my $baseMeasurementName = $currentMeasurements{$viewedMeasurementKey}{baseMeasurementName};
    my $columnName;
    foreach my $measurementKey ( sort keys %{ $baseMeasurements{$baseMeasurementName}{measurementsKeys} } ) {
        next unless $measurementKey eq $viewedMeasurementKey;

        $columnName = $currentMeasurements{$measurementKey}{columnName};

        $viewedMeasurement->{columnName} = $columnName;

        print "data.addColumn('number', '$columnName')\n";

        next if exists $myParameter{noFlags};

        print "data.addColumn({type:'boolean',role:'certainty'})\n";
        print "data.addColumn({type:'string',role:'annotation','p': {'html': true}})\n";
        print "data.addColumn({type:'string',role:'annotationText','p': {'html': true}})\n";
    }

    print "data.addColumn({type:'string',role:'tooltip','p': {'html': true}})\n";

    # Don't view if there is no data
    next unless exists $currentMeasurements{$viewedMeasurementKey}{measurementsFound};

    my $viewOptions = $viewedMeasurement->{viewOptions};

    my $axisIndex;
    $axisIndex = 0 if defined $viewOptions && index( $viewOptions, "leftAxis" ) >= 0;
    $axisIndex = 1 if defined $viewOptions && index( $viewOptions, "rightAxis" ) >= 0;

    my %presentationOptions;
    getPresentationOptions(
        $currentMeasurements{$viewedMeasurementKey}{measurementName},
        $currentMeasurements{$viewedMeasurementKey}{displayName},
        $currentMeasurements{$viewedMeasurementKey}{measurementUnits},
        \%presentationOptions
    );

    my $currentAxisTitle     = $presentationOptions{axisTitle};
    my $currentAxisOptions   = $presentationOptions{axisOptions};
    my $currentAxisUnits     = $presentationOptions{measurementUnits};
    my $currentSeriesOptions = $presentationOptions{seriesOptions};

    $axisIndex = 1
      if !defined $axisIndex
      && defined $currentAxisTitle
      && ( ( defined $axisOptions[0] && $axisOptions[0]{axisTitle} ne $currentAxisTitle )
        || ( defined $axisOptions[1] && $axisOptions[1]{axisTitle} eq $currentAxisTitle ) );
    $axisIndex = 0 if !defined $axisIndex;

    # Determine y-axis title
    $axisOptions[$axisIndex]{axisTitle} = $currentAxisTitle
      if defined $currentAxisTitle && !defined $axisOptions[$axisIndex]{axisTitle};
    $axisOptions[$axisIndex]{axisTitle} = "unknown"
      if defined $currentAxisTitle && $axisOptions[$axisIndex]{axisTitle} ne $currentAxisTitle;

    # Determine y-axis measurement units
    $axisOptions[$axisIndex]{axisUnits} = $currentAxisUnits
      if defined $currentAxisUnits && !defined $axisOptions[$axisIndex]{axisUnits};
    $axisOptions[$axisIndex]{axisUnits} = "unknown"
      if defined $currentAxisUnits && $axisOptions[$axisIndex]{axisUnits} ne $currentAxisUnits;

    # Determine y-axis options
    $axisOptions[$axisIndex]{axisOptions} = $currentAxisOptions
      if defined $currentAxisOptions && !defined $axisOptions[$axisIndex]{axisOptions};
    $axisOptions[$axisIndex]{axisOptions} = "unknown"
      if defined $currentAxisOptions && $axisOptions[$axisIndex]{axisOptions} ne $currentAxisOptions;

    my $seriesOption = "{targetAxisIndex:$axisIndex";
    $seriesOption .= ",$currentSeriesOptions" if defined $currentSeriesOptions;
    $seriesOption .= "}";

    push @seriesOptions, $currentSeriesIndex++ . ":$seriesOption";

    push @controlColumns, $currentColumnNum;
    if ( !exists $myParameter{noFlags} ) {
        push @controlColumns, $currentColumnNum + 2;
    }

    my @currentViewedColumns = ( $currentColumnNum, $currentColumnNum + 1 );

    if ( !exists $myParameter{noFlags} ) {
        push @currentViewedColumns, $currentColumnNum + 2;
        push @currentViewedColumns, $currentColumnNum + 3;
        push @currentViewedColumns, $currentColumnNum + 4;
        $currentColumnNum += 3;
    }

    push @viewedColumns, @currentViewedColumns;

    my $fullMeasurementName = $viewedMeasurement->{visualizedMeasurement};

    $viewedFullMeasurementNames{$fullMeasurementName}{viewedColumns} = join( ",", @currentViewedColumns );
    $viewedFullMeasurementNames{$fullMeasurementName}{seriesOption}  = $seriesOption;
    $viewedFullMeasurementNames{$fullMeasurementName}{columnName}    = $columnName;

    $currentColumnNum += 2;

    # Function to toggle these from being viewed
    my $toggleCheckboxId = "tog" . $currentToggleNum++;

    $viewedMeasurement->{measurementCheckbox} =
      "<input type=\"checkbox\" id=\"$toggleCheckboxId\" checked=\"checked\" onclick=\"updateView()\">";

    push @viewedMeasurementsObjects,
        "{checkboxID:\"$toggleCheckboxId\",measurementName:\""
      . $viewedMeasurement->{visualizedMeasurement}
      . "\",viewedColumns:["
      . join( ",", @currentViewedColumns )
      . "],seriesOption:$seriesOption}";
}

print "data.addRows([" . join( ",\n", @dataRows ) . "]);\n";

my $chartOptions = "chartArea: {height: '80%', width: '90%'}" . ",tooltip: {isHtml: true}";

my @yAxisChartOptions;
my @yAxisControlOptions;
foreach my $axisIndex ( 0 .. 1 ) {
    next unless exists $axisOptions[$axisIndex];

    my $currentAxisTitle   = $axisOptions[$axisIndex]{axisTitle};
    my $currentAxisUnits   = $axisOptions[$axisIndex]{axisUnits};
    my $currentAxisOptions = $axisOptions[$axisIndex]{axisOptions};

    my @currentAxisOptionList;
    if ( defined $currentAxisTitle && $currentAxisTitle ne "unknown" ) {
        push @currentAxisOptionList, "title:'$currentAxisTitle'";
    }
    elsif ( defined $currentAxisUnits && $currentAxisUnits ne "unknown" ) {
        push @currentAxisOptionList, "title:'($currentAxisUnits)'";
    }
    push @currentAxisOptionList, $currentAxisOptions
      if defined $currentAxisOptions && $currentAxisOptions ne "unknown";

    push @yAxisChartOptions, "$axisIndex:{" . join( ",", @currentAxisOptionList ) . "}" if @currentAxisOptionList;

    push @yAxisControlOptions, "$axisIndex:{$currentAxisOptions}"
      if defined $currentAxisOptions && $currentAxisOptions ne "unknown";
}

$chartOptions .= ",vAxes:{" . join( ",", @yAxisChartOptions ) . "}" if @yAxisChartOptions;
$chartOptions .= ",legend:{position:'top'}";

print "dataGraph = new google.visualization.ChartWrapper({";
print "chartType: 'LineChart',containerId: 'dataGraphDiv'" . ",view:{columns:[0," . join( ",", @viewedColumns ) . "]}";
print ",options:{$chartOptions,series:{" . join( ",", @seriesOptions ) . "}}";
print "});\n";

print "var dataControl = new google.visualization.ControlWrapper({";
print "controlType: 'ChartRangeFilter',containerId: 'dataControlDiv'";
print ",options: {'filterColumnIndex': 0";
print ",ui: {chartType:'LineChart'";
print ",chartOptions:{chartArea:{width: '90%'}";
print ",vAxes:{" . join( ",", @yAxisControlOptions ) . "}" if @yAxisControlOptions;
print ",series:{" . join( ",", @seriesOptions ) . "}";
print "}";

print ",chartView: {columns: [0," . join( ",", @controlColumns ) . "]}}";
print "}});\n";

print "google.visualization.events.addListener(dataControl, 'statechange', zoomHandler);\n";

# Show the current time range, and provide ability to view/download data within that range
print "function zoomHandler(e) {\n";
print "var currentRange=dataControl.getState().range;\n";

print "var rangeStart=currentRange.start;\n";
print "rangeStart.setTime(rangeStart.getTime() - 28800000);\n";    # Convert UTC to PST
print "var rangeStartString=rangeStart.toISOString().slice(0,16).replace(\"T\",\" \");\n";

print "var rangeEnd=currentRange.end;\n";
print "rangeEnd.setTime(rangeEnd.getTime() - 28800000);\n";        # Convert UTC to PST
print "var rangeEndString=rangeEnd.toISOString().slice(0,16).replace(\"T\",\" \");\n";

print "var viewURL=\"https://hecate.hakai.org"
  . getBaseURL( $dataTable, $visualizedMeasurements )
  . "\"+\"&firstMeasurementTime=\"+encodeURI(rangeStartString)+\"&lastMeasurementTime=\"+encodeURI(rangeEndString);\n";
print "var downloadURL=viewURL+\"&download\";\n";
print "var customRange=\"Viewing \"+rangeStartString+\" to \"+rangeEndString"
  . "+\"<br><a href=\"+viewURL+\">view</a>\""
  . "+\" <a href=\"+downloadURL+\">download</a> only this interval\"\n";
print "document.getElementById('customRangeDiv').innerHTML=customRange;\n";
print "}\n";

# Create a dashboard
print "dashboard = new google.visualization.Dashboard(document.getElementById('dashboardDiv'));\n";

# Connect the data to the time slider
print "dashboard.bind(dataControl,dataGraph);\n";

# Draw the dashboard.
print "dashboard.draw(data);\n";

# Redraw the dashboard 200 ms after a window resize
print "\$(window).resize(function() {\n";
print "if(this.resizeTO) clearTimeout(this.resizeTO);\n";
print "this.resizeTO = setTimeout(function() {\n";
print "\$(this).trigger('resizeEnd');\n";
print "}, 200);\n";
print "});\n";
print "\$(window).on('resizeEnd', function() {\n";
print "dashboard.draw(data);\n";
print "});\n";

print "}\n";

# Build the list of related measurements
my $relatedMeasurementDetails;
my @relatedMeasurementObjects;
foreach my $measurementName ( sort keys %viewedMeasurements ) {

    my @otherMeasurementList;
    my @viewedMeasurementNames;
    my @measurementNames;
    foreach my $sensorNode ( sort @{ $viewedMeasurements{$measurementName}{sensorNodes} } ) {

        my $fullMeasurementName = "$sensorNode.$measurementName";

        if ( exists $viewedFullMeasurementNames{$fullMeasurementName} ) {
            my $displayName = $sensorNode;
            $displayName = $viewedFullMeasurementNames{$fullMeasurementName}{columnName}
              if exists $viewedFullMeasurementNames{$fullMeasurementName}
              && exists $viewedFullMeasurementNames{$fullMeasurementName}{columnName};

            push @viewedMeasurementNames, "<b>$displayName</b>";
        }
        else {
            my $toggleCheckboxId = "tog" . $currentToggleNum++;

            my $currentCheckbox = "<input type=\"checkbox\" id=\"$toggleCheckboxId\"";
            $currentCheckbox .= " checked=\"checked\"" if exists $viewedFullMeasurementNames{$fullMeasurementName};
            $currentCheckbox .= " onclick=\"updateView()\">";

            # push @otherMeasurementList,
            #    "$currentCheckbox<a href=\""
            #  . getBaseURL( $dataTable, $fullMeasurementName, $dateRangeParameters )
            #  . "\">$sensorNode</a>";

            push @otherMeasurementList, "$currentCheckbox$sensorNode";

            push @relatedMeasurementObjects,
              "{checkboxID:\"$toggleCheckboxId\",measurementName:\"$fullMeasurementName\"}";
        }

        push @measurementNames, $fullMeasurementName;
    }

    # Nothing to add
    next unless @otherMeasurementList;

    $relatedMeasurementDetails .= "<div id=\"relatedMeasurements\"><font size=\"-1\">\n"
      unless defined $relatedMeasurementDetails;

    $relatedMeasurementDetails .=
        "<p><b>Add</b> measurements related to "
      . join( ", ", @viewedMeasurementNames ) . ": "
      . join( " ",  @otherMeasurementList )
      . "</p>\n";
}

# Add in the functions to show/hide data
if ( @viewedMeasurementsObjects || @relatedMeasurementObjects ) {

    print "var viewedMeasurements=[" . join( ",\n", @viewedMeasurementsObjects ) . "];\n" if @viewedMeasurementsObjects;
    print "var relatedMeasurements=[" . join( ",\n", @relatedMeasurementObjects ) . "];\n"
      if @relatedMeasurementObjects;

    print "function updateView() {";

    print "var selectedMeasurements=[];\n";

    if ( scalar(@viewedMeasurementsObjects) == 1 ) {
        print "selectedMeasurements.push(viewedMeasurements[0].measurementName);\n";
    }
    elsif ( scalar(@viewedMeasurementsObjects) > 1 ) {
        print "var selectedColumns=[0];\n";
        print "var selectedSeries=[];\n";

        print "var numViewedMeasurements = viewedMeasurements.length;\n";
        print "for (var i = 0; i < numViewedMeasurements; i++) {\n";
        print "if (document.getElementById(viewedMeasurements[i].checkboxID).checked) {\n";
        print "selectedColumns=selectedColumns.concat(viewedMeasurements[i].viewedColumns);";
        print "selectedSeries.push(viewedMeasurements[i].seriesOption);";
        print "selectedMeasurements.push(viewedMeasurements[i].measurementName);\n";
        print "}\n";
        print "}\n";

        print
"var customizeViewedMeasurementsHtml=\"<font size=-1>You can now toggle on/off viewed measurements</font>\";\n";

        print "if (selectedColumns.length>1){\n";
        print "dataGraph.setView({'columns':selectedColumns});\n";
        print "dataGraph.setOptions({$chartOptions,'series':selectedSeries});\n";
        print "if (selectedMeasurements.length<" . scalar(@viewedMeasurementList) . "){\n";
        print "var customizeMeasurementsURL=\"https://hecate.hakai.org"
          . getBaseURL($dataTable)
          . "\"+\"&measurements=\"+selectedMeasurements.join()";
        print "+encodeURI(\"$dateRangeParameters\")" if defined $dateRangeParameters;
        print ";\n";
        print "customizeViewedMeasurementsHtml=\"<font size=-1><a href=\"+customizeMeasurementsURL+\">"
          . "Here</a> is a link that only includes the \"+selectedMeasurements.length+\" selected measurements</font>\";\n";
        print "};\n";
        print "};\n";

        print "document.getElementById('customizeViewedMeasurementsDiv').innerHTML=customizeViewedMeasurementsHtml;\n";
    }

    if (@relatedMeasurementObjects) {

        print "var additionalMeasurements=[];\n";

        print "var numRelatedMeasurements = relatedMeasurements.length;\n";
        print "for (var i = 0; i < numRelatedMeasurements; i++) {\n";
        print "if (document.getElementById(relatedMeasurements[i].checkboxID).checked) {\n";
        print "additionalMeasurements.push(relatedMeasurements[i].measurementName);\n";
        print "}\n";
        print "}\n";

        print "var customizeRelatedMeasurementsHtml=\"<font size=-1><b>Related measurements</b></font>\";\n";

        print "if (additionalMeasurements.length>0){\n";
        print "selectedMeasurements.push(additionalMeasurements);\n";
        print "var customizeMeasurementsURL=\"https://hecate.hakai.org"
          . getBaseURL($dataTable)
          . "\"+\"&measurements=\"+selectedMeasurements.join()";
        print "+encodeURI(\"$dateRangeParameters\")" if defined $dateRangeParameters;
        print ";\n";
        print
"customizeRelatedMeasurementsHtml=\"<font size=-1>Click <a href=\"+customizeMeasurementsURL+\">here</a> to add measurements selected below</font>\";\n";
        print "};";

        print "document.getElementById('customizeAddedMeasurementsDiv').innerHTML=customizeRelatedMeasurementsHtml;\n";
    }

    print "dataGraph.draw();\n";

    print "}\n";
}

print "</script>\n";

print "</head>\n";
print "<body>\n";

print $bsBody;    # Add in Bootstrap stuff

printNavigationHeader("Custom View ($firstMeasurementTime PST to $lastMeasurementTime PST)");

if ( $pgName eq "hakaidev" ) {

    my $productionURL = $currentURL;
    $productionURL =~ s/&dev//;
    $productionURL =~ s/&test//;

    print "<div class=\"alert\">\n";
    print "You are viewing data from the test database\n";
    print "<br><br>Please click <a href=\"$productionURL\">here</a> to switch to the production database\n";
    print "</div>\n";
}

# Include a list of measurements *above* the graph, with the option to toggel them on/off
if ( $viewableMeasurements > 1 ) {
    my @measurementList;
    foreach my $viewedMeasurement (@viewedMeasurementList) {

        my $viewedMeasurementKey = $viewedMeasurement->{measurementKey};
        next
          unless exists $currentMeasurements{$viewedMeasurementKey}{columnNum}
          && exists $currentMeasurements{$viewedMeasurementKey}{measurementsFound};

        my $measurementLink = $viewedMeasurement->{measurementCheckbox} . $viewedMeasurement->{columnName};

        push @measurementList, $measurementLink;
    }

    print
"<div id=\"customizeViewedMeasurementsDiv\"><font size=\"-1\">You can now toggle on/off viewed measurements</font></div>";

    print "<font size=\"-1\"><b>Viewed measurements:</b> " . join( " ", @measurementList ) . "</font>\n";
}

if ( $viewableMeasurements < scalar(@viewedMeasurementList) ) {
    my @missingMeasurementList;
    foreach my $viewedMeasurement (@viewedMeasurementList) {

        my $viewedMeasurementKey = $viewedMeasurement->{measurementKey};
        next
          if exists $currentMeasurements{$viewedMeasurementKey}{columnNum}
          && exists $currentMeasurements{$viewedMeasurementKey}{measurementsFound};

        push @missingMeasurementList, $viewedMeasurement->{columnName};
    }

    print "<br><font size=\"-1\"><b>Missing measurements (no data available):</b> "
      . join( " ", @missingMeasurementList )
      . "</font>\n";
}

print "<div id=\"dashboardDiv\">\n";
print "<div id=\"dataGraphDiv\" style='height: 400px;'></div>\n";
print "<div id=\"dataControlDiv\" style='height: 50px;'></div>\n";
print "</div>\n";

print "<hr><table width=\"100%\"><tr><td align=\"center\"><font size=\"-1\">\n";

print "<b>View: </b><a href=\"" . getBaseURL( $dataTable, $visualizedMeasurements ) . "\">all</a>";

#print " <a href=\"" . getBaseURL( $dataTable, $visualizedMeasurements, "&dateRange=ytd" ) . "\">this year</a>"
#  unless defined $dateRange && $dateRange eq "ytd";
#print " <a href=\"" . getBaseURL( $dataTable, $visualizedMeasurements, "&dateRange=mtd" ) . "\">this month</a>"
#  unless defined $dateRange && $dateRange eq "mtd";
print " <a href=\""
  . getBaseURL( $dataTable, $visualizedMeasurements, "&dateRange=last12weeks" )
  . "\">last 12 weeks</a>"
  unless defined $dateRange && $dateRange eq "last12weeks";
print " <a href=\""
  . getBaseURL( $dataTable, $visualizedMeasurements, "&dateRange=last4weeks" )
  . "\">last 4 weeks</a>"
  unless defined $dateRange && $dateRange eq "last4weeks";
print " <a href=\"" . getBaseURL( $dataTable, $visualizedMeasurements, "&dateRange=last1week" ) . "\">last week</a>"
  unless defined $dateRange && $dateRange eq "last1week";

if ($firstDatabaseMeasurementTime) {
    my $firstMeasurementTimestamp = DateTime->new(
        year      => ( substr $firstDatabaseMeasurementTime, 0, 4 ),
        month     => ( substr $firstDatabaseMeasurementTime, 5, 2 ),
        day       => 1,
        time_zone => 'UTC'
    );

    my $currentMeasurementTimestamp = DateTime->now( time_zone => 'UTC' );

    my $lastYear  = $currentMeasurementTimestamp->year();
    my $lastMonth = $currentMeasurementTimestamp->month();
    while ( $firstMeasurementTimestamp->year() <= $lastYear ) {

        my $currentYear = $firstMeasurementTimestamp->year();

        if ( defined $dateRange && $dateRange eq $currentYear ) {
            print "<br>$currentYear: ";
        }
        else {
            print "<br><a href=\""
              . getBaseURL( $dataTable, $visualizedMeasurements, "&dateRange=$currentYear" )
              . "\">$currentYear</a>: ";
        }

        while ($firstMeasurementTimestamp->year() < $lastYear
            || $firstMeasurementTimestamp->month() <= $lastMonth )
        {
            my $currentYearMonth = substr $firstMeasurementTimestamp->ymd, 0, 7;
            my $currentYearMonthString = $firstMeasurementTimestamp->month_abbr();

            if ( defined $dateRange && $dateRange eq $currentYearMonth ) {
                print " $currentYearMonthString";
            }
            else {
                print " <a href=\""
                  . getBaseURL( $dataTable, $visualizedMeasurements, "&dateRange=$currentYearMonth" )
                  . "\">$currentYearMonthString</a>";
            }

            last if $firstMeasurementTimestamp->month() == 12;
            $firstMeasurementTimestamp->set_month( $firstMeasurementTimestamp->month() + 1 );
        }

        $firstMeasurementTimestamp->set_year( $currentYear + 1 );
        $firstMeasurementTimestamp->set_month(1);    # Reset to January
    }
}

print "<br><b>Download: </b> <a href=\"" . getBaseURL( $dataTable, $visualizedMeasurements ) . "&download\">all</a>";
print " <a href=\""
  . getBaseURL( $dataTable, $visualizedMeasurements, $dateRangeParameters )
  . "&download\">$firstMeasurementTime to $lastMeasurementTime</a>";
print "</font>\n";

# Show current range, with options to download and view
print "</td><td align=\"center\"><font size=\"-1\">\n";

if ($shiftDays) {
    print "<a href=\""
      . getBaseURL( $dataTable, $visualizedMeasurements,
        "&firstMeasurementTime=$twoShiftsBeforeStart&lastMeasurementTime=$twoShiftsBeforeEnd" )
      . "\">&lt= "
      . ( 2 * $shiftDays )
      . " days</a>";
    print " <a href=\""
      . getBaseURL( $dataTable, $visualizedMeasurements,
        "&firstMeasurementTime=$oneShiftBeforeStart&lastMeasurementTime=$oneShiftBeforeEnd" )
      . "\">&lt;= $shiftDays days</a>";
    print " <a href=\""
      . getBaseURL( $dataTable, $visualizedMeasurements,
        "&firstMeasurementTime=$oneShiftBeforeStart&lastMeasurementTime=$oneShiftAfterEnd" )
      . "\">&lt= $shiftDays days =&gt;</a>";
    print " <a href=\""
      . getBaseURL( $dataTable, $visualizedMeasurements,
        "&firstMeasurementTime=$oneShiftAfterStart&lastMeasurementTime=$oneShiftAfterEnd" )
      . "\">$shiftDays days =&gt;</a>";
    print " <a href=\""
      . getBaseURL( $dataTable, $visualizedMeasurements,
        "&firstMeasurementTime=$twoShiftsAfterStart&lastMeasurementTime=$twoShiftsAfterEnd" )
      . "\">"
      . ( 2 * $shiftDays )
      . " days =&gt;</a>";
}

print "<div id=\"customRangeDiv\">Viewing $firstMeasurementTime to $lastMeasurementTime</div>";

print "<br>";

print "<br><a href=\""
  . getBaseURL( $dataTable, $visualizedMeasurements, $dateRangeParameters )
  . "&customize\">customize</a> (graph/add <b>any</b> measurement)";

print "</td><td align=\"center\"><font size=\"-1\">\n";

print "<b>Frequency:</b> " . join( ", ", @otherSampleIntervalLinks ) . " measurements"
  if @otherSampleIntervalLinks;

if ( $dataTable eq $oneHourTable || $dataTable eq $fiveMinuteTable ) {
    if ( defined $originalFlag ) {
        my $processedURL = $currentURL;
        $processedURL =~ s/&original//;

        print "<br><b>NOTE:</b>Viewing <b>original/raw</b> measurements,<br>view <a href=\""
          . $processedURL
          . "\">derived/processed measurements</a>";
    }
    else {
        print
"<br>Viewing <b>derived/processed</b> measurements,<br>view <a href=\"$currentURL&original\">original/raw measurements</a>";
    }
}

if ( !defined $originalFlag ) {
    print "<br><b>Annotations:</b> ";

    if ( exists $myParameter{noFlags} ) {
        my $withFlagsURL = $currentURL;
        $withFlagsURL =~ s/&noFlags//;

        print "<b>hidden</b> <a href=\"$withFlagsURL\">show</a>";
    }
    else {
        print "<a href=\"$currentURL&noFlags\">hide</a> shown";
    }
}

print "</font></td></tr></table>\n";

print "<hr><div id=\"customizeAddedMeasurementsDiv\"><font size=\"-1\">"
  . "<b>Related measurements</b></font></div>"
  . "<font size=\"-1\">$relatedMeasurementDetails</font></div>\n"
  if defined $relatedMeasurementDetails;

print "<hr><div id=\"measurementSummary\">\n";

print "<table class=\"myTable\"><thead>";
print "<td>Measurement</td>";
print "<td># found</td>";
print "<td># missing</td>";
print "<td>Quality level</td>";
print "<td># QC flagged</td>";
print "<td>Measurement calculation</td>";
print "</thead>\n";

foreach my $baseMeasurementName ( sort keys %baseMeasurements ) {
    foreach my $measurementKey ( sort keys %{ $baseMeasurements{$baseMeasurementName}{measurementsKeys} } ) {

        my $measurementsFound = $currentMeasurements{$measurementKey}{measurementsFound};
        $measurementsFound = 0 unless $measurementsFound;
        my $measurementsMissing = $currentMeasurements{$measurementKey}{measurementsMissing};
        $measurementsMissing = 0 unless $measurementsMissing;
        my $measurementCalculation = $currentMeasurements{$measurementKey}{measurementCalculation};
        $measurementCalculation = "" unless $measurementCalculation;

        my $baseMeasurementName = $currentMeasurements{$measurementKey}{baseMeasurementName};

        my $qlMeasurementKey = $baseMeasurements{$baseMeasurementName}{qlMeasurementKey};
        my $qcMeasurementKey = $baseMeasurements{$baseMeasurementName}{qcMeasurementKey};

        my $flaggedMeasurements;
        $flaggedMeasurements = $currentMeasurements{$qcMeasurementKey}{flaggedMeasurements}
          if defined $qcMeasurementKey;
        $flaggedMeasurements = 0 unless defined $flaggedMeasurements;

        my @qualityLevels;
        if ( defined $qlMeasurementKey && exists $currentMeasurements{$qlMeasurementKey}{qualityLevelCounts} ) {
            foreach $qualityLevel ( 0 .. 3 ) {
                next unless exists $currentMeasurements{$qlMeasurementKey}{qualityLevelCounts}{$qualityLevel};

                my $qualityLevelCount = $currentMeasurements{$qlMeasurementKey}{qualityLevelCounts}{$qualityLevel};

                if ( $numIntervals == $qualityLevelCount ) {
                    push @qualityLevels, $qualityLevel;
                }
                else {
                    push @qualityLevels, $qualityLevel . " ($qualityLevelCount)";
                }
            }
        }

        my $qualityLevelSummary = "";
        $qualityLevelSummary = join( ", ", @qualityLevels ) if @qualityLevels;

        my $measurementLink =
            "<a href=\""
          . getBaseURL( $dataTable, $currentMeasurements{$measurementKey}{fullMeasurementName}, $dateRangeParameters )
          . "\">"
          . $currentMeasurements{$measurementKey}{columnName} . "</a>";

        print
"<tr><td>$measurementLink</td><td>$measurementsFound</td><td>$measurementsMissing</td><td>$qualityLevelSummary</td><td>$flaggedMeasurements</td><td>$measurementCalculation</td></tr>\n";
    }
}

print "</table>\n";

print "</div>\n";

print "</body>\n";
print "</html>\n";

# Log and audit all access
auditDataAccess();

#
# End of viewsndata.pl
#