#!/usr/bin/perl
################################################################################
# Copyright (c) 2014, Tula Foundation, and individual contributors.
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
# This script is used to publish data from the Hakai sensor network.  This tool
# is based on the original process-LoggerNet-data.pl tool initiated in the fall
# of 2013.
#
# Created by: Ray Brunsting (ray@tula.org)
# Created on: December 1, 2014
################################################################################

use warnings;
use Log::Log4perl;
use Log::Log4perl::Layout;
use Log::Log4perl::Level;
use Getopt::Long;
use Spreadsheet::XLSX;
use Scalar::Util qw{ looks_like_number };
use Statistics::Lite qw{sum mean median min max};
use File::Find ();
use File::Basename;
use DateTime;
use DateTime::Format::Excel;
use DBI;
use JSON;
use Encode;
use utf8;
use Email::Send;
use Email::Send::Gmail;
use Email::Simple::Creator;
use Email::MIME;

################################################################################
# Force screen output to be UTF8. This was documented at
# http://stackoverflow.com/questions/627661/how-can-i-output-utf-8-from-perl
binmode( STDOUT, ":utf8" );

################################################################################
# Parse the command line and determine all of the input parameters
my (
    $specificationFilename, $refresh,              $daysToRefresh, $syncDB,
    $compareDB,             $rebuildDB,            $pgHost,        $pgDB,
    $sourceDB,              $destinationDB,        $pgUser,        $pgPassword,
    $notificationUser,      $notificationPassword, $quiet,         $error,
    $debug,                 $trace,                $logFile,       $help
);

#-- prints usage if there is an unknown parameter or help option is passed
usage()
  if (
    !GetOptions(
        'h|?|help'               => \$help,
        'specification=s'        => \$specificationFilename,
        'refresh'                => \$refresh,
        'daysToRefresh=s'        => \$daysToRefresh,
        'compareDB'              => \$compareDB,
        'syncDB'                 => \$syncDB,
        'rebuildDB'              => \$rebuildDB,
        'pgHost=s'               => \$pgHost,
        'pgDB=s'                 => \$pgDB,
        'sourceDB=s'             => \$sourceDB,
        'destinationDB=s'        => \$destinationDB,
        'pgUser=s'               => \$pgUser,
        'pgPassword=s'           => \$pgPassword,
        'notificationUser=s'     => \$notificationUser,
        'notificationPassword=s' => \$notificationPassword,
        'q|quiet'                => \$quiet,
        'error'                  => \$error,
        'debug'                  => \$debug,
        'trace'                  => \$trace,
        'logFile=s'              => \$logFile
    )
    or defined $help
    or !defined $specificationFilename
    or !defined $pgUser
    or !defined $pgPassword
  );

sub usage {
    print "Unknown option: @_\n" if (@_);
    print "usage: program [-h|?]\n";
    print "\t-specification         <export specifications file>]\n";
    print "\t[-refresh]             <- refresh (replace) all output files ->\n";
    print "\t[-daysToRefresh        <number of days to refresh (default=30)>\n";
    print "\t[-compareDB]           <- compare data to database ->\n";
    print "\t[-syncDB]              <- synchronize data to database ->\n";
    print "\t[-rebuildDB]           <- delete and rebuild data tables ->\n";
    print "\t[-pgHost               <postgreSQL host>]\n";
    print "\t[-pgDB                 <postgreSQL database>]\n";
    print "\t[-sourceDB             <source database>]\n";
    print "\t[-destinationDB        <destination database>]\n";
    print "\t-pgUser                <postgreSQL user>\n";
    print "\t-pgPassword            <postgreSQL password>\n";
    print "\t[-notificationUser     <notification user>]\n";
    print "\t[-notificationPassword <notification password>]\n";
    print "\t[-quiet]               <- don't send email notifications ->\n";
    print "\t[-error]\n";
    print "\t[-debug]\n";
    print "\t[-trace]\n";
    print "\t[-logFile           <log file>]\n";
    exit;
}

################################################################################
# Initialize the logger
my $log       = Log::Log4perl->get_logger("");
my $logLayout = Log::Log4perl::Layout::PatternLayout->new("%d %m%n");
if ( defined $logFile ) {
    print localtime() . " Logging to $logFile\n";

    my $logFileAppender = Log::Log4perl::Appender->new(
        "Log::Log4perl::Appender::File",
        name     => "filelog",
        filename => $logFile
    );
    $logFileAppender->layout($logLayout);
    $log->add_appender($logFileAppender);
}
else {
    my $screenLogAppender = Log::Log4perl::Appender->new( "Log::Log4perl::Appender::Screen", name => "screenlog" );
    $screenLogAppender->layout($logLayout);
    $log->add_appender($screenLogAppender);
}

$log->level($INFO);
if ( defined $trace ) {
    $log->info("Log level set to TRACE");
    $log->level($TRACE);
}
elsif ( defined $debug ) {
    $log->info("Log level set to DEBUG");
    $log->level($DEBUG);
}
elsif ( defined $error ) {
    $log->info("Log level set to ERROR");
    $log->level($ERROR);
}
else {
    $log->info("Log level set to INFO");
    $log->level($INFO);
}

################################################################################
# Globals, contannts, defaults
my $oneMinuteTable   = "1minuteSamples";
my $fiveMinuteTable  = "5minuteSamples";
my $oneHourTable     = "1hourSamples";
my $oneDayTable      = "1daySamples";
my $diagnosticsTable = "Diagnostics";
$pgHost        = "db.hakai.org" unless defined $pgHost;
$pgDB          = "hakaidev"     unless defined $pgDB;
$sourceDB      = $pgDB          unless defined $sourceDB;
$destinationDB = $pgDB          unless defined $destinationDB;

################################################################################
# General utility functions
sub log10 {
    my $n = shift;
    return log($n) / log(10);
}

################################################################################
# Utility function to help parse cell contents from an xlsx document
sub getCellValue {
    my ( $worksheet, $row, $col ) = @_;

    my $cell = $worksheet->get_cell( $row, $col );
    return undef unless defined $cell;

    my $cellValue = decode( 'utf8', $cell->unformatted() );
    $cellValue =~ s/&amp;/&/g;
    $cellValue =~ s/&gt;/>/g;
    $cellValue =~ s/&lt;/</g;
    $cellValue =~ s/&quot;/"/g;
    $cellValue =~ s/&apos;/'/g;
    $cellValue =~ s/_x000D_/\n/g;

    return $cellValue;
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
# Create and populate data structures to more efficiently handle sample times
my $minRefreshDays = $daysToRefresh;
$minRefreshDays = 60 unless defined $minRefreshDays && $minRefreshDays > 0;
$minRefreshDays += 4;    # Include an initial 4 days to support accumulated/aggregated data
my $dailySamplingTimes = 0;
my @dailySamplingTime;
my %dailySamplingTimeIndex;
my $hourlySamplingTimes = 0;
my @hourlySamplingTime;
my %hourlySamplingTimeIndex;
my $fiveMinSamplingTimes = 0;
my @fiveMinSamplingTime;
my %fiveMinSamplingTimeIndex;
my $oneMinSamplingTimes = 0;
my @oneMinSamplingTime;
my %oneMinSamplingTimeIndex;
{

    my $currentSamplingTime;
    if ( defined $refresh ) {
        $currentSamplingTime = DateTime->new(
            year      => 2012,
            month     => 10,
            day       => 1,
            hour      => 0,
            minute    => 0,
            second    => 0,
            time_zone => 'UTC'
        );
    }
    else {
        $currentSamplingTime = DateTime->today( time_zone => 'UTC' );
        $currentSamplingTime->subtract( days => $minRefreshDays );
    }

    $log->info( "Caching samplings dates and times starting on " . $currentSamplingTime->ymd );

    my $lastSamplingTime = DateTime->today( time_zone => 'UTC' );
    $lastSamplingTime->add( days => 1 );

    my $lastSamplingDate = $lastSamplingTime->ymd;

    my @hourStrings;
    foreach my $hourNum ( 0 .. 23 ) {
        if ( $hourNum < 10 ) {
            push @hourStrings, "0$hourNum";
        }
        else {
            push @hourStrings, "$hourNum";
        }
    }

    my @fiveMinuteStrings;
    for ( my $minuteNum = 0 ; $minuteNum <= 55 ; $minuteNum += 5 ) {
        if ( $minuteNum < 10 ) {
            push @fiveMinuteStrings, "0$minuteNum";
        }
        else {
            push @fiveMinuteStrings, "$minuteNum";
        }
    }

    my @oneMinuteStrings;
    for ( my $minuteNum = 0 ; $minuteNum <= 59 ; $minuteNum++ ) {
        if ( $minuteNum < 10 ) {
            push @oneMinuteStrings, "0$minuteNum";
        }
        else {
            push @oneMinuteStrings, "$minuteNum";
        }
    }

    while (1) {
        my $currentSamplingDate = $currentSamplingTime->ymd;
        last if $currentSamplingDate gt $lastSamplingDate;

        my $dailySamplingTime = "$currentSamplingDate 00:00:00";
        $dailySamplingTimeIndex{$dailySamplingTime} = $dailySamplingTimes;
        $dailySamplingTime[ $dailySamplingTimes++ ] = $dailySamplingTime;

        foreach my $hourString (@hourStrings) {
            my $hourlySamplingTime = "$currentSamplingDate $hourString:00:00";
            $hourlySamplingTimeIndex{$hourlySamplingTime} = $hourlySamplingTimes;
            $hourlySamplingTime[ $hourlySamplingTimes++ ] = $hourlySamplingTime;

            foreach my $minuteString (@fiveMinuteStrings) {
                my $fiveMinSamplingTime = "$currentSamplingDate $hourString:$minuteString:00";
                $fiveMinSamplingTimeIndex{$fiveMinSamplingTime} = $fiveMinSamplingTimes;
                $fiveMinSamplingTime[ $fiveMinSamplingTimes++ ] = $fiveMinSamplingTime;
            }

            foreach my $minuteString (@oneMinuteStrings) {
                my $oneMinSamplingTime = "$currentSamplingDate $hourString:$minuteString:00";
                $oneMinSamplingTimeIndex{$oneMinSamplingTime} = $oneMinSamplingTimes;
                $oneMinSamplingTime[ $oneMinSamplingTimes++ ] = $oneMinSamplingTime;
            }
        }

        $currentSamplingTime->add( days => 1 );
    }
}
$log->info( "Cached $dailySamplingTimes daily, $hourlySamplingTimes hourly"
      . ", $fiveMinSamplingTimes five minute sampling times"
      . ", and $oneMinSamplingTimes one minute sampling times" );

################################################################################
# Read in the current list of data files from the database
my %dbMeasurements;
if ( !$rebuildDB ) {
    my $dbh = DBI->connect( "DBI:Pg:dbname=$destinationDB;host=$pgHost", $pgUser, $pgPassword, { 'RaiseError' => 1 } );

    my $measurementSth = $dbh->prepare(
"SELECT sensor_node,data_table,measurement_name,measurement_calculation,first_measurement_time at time zone 'PST'"
          . ",database_table,database_column,import_flag,export_flag"
          . ",standard_name,display_name,measurement_type,measurement_function,measurement_units"
          . ",elevation,sensor_type,serial_number,sensor_description,sensor_documentation,sensor_comments"
          . " FROM sn.measurements" );
    $measurementSth->execute();
    my (
        $sensorNode,           $dataTable,         $measurementName,     $measurementCalculation,
        $firstMeasurementTime, $databaseTable,     $databaseColumn,      $importFlag,
        $exportFlag,           $standardName,      $displayName,         $measurementType,
        $measurementFunction,  $measurementUnits,  $elevation,           $sensorType,
        $serialNumber,         $sensorDescription, $sensorDocumentation, $sensorComments
    );

    $measurementSth->bind_columns(
        \$sensorNode,           \$dataTable,         \$measurementName,     \$measurementCalculation,
        \$firstMeasurementTime, \$databaseTable,     \$databaseColumn,      \$importFlag,
        \$exportFlag,           \$standardName,      \$displayName,         \$measurementType,
        \$measurementFunction,  \$measurementUnits,  \$elevation,           \$sensorType,
        \$serialNumber,         \$sensorDescription, \$sensorDocumentation, \$sensorComments
    );

    while ( $measurementSth->fetch ) {

        my $measurementKey = lc "$sensorNode.$dataTable.$measurementName";

        $displayName = decode( 'utf8', $displayName ) if defined $displayName && !utf8::is_utf8($displayName);
        $measurementUnits = decode( 'utf8', $measurementUnits )
          if defined $measurementUnits && !utf8::is_utf8($measurementUnits);

        $dbMeasurements{$measurementKey}{measurementCalculation} = $measurementCalculation;
        $dbMeasurements{$measurementKey}{firstMeasurementTime}   = $firstMeasurementTime;
        $dbMeasurements{$measurementKey}{databaseTable}          = $databaseTable;
        $dbMeasurements{$measurementKey}{databaseColumn}         = $databaseColumn;
        $dbMeasurements{$measurementKey}{importFlag}             = $importFlag;
        $dbMeasurements{$measurementKey}{exportFlag}             = $exportFlag;
        $dbMeasurements{$measurementKey}{standardName}           = $standardName;
        $dbMeasurements{$measurementKey}{displayName}            = $displayName;
        $dbMeasurements{$measurementKey}{measurementType}        = $measurementType;
        $dbMeasurements{$measurementKey}{function}               = $measurementFunction;
        $dbMeasurements{$measurementKey}{units}                  = $measurementUnits;
        $dbMeasurements{$measurementKey}{elevation}              = $elevation;
        $dbMeasurements{$measurementKey}{sensorType}             = $sensorType;
        $dbMeasurements{$measurementKey}{serialNumber}           = $serialNumber;
        $dbMeasurements{$measurementKey}{sensorDescription}      = $sensorDescription;
        $dbMeasurements{$measurementKey}{sensorDocumentation}    = $sensorDocumentation;
        $dbMeasurements{$measurementKey}{comments}               = $sensorComments;
    }
    $log->info( "Found " . scalar( keys %dbMeasurements ) . " measurements in the database" );
}

################################################################################
# Read and parse the specification file
my $specificationWorkbook = Spreadsheet::XLSX->new($specificationFilename);
die "Problems reading and parsing specification file $specificationFilename"
  unless defined $specificationWorkbook;

################################################################################
# Track all measurement tables and individual measurements
my %allTables;
my @allMeasurements;
my %allMeasurementIndexes;

sub addMeasurement {
    my (
        $sensorNode,          $dataTable,       $measurementName,        $standardName,
        $displayName,         $measurementType, $function,               $units,
        $minimumValue,        $maximumValue,    $linkedMeasurementsName, $clipRange,
        $aggregateFiveMin,    $storeInDB,       $hasQC,                  $deploymentTime,
        $elevation,           $sensorType,      $serialNumber,           $sensorDescription,
        $sensorDocumentation, $comments
    ) = @_;

    my $measurementIndex = scalar(@allMeasurements);

    # Assume a default display name unless it has been explicitly configured
    $displayName = $measurementName unless defined $displayName && length $displayName;

    my $fullTableName       = "$sensorNode.$dataTable";
    my $fullMeasurementName = "$fullTableName.$measurementName";
    my $measurementKey      = lc $fullMeasurementName;
    if ( exists $allMeasurementIndexes{$measurementKey} ) {
        $log->debug("Ignoring duplicate specificaion of $fullMeasurementName");
        return;
    }

    my $databaseTable;
    my $databaseColumn;
    if ( exists $dbMeasurements{$measurementKey} ) {
        $databaseTable  = $dbMeasurements{$measurementKey}{databaseTable};
        $databaseColumn = $dbMeasurements{$measurementKey}{databaseColumn};
    }
    else {
        # Generate a database table name
        $databaseTable = "${sensorNode}_${dataTable}";
        $databaseTable =~ s/BoL/Bol/;          # Burkolator
        $databaseTable =~ s/SeaFET/Seafet/;    # SeaFET
        $databaseTable =~ s/([a-z])([A-Z])/$1_$2/g;
        $databaseTable = lc $databaseTable;
        $databaseTable =~ s/samples$//;
        $databaseTable =~ s/_$//;

        # Generate a database column name
        $databaseColumn = $measurementName;
        $databaseColumn =~ s/([a-z])([A-Z])/$1_$2/g;
        $databaseColumn =~ s/([A-Z][A-Z])([a-z])/$1_$2/g;
        $databaseColumn = lc $databaseColumn;
        $databaseColumn =~ s/^12/twelve_/g;
        $databaseColumn =~ s/^24/twenty4/g;
        $databaseColumn =~ s/^36/thirty6/g;
        $databaseColumn =~ s/^48/forty8/g;
        $databaseColumn =~ s/^72/seventy2/g;
        $databaseColumn =~ s/^1/one_/g;
    }

    if ( !defined $allTables{$sensorNode} || !defined $allTables{$sensorNode}{$dataTable} ) {
        $allTables{$sensorNode}{$dataTable}{sensorNode}    = $sensorNode;
        $allTables{$sensorNode}{$dataTable}{dataTable}     = $dataTable;
        $allTables{$sensorNode}{$dataTable}{databaseTable} = $databaseTable;

        if ( $dataTable eq $oneMinuteTable ) {
            $allTables{$sensorNode}{$dataTable}{sampleInterval} = 1;
        }
        elsif ( $dataTable eq $fiveMinuteTable ) {
            $allTables{$sensorNode}{$dataTable}{sampleInterval} = 5;
        }
        elsif ( $dataTable eq $oneHourTable || $dataTable eq $diagnosticsTable ) {
            $allTables{$sensorNode}{$dataTable}{sampleInterval} = 60;
        }
        elsif ( $dataTable eq $oneDayTable ) {
            $allTables{$sensorNode}{$dataTable}{sampleInterval} = 24 * 60;
        }

        $allMeasurements[$measurementIndex]{measurementIndex} = $measurementIndex;
        $allMeasurements[$measurementIndex]{sensorNode}       = $sensorNode;
        $allMeasurements[$measurementIndex]{dataTable}        = $dataTable;
        $allMeasurements[$measurementIndex]{measurementName}  = "RECORD";
        $allMeasurements[$measurementIndex]{measurementType}  = "primary";
        $allMeasurements[$measurementIndex]{units}            = "RN";
        $allMeasurements[$measurementIndex]{databaseTable}    = $databaseTable;
        $allMeasurements[$measurementIndex]{databaseColumn}   = "record";
        $allMeasurements[$measurementIndex]{importFlag}       = 1;
        $allMeasurements[$measurementIndex]{exportFlag}       = 1;
        $allMeasurements[$measurementIndex]{aggregateFiveMin} = 0;

        $allMeasurementIndexes{ lc "$fullTableName.RECORD" } = $measurementIndex;
        $measurementIndex++;
    }

    my ( $qlMeasurementIndex, $qcMeasurementIndex, $uqlMeasurementIndex );
    if ( defined $hasQC && uc $hasQC eq "YES" ) {
        my $qlMeasurementName     = getBaseName($measurementName) . "_QL";
        my $fullQLMeasurementName = "$fullTableName.$qlMeasurementName";
        my $qlMeasurementKey      = lc $fullQLMeasurementName;
        $qlMeasurementIndex = $allMeasurementIndexes{$qlMeasurementKey};
        if ( !defined $qlMeasurementIndex ) {
            $qlMeasurementIndex                                        = $measurementIndex++;
            $allMeasurements[$qlMeasurementIndex]{measurementIndex}    = $qlMeasurementIndex;
            $allMeasurements[$qlMeasurementIndex]{sensorNode}          = $sensorNode;
            $allMeasurements[$qlMeasurementIndex]{dataTable}           = $dataTable;
            $allMeasurements[$qlMeasurementIndex]{measurementName}     = $qlMeasurementName;
            $allMeasurements[$qlMeasurementIndex]{displayName}         = getBaseName($displayName) . " Q level";
            $allMeasurements[$qlMeasurementIndex]{fullMeasurementName} = "$sensorNode.$dataTable.$qlMeasurementName";
            $allMeasurements[$qlMeasurementIndex]{measurementType}     = "Quality level";
            $allMeasurements[$qlMeasurementIndex]{units}               = "Quality level";
            $allMeasurements[$qlMeasurementIndex]{databaseTable}       = $databaseTable;
            $allMeasurements[$qlMeasurementIndex]{databaseColumn}      = getBaseName($databaseColumn) . "_ql";
            $allMeasurements[$qlMeasurementIndex]{importFlag}          = 1;
            $allMeasurements[$qlMeasurementIndex]{exportFlag}          = 1;
            $allMeasurements[$qlMeasurementIndex]{aggregateFiveMin}    = 0;
            $allMeasurements[$qlMeasurementIndex]{qcField}             = 1;
            $allMeasurements[$qlMeasurementIndex]{qlField}             = 1;
            $allMeasurements[$qlMeasurementIndex]{deploymentTime} = $deploymentTime if defined $deploymentTime;
            $allMeasurementIndexes{$qlMeasurementKey} = $qlMeasurementIndex;
        }

        my $qcMeasurementName     = getBaseName($measurementName) . "_QC";
        my $fullQCMeasurementName = "$fullTableName.$qcMeasurementName";
        my $qcMeasurementKey      = lc $fullQCMeasurementName;
        $qcMeasurementIndex = $allMeasurementIndexes{$qcMeasurementKey};
        if ( !defined $qcMeasurementIndex ) {
            $qcMeasurementIndex                                        = $measurementIndex++;
            $allMeasurements[$qcMeasurementIndex]{measurementIndex}    = $qcMeasurementIndex;
            $allMeasurements[$qcMeasurementIndex]{sensorNode}          = $sensorNode;
            $allMeasurements[$qcMeasurementIndex]{dataTable}           = $dataTable;
            $allMeasurements[$qcMeasurementIndex]{measurementName}     = $qcMeasurementName;
            $allMeasurements[$qcMeasurementIndex]{displayName}         = getBaseName($displayName) . " Q flags";
            $allMeasurements[$qcMeasurementIndex]{fullMeasurementName} = "$sensorNode.$dataTable.$qcMeasurementName";
            $allMeasurements[$qcMeasurementIndex]{measurementType}     = "Quality flags";
            $allMeasurements[$qcMeasurementIndex]{units}               = "Quality flags";
            $allMeasurements[$qcMeasurementIndex]{databaseTable}       = $databaseTable;
            $allMeasurements[$qcMeasurementIndex]{databaseColumn}      = getBaseName($databaseColumn) . "_qc";
            $allMeasurements[$qcMeasurementIndex]{importFlag}          = 1;
            $allMeasurements[$qcMeasurementIndex]{exportFlag}          = 1;
            $allMeasurements[$qcMeasurementIndex]{aggregateFiveMin}    = 0;
            $allMeasurements[$qcMeasurementIndex]{qcField}             = 1;
            $allMeasurements[$qcMeasurementIndex]{qcFlag}              = 1;
            $allMeasurements[$qcMeasurementIndex]{deploymentTime} = $deploymentTime if defined $deploymentTime;
            $allMeasurementIndexes{$qcMeasurementKey} = $qcMeasurementIndex;
        }

        my $uqlMeasurementName     = getBaseName($measurementName) . "_UQL";
        my $fullUQLMeasurementName = "$fullTableName.$uqlMeasurementName";
        my $uqlMeasurementKey      = lc $fullUQLMeasurementName;
        $uqlMeasurementIndex = $allMeasurementIndexes{$uqlMeasurementKey};
        if ( !defined $uqlMeasurementIndex ) {
            $uqlMeasurementIndex                                        = $measurementIndex++;
            $allMeasurements[$uqlMeasurementIndex]{measurementIndex}    = $uqlMeasurementIndex;
            $allMeasurements[$uqlMeasurementIndex]{sensorNode}          = $sensorNode;
            $allMeasurements[$uqlMeasurementIndex]{dataTable}           = $dataTable;
            $allMeasurements[$uqlMeasurementIndex]{measurementName}     = $uqlMeasurementName;
            $allMeasurements[$uqlMeasurementIndex]{displayName}         = getBaseName($displayName) . " UNESCO Q level";
            $allMeasurements[$uqlMeasurementIndex]{fullMeasurementName} = "$sensorNode.$dataTable.$uqlMeasurementName";
            $allMeasurements[$uqlMeasurementIndex]{measurementType}     = "UNESCO QC";
            $allMeasurements[$uqlMeasurementIndex]{units}               = "UNESCO QC";
            $allMeasurements[$uqlMeasurementIndex]{databaseTable}       = $databaseTable;
            $allMeasurements[$uqlMeasurementIndex]{databaseColumn}      = getBaseName($databaseColumn) . "_uql";
            $allMeasurements[$uqlMeasurementIndex]{importFlag}          = 1;
            $allMeasurements[$uqlMeasurementIndex]{exportFlag}          = 1;
            $allMeasurements[$uqlMeasurementIndex]{aggregateFiveMin}    = 0;
            $allMeasurements[$uqlMeasurementIndex]{qcField}             = 1;
            $allMeasurements[$uqlMeasurementIndex]{uqlField}            = 1;
            $allMeasurements[$uqlMeasurementIndex]{deploymentTime} = $deploymentTime if defined $deploymentTime;

            $allMeasurements[$uqlMeasurementIndex]{qlMeasurementIndex} = $qlMeasurementIndex;
            $allMeasurements[$uqlMeasurementIndex]{qcMeasurementIndex} = $qcMeasurementIndex;

            $allMeasurementIndexes{$uqlMeasurementKey} = $uqlMeasurementIndex;
        }
    }

    $allMeasurements[$measurementIndex]{measurementIndex} = $measurementIndex;
    $allMeasurements[$measurementIndex]{sensorNode}       = $sensorNode;
    $allMeasurements[$measurementIndex]{dataTable}        = $dataTable;
    $allMeasurements[$measurementIndex]{measurementName}  = $measurementName;
    $allMeasurements[$measurementIndex]{standardName}     = $standardName if defined $standardName;
    $allMeasurements[$measurementIndex]{displayName}      = $displayName;
    $allMeasurements[$measurementIndex]{measurementType}  = $measurementType;
    $allMeasurements[$measurementIndex]{function}         = $function
      if defined $function;
    $allMeasurements[$measurementIndex]{units} = $units
      if defined $units;
    $allMeasurements[$measurementIndex]{minimumValue} = $minimumValue
      if defined $minimumValue;
    $allMeasurements[$measurementIndex]{maximumValue} = $maximumValue
      if defined $maximumValue;

    $allMeasurements[$measurementIndex]{databaseTable}  = $databaseTable;
    $allMeasurements[$measurementIndex]{databaseColumn} = $databaseColumn;

    if ( exists $dbMeasurements{$measurementKey} ) {
        $allMeasurements[$measurementIndex]{importFlag} = $dbMeasurements{$measurementKey}{importFlag};
        $allMeasurements[$measurementIndex]{exportFlag} = $dbMeasurements{$measurementKey}{exportFlag};
    }
    else {
        # Assume we both import and export all measurements by default
        my $importFlag = 1;
        my $exportFlag = 1;

        # If not storing in the database, neither import nor export the data
        if ( defined $storeInDB && uc $storeInDB eq "NO" ) {
            $importFlag = 0;
            $exportFlag = 0;
        }

        # Exception: exclude 5 minute min and max values for most measurements
        elsif ($dataTable eq $fiveMinuteTable
            && ( index( lc $measurementName, "_min" ) > 0 || index( lc $measurementName, "_max" ) > 0 )
            && index( lc $measurementName, "ecprobe" ) != 0
            && index( lc $measurementName, "pls" ) != 0
            && index( $sensorNode,         "SA_" ) != 0
            && index( $sensorNode,         "BoL" ) < 0
            && $sensorNode ne "TSN4"
            && $sensorNode ne "PruthMooring"
            && $sensorNode ne "QU5_Mooring"
            && $sensorNode ne "WTS693Lake" )
        {
            $importFlag = 0;
            $exportFlag = 0;
        }

        $allMeasurements[$measurementIndex]{importFlag} = $importFlag;
        $allMeasurements[$measurementIndex]{exportFlag} = $exportFlag;
    }

    $allMeasurements[$measurementIndex]{aggregateFiveMin} = 0;
    if ( defined $aggregateFiveMin && uc $aggregateFiveMin eq "YES" ) {
        $allMeasurements[$measurementIndex]{aggregateFiveMin} = 1;
    }

    $allMeasurements[$measurementIndex]{qlMeasurementIndex}  = $qlMeasurementIndex  if defined $qlMeasurementIndex;
    $allMeasurements[$measurementIndex]{qcMeasurementIndex}  = $qcMeasurementIndex  if defined $qcMeasurementIndex;
    $allMeasurements[$measurementIndex]{uqlMeasurementIndex} = $uqlMeasurementIndex if defined $uqlMeasurementIndex;
    $allMeasurements[$measurementIndex]{deploymentTime}      = $deploymentTime      if defined $deploymentTime;
    $allMeasurements[$measurementIndex]{elevation}           = $elevation           if defined $elevation;
    $allMeasurements[$measurementIndex]{sensorType}          = $sensorType          if defined $sensorType;
    $allMeasurements[$measurementIndex]{serialNumber}        = $serialNumber        if defined $serialNumber;
    $allMeasurements[$measurementIndex]{sensorDescription}   = $sensorDescription   if defined $sensorDescription;
    $allMeasurements[$measurementIndex]{sensorDocumentation} = $sensorDocumentation if defined $sensorDocumentation;
    $allMeasurements[$measurementIndex]{comments}            = $comments            if defined $comments;

    if ( length $deploymentTime ) {
        $allMeasurements[$measurementIndex]{clipCalculation} = {
            sensorNode      => $sensorNode,
            tableName       => $dataTable,
            measurementName => $measurementName,
            calculation     => "clip",
            parameters      => "lastTimestamp=" . $deploymentTime
        };
    }

    if (
           index( uc $measurementName, "AIRPRESSURE" ) > 0
        && index( uc $measurementName, "DELTA" ) < 0    # Ignore Delta Air Pressure
        && index( uc $measurementName, "_STD" ) < 0
        && index( uc $measurementName, "_QL" ) < 0
        && index( uc $measurementName, "_UQL" ) < 0
        && index( uc $measurementName, "_QC" ) < 0
      )
    {
        $allMeasurements[$measurementIndex]{airPressureCalculation} = {
            sensorNode          => $sensorNode,
            tableName           => $dataTable,
            measurementName     => $measurementName,
            measurementUnit     => $units,
            measurementFunction => $function,
            calculation         => "normalizeAirPressure"
        };
    }

    if ( length $minimumValue || length $maximumValue ) {

        my $clipRangeBefore;
        my $clipRangeAfter;

        if ( length $clipRange ) {
            ( $clipRangeBefore, $clipRangeAfter ) = split( /,/, $clipRange );
            $clipRangeAfter = $clipRangeBefore unless defined $clipRangeAfter;

            if ( $dataTable eq $oneMinuteTable ) {
                $clipRangeBefore = int( $clipRangeBefore + 0.9 );
                $clipRangeAfter  = int( $clipRangeAfter + 0.9 );
            }
            elsif ( $dataTable eq $fiveMinuteTable ) {
                $clipRangeBefore = int( ( $clipRangeBefore + 4.9 ) / 5.0 );
                $clipRangeAfter  = int( ( $clipRangeAfter + 4.9 ) / 5.0 );
            }
            elsif ( $dataTable eq $oneHourTable ) {
                $clipRangeBefore = int( ( $clipRangeBefore + 59.9 ) / 60.0 );
                $clipRangeAfter  = int( ( $clipRangeAfter + 59.9 ) / 60.0 );
            }
            else {
                $clipRangeBefore = 0;
                $clipRangeAfter  = 0;
            }
        }
        else {
            $clipRangeBefore = 0;
            $clipRangeBefore = 3 if $dataTable eq $fiveMinuteTable;
            $clipRangeBefore = 15 if $dataTable eq $oneMinuteTable;
            $clipRangeAfter  = $clipRangeBefore;
        }

        my $rangeClipParameters = "clipRangeBefore=$clipRangeBefore,clipRangeAfter=$clipRangeAfter";
        $rangeClipParameters .= ",linkedMeasurements=$linkedMeasurementsName" if length $linkedMeasurementsName;
        $rangeClipParameters .= ",minimumValue=" . $minimumValue              if length $minimumValue;
        $rangeClipParameters .= ",maximumValue=" . $maximumValue              if length $maximumValue;

        $allMeasurements[$measurementIndex]{rangeClipCalculation} = {
            sensorNode          => $sensorNode,
            tableName           => $dataTable,
            measurementName     => $measurementName,
            measurementUnit     => $units,
            measurementFunction => $function,
            calculation         => "rangeClip",
            parameters          => $rangeClipParameters
        };
    }

    $allMeasurementIndexes{$measurementKey} = $measurementIndex;
    $measurementIndex++;
}

################################################################################
# Utility function related to measurements
my @allData;

################################################################################
# Utility function to track the first and last timestamp of each measurement,
# and automatically track that measurement and QC flag are referenced
sub updateFirstTimestamp {
    my ( $measurementIndex, $firstTimestamp ) = @_;

    return
      if defined $allMeasurements[$measurementIndex]{firstTimestamp}
      && $allMeasurements[$measurementIndex]{firstTimestamp} le $firstTimestamp;

    my $sensorNode = $allMeasurements[$measurementIndex]{sensorNode};
    my $dataTable  = $allMeasurements[$measurementIndex]{dataTable};

    $allMeasurements[$measurementIndex]{isReferenced}   = 1;
    $allMeasurements[$measurementIndex]{firstTimestamp} = $firstTimestamp;

    $allMeasurements[ $allMeasurements[$measurementIndex]{qlMeasurementIndex} ]{isReferenced} = 1
      if exists $allMeasurements[$measurementIndex]{qlMeasurementIndex};
    $allMeasurements[ $allMeasurements[$measurementIndex]{qcMeasurementIndex} ]{isReferenced} = 1
      if exists $allMeasurements[$measurementIndex]{qcMeasurementIndex};
    $allMeasurements[ $allMeasurements[$measurementIndex]{uqlMeasurementIndex} ]{isReferenced} = 1
      if exists $allMeasurements[$measurementIndex]{uqlMeasurementIndex};

    return
      if defined $allTables{$sensorNode}{$dataTable}{firstTimestamp}
      && $allTables{$sensorNode}{$dataTable}{firstTimestamp} le $firstTimestamp;

    $allTables{$sensorNode}{$dataTable}{firstTimestamp} = $firstTimestamp;
}

sub updateLastTimestamp {
    my ( $measurementIndex, $lastTimestamp ) = @_;

    return
      if defined $allMeasurements[$measurementIndex]{lastTimestamp}
      && $allMeasurements[$measurementIndex]{lastTimestamp} ge $lastTimestamp;

    my $sensorNode = $allMeasurements[$measurementIndex]{sensorNode};
    my $dataTable  = $allMeasurements[$measurementIndex]{dataTable};

    $allMeasurements[$measurementIndex]{isReferenced}  = 1;
    $allMeasurements[$measurementIndex]{lastTimestamp} = $lastTimestamp;

    $allMeasurements[ $allMeasurements[$measurementIndex]{qlMeasurementIndex} ]{isReferenced} = 1
      if exists $allMeasurements[$measurementIndex]{qlMeasurementIndex};
    $allMeasurements[ $allMeasurements[$measurementIndex]{qcMeasurementIndex} ]{isReferenced} = 1
      if exists $allMeasurements[$measurementIndex]{qcMeasurementIndex};
    $allMeasurements[ $allMeasurements[$measurementIndex]{uqlMeasurementIndex} ]{isReferenced} = 1
      if exists $allMeasurements[$measurementIndex]{uqlMeasurementIndex};

    return
      if defined $allTables{$sensorNode}{$dataTable}{lastTimestamp}
      && $allTables{$sensorNode}{$dataTable}{lastTimestamp} ge $lastTimestamp;

    $allTables{$sensorNode}{$dataTable}{lastTimestamp} = $lastTimestamp;
}

sub getMeasurementIndex {
    my ( $sensorNode, $dataTable, $measurementName ) = @_;

    return undef if !defined $sensorNode;

    my $fullMeasurementName;
    if ( defined $dataTable ) {
        $fullMeasurementName = "$sensorNode.$dataTable.$measurementName";
    }
    else {
        $fullMeasurementName = $sensorNode;    # Support passing in full name
    }

    my $measurementIndex = $allMeasurementIndexes{ lc $fullMeasurementName };
    if ( !defined $measurementIndex ) {
        $log->debug("WARNING: Failed to find specification of measurement $fullMeasurementName");
        return undef;
    }

    return $measurementIndex;
}

sub maxSizeMeasurementArray {
    my ($measurementIndex) = @_;

    # Trigger allocation of an array big enough to hold all of the data
    my $maxMeasurements;
    $maxMeasurements = $dailySamplingTimes   if $allMeasurements[$measurementIndex]{dataTable} eq $oneDayTable;
    $maxMeasurements = $hourlySamplingTimes  if $allMeasurements[$measurementIndex]{dataTable} eq $oneHourTable;
    $maxMeasurements = $fiveMinSamplingTimes if $allMeasurements[$measurementIndex]{dataTable} eq $fiveMinuteTable;
    $maxMeasurements = $oneMinSamplingTimes  if $allMeasurements[$measurementIndex]{dataTable} eq $oneMinuteTable;

    if ( defined $maxMeasurements && $maxMeasurements > $#{ $allData[$measurementIndex] } ) {
        $log->debug( "Allocated array of size $maxMeasurements for measurement "
              . $allMeasurements[$measurementIndex]{sensorNode} . "."
              . $allMeasurements[$measurementIndex]{dataTable} . "."
              . $allMeasurements[$measurementIndex]{measurementName} );
        $allData[$measurementIndex][$maxMeasurements] = undef;
    }
}

################################################################################
# Support synchronizing data from and to the PostgreSQL database
my %allDatabaseColumns;

sub getCurrentDatabaseColumns {
    my ( $dbh, $dbSchema, $dbTable, $dbColumns ) = @_;

    if ( !exists $allDatabaseColumns{$dbSchema} ) {
        my $sth = $dbh->prepare("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema=?");
        $sth->execute($dbSchema);
        my ( $currentTableName, $currentColumnName );
        $sth->bind_columns( \$currentTableName, \$currentColumnName );

        while ( $sth->fetch ) {
            $allDatabaseColumns{$dbSchema}{$currentTableName}{$currentColumnName} = 1;
        }
    }

    return unless exists $allDatabaseColumns{$dbSchema} && exists $allDatabaseColumns{$dbSchema}{$dbTable};

    foreach my $dbColumn ( keys %{ $allDatabaseColumns{$dbSchema}{$dbTable} } ) {
        $dbColumns->{$dbColumn} = 1;
    }
}

################################################################################
# Synchronize data table to database
my %updatedDatabaseViews;

sub syncToDB {
    my ( $dbh, $dbSchema, $sensorNode, $dataTable ) = @_;

    my $databaseTable = $allTables{$sensorNode}{$dataTable}{databaseTable};
    my $fullTableName = "$dbSchema.$databaseTable";

    # Get the current list of columns in the database
    my $tableExists;
    my %allColumnNames;

    getCurrentDatabaseColumns( $dbh, $dbSchema, $databaseTable, \%allColumnNames );

    if ( scalar( keys %allColumnNames ) && $rebuildDB ) {
        $dbh->do("DROP TABLE IF EXISTS $fullTableName CASCADE");

        $log->info("[$destinationDB:$fullTableName] Deleted database table");

        undef %allColumnNames;
    }
    elsif ( scalar( keys %allColumnNames ) ) {
        $tableExists = 1;
    }

    my @currentMeasurements;
    my @currentDataMeasurements;
    my @currentUQualityFields;
    my @currentQualityFields;
    my @newMeasurements;
    foreach my $currentMeasurement (@allMeasurements) {
        next
          unless $currentMeasurement->{sensorNode} eq $sensorNode
          && $currentMeasurement->{dataTable} eq $dataTable
          && $currentMeasurement->{isReferenced}
          && exists $currentMeasurement->{databaseColumn};

        # Don't need any QC columns with the original/raw data
        next if exists $currentMeasurement->{qcField} && $dbSchema eq "sn_original";

        push @currentMeasurements, $currentMeasurement;

        if ( exists $currentMeasurement->{uqlField} ) {
            push @currentUQualityFields, $currentMeasurement;
        }
        elsif ( exists $currentMeasurement->{qcField} ) {
            push @currentQualityFields, $currentMeasurement;
        }
        else {
            push @currentDataMeasurements, $currentMeasurement;
        }

        next if exists $allColumnNames{ $currentMeasurement->{databaseColumn} };
        push @newMeasurements, $currentMeasurement;
    }

    return unless @currentMeasurements;

    my $viewNeeded;
    if ( $tableExists && scalar(@newMeasurements) ) {
        my @newColumnDefinitions;
        foreach my $newMeasurement (@newMeasurements) {
            my $columnName = $newMeasurement->{databaseColumn};

            my $dataType = "double precision";
            $dataType = "smallint"          if $columnName =~ /_ql$/;
            $dataType = "smallint"          if $columnName =~ /_uql$/;
            $dataType = "character varying" if $columnName =~ /_qc$/;
            $dataType = "integer"           if $columnName eq "record";

            $log->info("[$destinationDB:$fullTableName] Adding column $columnName $dataType");

            push @newColumnDefinitions, "ADD COLUMN $columnName $dataType";
        }

        my $sql = "ALTER TABLE $fullTableName " . join( ',', @newColumnDefinitions );

        $dbh->do($sql);

        $log->info("[$destinationDB:$fullTableName] Altered database table");

        $viewNeeded = 1;
    }
    elsif ( !$tableExists ) {
        my @allColumnDefinitions = ("measurement_time timestamp with time zone NOT NULL PRIMARY KEY");
        foreach my $currentMeasurement (@currentMeasurements) {
            my $columnName = $currentMeasurement->{databaseColumn};

            my $dataType = "double precision";
            $dataType = "smallint"          if $columnName =~ /_ql$/;
            $dataType = "smallint"          if $columnName =~ /_uql$/;
            $dataType = "character varying" if $columnName =~ /_qc$/;
            $dataType = "integer"           if $columnName eq "record";

            $log->debug("[$destinationDB:$fullTableName] Adding column $columnName $dataType");

            push @allColumnDefinitions, "$columnName $dataType";
        }

        my $sql = "CREATE TABLE $fullTableName (" . join( ',', @allColumnDefinitions ) . ") WITH (OIDS=FALSE)";

        $dbh->do($sql);
        $dbh->do("ALTER TABLE $fullTableName OWNER TO hakai_admin");
        $dbh->do("GRANT ALL ON TABLE $fullTableName TO hakai_admin WITH GRANT OPTION");
        $dbh->do("GRANT SELECT ON TABLE $fullTableName TO hakai_read_only");
        $dbh->do("GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE $fullTableName TO hakai_read_write");

        $log->info("[$destinationDB:$fullTableName] Created database table");

        $viewNeeded = 1;
    }

    # Create a database view that includes the sensor node and measurement names in the column name
    if ($viewNeeded) {
        my $dbViewName = "$sensorNode:$dataTable";

        my @viewColumnNames;
        my @tableColumnNames;

        foreach my $currentMeasurement (@currentMeasurements) {
            my $columnName = $currentMeasurement->{databaseColumn};

            next if uc($columnName) eq "RECORD";    # Exclude RECORD number

            push @viewColumnNames, "$sensorNode:" . $currentMeasurement->{measurementName};

            # Convert lat and long from Degrees Minutes (DM) to Decimal Degrees
            my $tableColumnName = "$fullTableName.$columnName";
            if ( index( uc( $currentMeasurement->{measurementName} ), "LATITUDE" ) >= 0 ) {
                $tableColumnName = "TRUNC($tableColumnName/100)+($tableColumnName-100*TRUNC($tableColumnName/100))/60";
            }
            elsif ( index( uc( $currentMeasurement->{measurementName} ), "LONGITUDE" ) >= 0 ) {
                $tableColumnName =
                  "-1*(TRUNC($tableColumnName/100)+($tableColumnName-100*TRUNC($tableColumnName/100))/60)";
            }
            elsif ( $tableColumnName =~ /_uql$/ ) {
                $tableColumnName = "COALESCE($tableColumnName,2)";
            }

            push @tableColumnNames, $tableColumnName;
        }

        my $sql =
            "CREATE VIEW sn.\"$dbViewName\" (\"measurementTime\",\""
          . join( "\",\"", @viewColumnNames )
          . "\") AS SELECT measurement_time,"
          . join( ",", @tableColumnNames )
          . " FROM $fullTableName";

        $dbh->do("DROP VIEW IF EXISTS sn.\"$dbViewName\"") unless $rebuildDB;
        $dbh->do($sql);
        $dbh->do("ALTER VIEW sn.\"$dbViewName\" OWNER TO hakai_admin");
        $dbh->do("GRANT ALL ON sn.\"$dbViewName\" TO hakai_admin WITH GRANT OPTION");
        $dbh->do("GRANT SELECT ON sn.\"$dbViewName\" TO hakai_read_only");
        $dbh->do("GRANT SELECT ON sn.\"$dbViewName\" TO hakai_read_write");

        $log->info("[$destinationDB:$fullTableName] Created database view $dbViewName");

        $updatedDatabaseViews{$sensorNode}{$dataTable} = 1;
    }

    my $sampleInterval = $allTables{$sensorNode}{$dataTable}{sampleInterval};
    my $samplingTimeIndex;
    my $samplingTime;
    if ( $sampleInterval == 1440 ) {
        $samplingTimeIndex = \%dailySamplingTimeIndex;
        $samplingTime      = \@dailySamplingTime;
    }
    elsif ( $sampleInterval == 60 ) {
        $samplingTimeIndex = \%hourlySamplingTimeIndex;
        $samplingTime      = \@hourlySamplingTime;
    }
    elsif ( $sampleInterval == 5 ) {
        $samplingTimeIndex = \%fiveMinSamplingTimeIndex;
        $samplingTime      = \@fiveMinSamplingTime;
    }
    elsif ( $sampleInterval == 1 ) {
        $samplingTimeIndex = \%oneMinSamplingTimeIndex;
        $samplingTime      = \@oneMinSamplingTime;
    }

    my $firstTimestamp = $allTables{$sensorNode}{$dataTable}{firstTimestamp};
    my $firstTimeIndex = $samplingTimeIndex->{$firstTimestamp};
    my $lastTimestamp  = $allTables{$sensorNode}{$dataTable}{lastTimestamp};
    my $lastTimeIndex  = $samplingTimeIndex->{$lastTimestamp};

    # Unless we are refreshing all data, move the clock ahead by 4 days so we don't
    # get tripped up by accumulated/aggregated measurement values
    if ( !defined $refresh ) {
        if ( $sampleInterval == 1440 ) {
            $firstTimeIndex += 4;
        }
        elsif ( $sampleInterval == 60 ) {
            $firstTimeIndex += 4 * 24;
        }
        elsif ( $sampleInterval == 5 ) {
            $firstTimeIndex += 4 * 24 * 12;
        }

        # First time edge case
        if ( $firstTimeIndex < $lastTimeIndex ) {
            $firstTimestamp = $samplingTime->[$firstTimeIndex];
        }
        else {
            $firstTimeIndex = $samplingTimeIndex->{$firstTimestamp};
        }
    }

    # Fetch all of the current database records
    my %databaseMeasurementTimes;
    {
        my @currentColumnNames;
        my @currentDataIndexes;
        foreach my $currentMeasurement (@currentDataMeasurements) {
            push @currentColumnNames, $currentMeasurement->{databaseColumn};
            push @currentDataIndexes, $currentMeasurement->{measurementIndex};
        }
        my @currentQualityIndexes;
        foreach my $currentMeasurement (@currentQualityFields) {
            push @currentColumnNames,    $currentMeasurement->{databaseColumn};
            push @currentQualityIndexes, $currentMeasurement->{measurementIndex};
        }

        # NOTE: Ignore UQuality fields when comparing to existing data

        my $sql =
            "SELECT measurement_time at time zone 'PST',"
          . join( ',', @currentColumnNames )
          . " FROM $fullTableName WHERE measurement_time>=? ORDER BY measurement_time";

        my $sth = $dbh->prepare($sql);
        $sth->execute("$firstTimestamp-0800");

        my $unchangedRows = 0;
        my $changedRows   = 0;
        my $firstUnchangedTimestamp;
        my $lastUnchangedTimestamp;
        my $rows = [];    # cache for batches of rows
        while (
            my $row = (
                shift(@$rows) ||    # get row from cache, or reload cache:
                  shift( @{ $rows = $sth->fetchall_arrayref( undef, 10000 ) || [] } )
            )
          )
        {
            my $measurementTime = $row->[0];

            my $currentTimeIndex = $samplingTimeIndex->{$measurementTime};
            my $columnIndex      = 1;
            my @currentChanges;

            # Process all of the data measurements first
            foreach my $measurementIndex (@currentDataIndexes) {
                my $dbMeasurementValue = $row->[ $columnIndex++ ];
                my $measurementValue   = $allData[$measurementIndex][$currentTimeIndex];

                if ( defined $measurementValue && !defined $dbMeasurementValue ) {
                    push @currentChanges,
                      $allMeasurements[$measurementIndex]{databaseColumn} . " set to $measurementValue";
                }
                elsif ( !defined $measurementValue && defined $dbMeasurementValue ) {
                    my $qcMeasurementIndex = $allMeasurements[$measurementIndex]{qcMeasurementIndex};
                    if ( defined $qcMeasurementIndex && defined $qcData[$qcMeasurementIndex]{$currentTimeIndex} ) {
                        push @currentChanges,
                            $allMeasurements[$measurementIndex]{databaseColumn}
                          . " removed $dbMeasurementValue and flagged with "
                          . $qcData[$qcMeasurementIndex]{$currentTimeIndex};
                    }
                }
                elsif (defined $measurementValue
                    && defined $dbMeasurementValue
                    && abs( $dbMeasurementValue - $measurementValue ) > 0.0001 )
                {
                    push @currentChanges,
                      $allMeasurements[$measurementIndex]{databaseColumn}
                      . " changed from $dbMeasurementValue to $measurementValue";
                }
            }

            # Next process all of the quality fields
            foreach my $measurementIndex (@currentQualityIndexes) {
                my $dbMeasurementValue = $row->[ $columnIndex++ ];
                my $measurementValue   = $qcData[$measurementIndex]{$currentTimeIndex};

                if ( defined $measurementValue && !defined $dbMeasurementValue ) {
                    push @currentChanges,
                      $allMeasurements[$measurementIndex]{databaseColumn} . " set to $measurementValue";
                }
                elsif ( !defined $measurementValue && defined $dbMeasurementValue ) {
                    push @currentChanges,
                      $allMeasurements[$measurementIndex]{databaseColumn} . " removed Quality flag $dbMeasurementValue";
                }
                elsif (defined $measurementValue
                    && defined $dbMeasurementValue
                    && $dbMeasurementValue ne $measurementValue )
                {
                    push @currentChanges,
                      $allMeasurements[$measurementIndex]{databaseColumn}
                      . " changed from $dbMeasurementValue to $measurementValue";
                }
            }

            if (@currentChanges) {
                foreach my $currentChange (@currentChanges) {
                    $log->info("[$fullTableName:$measurementTime] $currentChange");
                }

                if ($syncDB) {
                    $dbh->do( "DELETE FROM $fullTableName WHERE measurement_time>=?", undef, "$measurementTime-0800" );
                    $log->info("[$destinationDB:$fullTableName] Removed existing data starting at $measurementTime");
                    last;
                }

                $changedRows++;
            }
            else {
                $firstUnchangedTimestamp = $measurementTime unless defined $firstUnchangedTimestamp;
                $lastUnchangedTimestamp = $measurementTime;
                $unchangedRows++;
            }

            $databaseMeasurementTimes{$measurementTime} = 1;
        }

        $log->info("[$destinationDB:$fullTableName] $changedRows rows have one or more different value")
          if $changedRows;

        if ($unchangedRows) {
            $log->info( "[$destinationDB:$fullTableName] $unchangedRows rows remain unchanged"
                  . " between $firstUnchangedTimestamp and $lastUnchangedTimestamp" );
        }
        else {
            $log->info("[$destinationDB:$fullTableName] No data remains unchanged starting at $firstTimestamp")
              if $tableExists;
        }
    }

    return unless $syncDB || $rebuildDB;

    if ( $lastTimeIndex >= $firstTimeIndex ) {

        my $lastParentMeasurementIndex = 0;
        my @insertedMeasurements;
        foreach my $currentMeasurement (@currentMeasurements) {

            next if exists $currentMeasurement->{qcField};

            my $currentInsertedIndex = scalar(@insertedMeasurements) - 1;
            if ( !exists $currentMeasurement->{parentMeasurementIndex}
                || $currentMeasurement->{parentMeasurementIndex} != $lastParentMeasurementIndex )
            {
                $currentInsertedIndex++;

                $lastParentMeasurementIndex = $currentMeasurement->{parentMeasurementIndex}
                  if exists $currentMeasurement->{parentMeasurementIndex};

                $insertedMeasurements[$currentInsertedIndex]{qlMeasurementIndex} =
                  $currentMeasurement->{qlMeasurementIndex}
                  if exists $currentMeasurement->{qlMeasurementIndex};
                $insertedMeasurements[$currentInsertedIndex]{qcMeasurementIndex} =
                  $currentMeasurement->{qcMeasurementIndex}
                  if exists $currentMeasurement->{qcMeasurementIndex};
                $insertedMeasurements[$currentInsertedIndex]{uqlMeasurementIndex} =
                  $currentMeasurement->{uqlMeasurementIndex}
                  if exists $currentMeasurement->{uqlMeasurementIndex};
            }

            push @{ $insertedMeasurements[$currentInsertedIndex]{dataIndexes} },
              $currentMeasurement->{measurementIndex};
        }

        my @currentColumnNames;
        foreach my $insertedMeasurement (@insertedMeasurements) {

            # First insert all of the data values related to this measurement
            foreach my $measurementIndex ( @{ $insertedMeasurement->{dataIndexes} } ) {
                push @currentColumnNames, $allMeasurements[$measurementIndex]{databaseColumn};
            }

            push @currentColumnNames, $allMeasurements[ $insertedMeasurement->{qlMeasurementIndex} ]{databaseColumn}
              if exists $insertedMeasurement->{qlMeasurementIndex};
            push @currentColumnNames, $allMeasurements[ $insertedMeasurement->{qcMeasurementIndex} ]{databaseColumn}
              if exists $insertedMeasurement->{qcMeasurementIndex};
            push @currentColumnNames, $allMeasurements[ $insertedMeasurement->{uqlMeasurementIndex} ]{databaseColumn}
              if exists $insertedMeasurement->{uqlMeasurementIndex};
        }

        my $newRows = 0;
        my $updateSth;
        for my $currentTimeIndex ( $firstTimeIndex .. $lastTimeIndex ) {
            my $measurementTime = $samplingTime->[$currentTimeIndex];
            next if exists $databaseMeasurementTimes{$measurementTime};

            $dbh->do( "COPY $fullTableName (measurement_time," . join( ',', @currentColumnNames ) . ") FROM STDIN" )
              if $newRows == 0;

            my @values = ("$measurementTime-0800");    # UTC

            foreach my $insertedMeasurement (@insertedMeasurements) {

                my $foundDataValue;

                # First insert all of the data values related to this measurement
                foreach my $measurementIndex ( @{ $insertedMeasurement->{dataIndexes} } ) {
                    if ( defined $allData[$measurementIndex][$currentTimeIndex] ) {
                        $foundDataValue = 1;
                        push @values, $allData[$measurementIndex][$currentTimeIndex];
                    }
                    else {
                        push @values, "\\N";    # NULL
                    }
                }

                if ( exists $insertedMeasurement->{qlMeasurementIndex} ) {
                    my $currentQualityLevel = $qcData[ $insertedMeasurement->{qlMeasurementIndex} ]{$currentTimeIndex};
                    my $currentQualityFlag  = $qcData[ $insertedMeasurement->{qcMeasurementIndex} ]{$currentTimeIndex};

                    # Data have not been QC-tested, or the information on quality is not available
                    my $currentUQualityLevel = 2;

                    if ( !$foundDataValue ) {
                        $currentUQualityLevel = 9;
                    }
                    elsif ( defined $currentQualityFlag ) {
                        if ( index( $currentQualityFlag, "AV" ) == 0 ) {

                            # Data have passed critical real-time quality control tests
                            # and are deemed adequate for use as preliminary data
                            $currentUQualityLevel = 1;
                        }
                        elsif (index( $currentQualityFlag, "AR" ) >= 0
                            || index( $currentQualityFlag, "BR" ) >= 0
                            || index( $currentQualityFlag, "SVD" ) >= 0 )

                        {
                # Data are considered to have failed one or more critical real-time QC checks.
                # If they are disseminated at all, it should be readily apparent that they are not of acceptable quality
                            $currentUQualityLevel = 4;
                        }
                        else {

                            # Data are considered to be either suspect or of high interest to data providers and users
                            # They are flagged suspect to draw further attention to them by operators.
                            $currentUQualityLevel = 3;
                        }
                    }

                    $currentQualityLevel = "\\N" unless defined $currentQualityLevel;
                    $currentQualityFlag  = "\\N" unless defined $currentQualityFlag;

                    push @values, $currentQualityLevel, $currentQualityFlag, $currentUQualityLevel;
                }
            }

            $dbh->pg_putcopydata( join( "\t", @values ) . "\n" );
            $newRows++;
        }

        if ($newRows) {
            $dbh->pg_putcopyend();
            $dbh->commit;
            $log->info("[$destinationDB:$fullTableName] $newRows rows have been added");
        }
        else {
            $log->info("[$destinationDB:$fullTableName] No rows added");
        }
    }
}

################################################################################
# Synchronize data table from the database
sub syncFromDB {
    my ( $dbh, $dbSchema, $sensorNode, $dataTable ) = @_;

    my $databaseTable = $allTables{$sensorNode}{$dataTable}{databaseTable};
    my $fulldataTable = "$dbSchema.$databaseTable";

    my %allColumnNames;
    getCurrentDatabaseColumns( $dbh, $dbSchema, $databaseTable, \%allColumnNames );

    if ( !%allColumnNames ) {
        $log->info("[$sourceDB:$fulldataTable] No data found in db") if $dataTable ne $oneDayTable;
        return;
    }

    my @currentColumnNames;
    my @currentMeasurementIndexes;
    my $skippedAggregatedColumns;
    for my $measurementIndex ( 0 .. $#allMeasurements ) {
        my $measurementName = $allMeasurements[$measurementIndex]{measurementName};
        my $columnName      = $allMeasurements[$measurementIndex]{databaseColumn};

        next
          unless $allMeasurements[$measurementIndex]{sensorNode} eq $sensorNode
          && $allMeasurements[$measurementIndex]{dataTable} eq $dataTable
          && defined $columnName
          && exists $allColumnNames{$columnName};

        if ( $allMeasurements[$measurementIndex]{importFlag} != 1 ) {
            $log->debug("[$sensorNode.$dataTable.$measurementName] Not importing from db");
            next;
        }

        if ( $dataTable eq $oneHourTable && $allMeasurements[$measurementIndex]{aggregateFiveMin} == 1 ) {
            $skippedAggregatedColumns = 1;
            $log->debug("[$sensorNode.$dataTable.$measurementName] Not importing from db; will be aggregated");
            next;
        }

        push @currentColumnNames,        $columnName;
        push @currentMeasurementIndexes, $measurementIndex;

        maxSizeMeasurementArray($measurementIndex);
    }

    if ( !@currentColumnNames ) {
        $log->info("[$sourceDB:$fulldataTable] No columns found in db, skipping") unless $skippedAggregatedColumns;
        return;
    }

    my $sampleInterval = $allTables{$sensorNode}{$dataTable}{sampleInterval};
    my $samplingTimeIndex;
    my $samplingTime;
    if ( $sampleInterval == 1440 ) {
        $samplingTimeIndex = \%dailySamplingTimeIndex;
        $samplingTime      = \@dailySamplingTime;
    }
    elsif ( $sampleInterval == 60 ) {
        $samplingTimeIndex = \%hourlySamplingTimeIndex;
        $samplingTime      = \@hourlySamplingTime;
    }
    elsif ( $sampleInterval == 5 ) {
        $samplingTimeIndex = \%fiveMinSamplingTimeIndex;
        $samplingTime      = \@fiveMinSamplingTime;
    }
    elsif ( $sampleInterval == 1 ) {
        $samplingTimeIndex = \%oneMinSamplingTimeIndex;
        $samplingTime      = \@oneMinSamplingTime;
    }

    # Fetch all of the current database records
    my $firstQueriedTimestamp = $samplingTime->[0];

    my $sql =
        "SELECT measurement_time at time zone 'PST',"
      . join( ',', @currentColumnNames )
      . " FROM $fulldataTable WHERE measurement_time>=? ORDER BY measurement_time";

    my $sth = $dbh->prepare($sql);
    $sth->execute("$firstQueriedTimestamp-0800");

    my $fetchedRows = 0;
    my @firstTimestamps;
    my @lastTimestamps;
    my $lastTimestamp;
    my $rows = [];    # cache for batches of rows
    while (
        my $row = (
            shift(@$rows) ||    # get row from cache, or reload cache:
              shift( @{ $rows = $sth->fetchall_arrayref( undef, 10000 ) || [] } )
        )
      )
    {
        $fetchedRows++;

        my $measurementTime = $row->[0];

        if ( !exists $samplingTimeIndex->{$measurementTime} ) {
            $log->error("ERROR: Failed to find measurementTime $measurementTime, skipping row $fetchedRows");
            next;
        }

        my $currentTimeIndex = $samplingTimeIndex->{$measurementTime};

        my $columnIndex = 0;
        foreach my $measurementIndex (@currentMeasurementIndexes) {
            $columnIndex++;

            next unless defined $row->[$columnIndex];

            $firstTimestamps[$columnIndex] = $measurementTime if !exists $firstTimestamps[$columnIndex];
            $lastTimestamps[$columnIndex] = $measurementTime;

            $allData[$measurementIndex][$currentTimeIndex] = $row->[$columnIndex];
        }

        $lastTimestamp = $measurementTime;
        $allTables{$sensorNode}{$dataTable}{dailyRecordCounts}{ substr $measurementTime, 0, 10 }++;
    }

    if ($fetchedRows) {
        $log->info( "[$sourceDB:$fulldataTable] Read $fetchedRows records ("
              . scalar(@currentColumnNames) . " of "
              . scalar( keys %allColumnNames )
              . " columns) between $firstQueriedTimestamp and $lastTimestamp" );

        my $columnIndex = 0;
        foreach my $measurementIndex (@currentMeasurementIndexes) {
            $columnIndex++;

            next unless exists $firstTimestamps[$columnIndex];

            $allMeasurements[$measurementIndex]{isRead} = 1;

            updateFirstTimestamp( $measurementIndex, $firstTimestamps[$columnIndex] );
            updateLastTimestamp( $measurementIndex, $lastTimestamps[$columnIndex] );
        }
    }
    else {
        $log->info("[$sourceDB:$fulldataTable] No data rows found starting at $firstQueriedTimestamp");
    }
}

################################################################################
# Parse the list of measurements, including data about each measurement
my $measurementsWorksheet = $specificationWorkbook->worksheet('SensorMeasurements');
if ( defined $measurementsWorksheet ) {
    my @columnNames;
    my ( undef, $maxCols ) = $measurementsWorksheet->col_range();
    my ( undef, $maxRows ) = $measurementsWorksheet->row_range();

    for my $columnNum ( 0 .. $maxCols ) {
        my $cellValue = getCellValue( $measurementsWorksheet, 0, $columnNum );
        $columnNames[$columnNum] = $cellValue if defined $cellValue;
    }

    for my $rowNum ( 1 .. $maxRows ) {
        my $sensorNode;
        my $measurementName;
        my $standardName;
        my $displayName;
        my $measurementType;
        my $function;
        my $units;
        my $minimumValue;
        my $maximumValue;
        my $linkedMeasurementsName;
        my $clipRange;
        my $aggregateFiveMin;
        my $storeInDB;
        my $hasQC;
        my $deploymentTime;
        my $elevation;
        my $sensorType;
        my $serialNumber;
        my $sensorDescription;
        my $sensorDocumentation;
        my $comments;

        for my $columnNum ( 0 .. $maxCols ) {
            next unless defined $columnNames[$columnNum];

            my $cellValue = getCellValue( $measurementsWorksheet, $rowNum, $columnNum );
            next unless defined $cellValue;

            if ( uc( $columnNames[$columnNum] ) eq "SENSOR NODE" ) {
                $sensorNode = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "MEASUREMENT NAME" ) {
                $measurementName = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "STANDARD NAME" ) {
                $standardName = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "DISPLAY NAME" ) {
                $displayName = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "MEASUREMENT TYPE" ) {
                $measurementType = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "FUNCTION" ) {
                $function = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "UNITS" ) {
                $units = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "MINIMUM VALUE" ) {
                $minimumValue = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "MAXIMUM VALUE" ) {
                $maximumValue = $cellValue;
            }
            elsif (uc( $columnNames[$columnNum] ) eq "LINKED MEASUREMENT NAME"
                || uc( $columnNames[$columnNum] ) eq "LINKED MEASUREMENT NAMES" )
            {
                $linkedMeasurementsName = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "CLIP RANGE" ) {
                $clipRange = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "AGGREGATE" ) {
                $aggregateFiveMin = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "STORE IN DB" ) {
                $storeInDB = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "QC" ) {
                $hasQC = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "DEPLOYMENT TIME" ) {
                $deploymentTime = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "ELEVATION" ) {
                $elevation = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "SENSOR TYPE" ) {
                $sensorType = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "SERIAL NUMBER" ) {
                $serialNumber = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "SENSOR DESCRIPTION" ) {
                $sensorDescription = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "SENSOR DOCUMENTATION" ) {
                $sensorDocumentation = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "COMMENTS" ) {
                $comments = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) ne "ORDINAL" ) {
                $log->warn( "WARNING: Unrecognized column heading \"$columnNames[$columnNum]\""
                      . "found in SensorMeasurements worksheet" );
            }
        }

        next
          unless defined $sensorNode
          && defined $measurementName
          && defined $measurementType;

        # Include each measurement in each data table
        foreach my $dataTable ( "$oneDayTable", "$oneHourTable", "$fiveMinuteTable", "$oneMinuteTable" ) {

            # Only support processing one minute data from the Pruth dock sensor node
            next if $dataTable eq $oneMinuteTable and $sensorNode ne "PruthDock";

            # No five minute data records for the KC Seaology sensor node
            next if $dataTable eq $fiveMinuteTable and $sensorNode eq "KCSeaology";

            addMeasurement(
                $sensorNode,          $dataTable,       $measurementName,        $standardName,
                $displayName,         $measurementType, $function,               $units,
                $minimumValue,        $maximumValue,    $linkedMeasurementsName, $clipRange,
                $aggregateFiveMin,    $storeInDB,       $hasQC,                  $deploymentTime,
                $elevation,           $sensorType,      $serialNumber,           $sensorDescription,
                $sensorDocumentation, $comments
            );

            my $parentMeasurementIndex = getMeasurementIndex( $sensorNode, $dataTable, $measurementName );
            $allMeasurements[$parentMeasurementIndex]{parentMeasurementIndex} = $parentMeasurementIndex;
            push @{ $allMeasurements[$parentMeasurementIndex]{childMeasurementIndexes} }, $parentMeasurementIndex;

            next
              if index( $measurementName, "_Med" ) > 0
              || index( $measurementName, "_Avg" ) > 0
              || index( $measurementName, "_Min" ) > 0
              || index( $measurementName, "_Max" ) > 0
              || index( $measurementName, "_Std" ) > 0;

            foreach my $measurementFunction ( 'Med', 'Avg', 'Min', 'Max', 'Std' ) {
                my $secondaryMeasurementName = $measurementName . "_" . $measurementFunction;

                next
                  if defined getMeasurementIndex( $sensorNode, $dataTable, $secondaryMeasurementName );

                my $secondaryDisplayName = $displayName;
                $secondaryDisplayName .= "_$measurementFunction" if defined $secondaryDisplayName;

                my $secondaryMimimumValue;
                my $secondaryMaximumValue;
                if ( $measurementFunction ne "Std" ) {
                    $secondaryMimimumValue = $minimumValue;
                    $secondaryMaximumValue = $maximumValue;
                }
                addMeasurement(
                    $sensorNode,            $dataTable,              $secondaryMeasurementName,
                    $standardName,          $secondaryDisplayName,   $measurementType,
                    $measurementFunction,   $units,                  $secondaryMimimumValue,
                    $secondaryMaximumValue, $linkedMeasurementsName, $clipRange,
                    $aggregateFiveMin,      $storeInDB,              $hasQC,
                    $deploymentTime,        $elevation,              $sensorType,
                    $serialNumber,          $sensorDescription,      $sensorDocumentation,
                    $comments
                );

                my $secondaryMeasurementIndex =
                  getMeasurementIndex( $sensorNode, $dataTable, $secondaryMeasurementName );
                $allMeasurements[$secondaryMeasurementIndex]{parentMeasurementIndex} = $parentMeasurementIndex;
                push @{ $allMeasurements[$parentMeasurementIndex]{childMeasurementIndexes} },
                  $secondaryMeasurementIndex;
            }
        }
    }

    my $numMeasurements = scalar(@allMeasurements);
    $log->info("Read and parsed $numMeasurements measurements");
}

################################################################################
# Automatically support one 'diagnostics' data table for each sensor node
my @diagnosticsMeasurements = (
    { name => "BattVolt_Avg",  function => "Avg", units => "Volts" },
    { name => "BattVolt_Min",  function => "Min", units => "Volts" },
    { name => "BattVolt_Max",  function => "Max", units => "Volts" },
    { name => "PanelTemp_Min", function => "Min", units => "Deg C" },
    { name => "PanelTemp_Max", function => "Max", units => "Deg C" },

    #{ name => "OSVersion",      function => "Smp" },
    { name => "SerialNumber", function => "Smp" },
    { name => "StartTime",    function => "Smp" },

    #{ name => "StationName",    function => "Smp" },
    { name => "RunSignature",   function => "Smp" },
    { name => "ProgSignature",  function => "Smp" },
    { name => "LithiumBattery", function => "Smp", units => "Volts" },
    { name => "Low12VCount",    function => "Smp" },
    { name => "SkippedScan",    function => "Smp" },
    { name => "CPUDriveFree",   function => "Smp" },
    { name => "WatchdogErrors", function => "Smp" },
    { name => "MaxProcTime",    function => "Smp" },
    { name => "DoseOutOfOrder", function => "Smp" }
);

foreach my $sensorNode ( sort keys %allTables ) {
    next
      if exists $allTables{$sensorNode}{diagnostics}
      || $sensorNode eq "Portable"
      || $sensorNode eq "SSN844PWR"
      || $sensorNode eq "QuadraLimpetSeaFET"
      || $sensorNode =~ /^QuadraFTS/
      || $sensorNode =~ /BoL/
      || $sensorNode =~ /Seaology/
      || $sensorNode =~ /^SA_/
      || $sensorNode eq "PruthMooring"
      || $sensorNode eq "QU5_Mooring"
      || $sensorNode eq "WTS693Lake";

    foreach my $diagnosticsMeasurement (@diagnosticsMeasurements) {

        addMeasurement(
            $sensorNode,
            $diagnosticsTable,    # data table
            $diagnosticsMeasurement->{name},
            undef,                # standardName
            $diagnosticsMeasurement->{displayName},
            "primary",            # measurement type
            $diagnosticsMeasurement->{function},
            $diagnosticsMeasurement->{units},
            undef,                # mimimumValue
            undef,                # maximumValue
            undef,                # linkedMeasurementsName
            undef,                # clipRange
            1,                    # storeInDB
            undef,                # hasQC
            undef,                # deploymentTime,
            undef,                # elevation
            undef,                # sensorType
            undef,                # serialNumber,
            undef,                # sensorDescription,
            undef,                # sensorDocumentation
            undef                 # comments
        );
    }
}

################################################################################
# Additional utility functions
sub getMeasurementCalculation {
    my ($measurementIndex) = @_;

    my $measurementCalculation = $allMeasurements[$measurementIndex]{measurementCalculation};
    $measurementCalculation =
        $allMeasurements[$measurementIndex]{sensorNode} . "."
      . $allMeasurements[$measurementIndex]{dataTable} . "."
      . $allMeasurements[$measurementIndex]{measurementName}
      unless length $measurementCalculation;

    return $measurementCalculation;
}

sub getNextTimeIndex {
    my ( $measurementIndex, $measurementTime ) = @_;

    my $dataTable = $allMeasurements[$measurementIndex]{dataTable};

    my $samplingTime;
    my $samplingTimes;
    if ( $dataTable eq $oneMinuteTable ) {
        $samplingTime  = \@oneMinSamplingTime;
        $samplingTimes = $oneMinSamplingTimes;
    }
    elsif ( $dataTable eq $fiveMinuteTable ) {
        $samplingTime  = \@fiveMinSamplingTime;
        $samplingTimes = $fiveMinSamplingTimes;
    }
    elsif ( $dataTable eq $oneHourTable ) {
        $samplingTime  = \@hourlySamplingTime;
        $samplingTimes = $hourlySamplingTimes;
    }
    elsif ( $dataTable eq $oneDayTable ) {
        $samplingTime  = \@dailySamplingTime;
        $samplingTimes = $dailySamplingTimes;
    }
    else {
        die "ERROR: Unsupported data table $dataTable";
    }

    my $leftIndex  = 0;
    my $rightIndex = $samplingTimes - 2;

    while ( $leftIndex <= $rightIndex ) {
        my $midIndex = int( ( $leftIndex + $rightIndex ) / 2.0 );

        if ( $samplingTime->[$midIndex] lt $measurementTime ) {
            $leftIndex = $midIndex + 1;
            next;
        }

        if ( $samplingTime->[$midIndex] gt $measurementTime ) {
            $rightIndex = $midIndex - 1;
            next;
        }

        # Exact match
        return $midIndex;
    }

    if (   $leftIndex > 0
        && $samplingTime->[ $leftIndex - 1 ] lt $measurementTime
        && $samplingTime->[$leftIndex] gt $measurementTime )
    {
        $log->debug( "Calculated next time of " . $samplingTime->[$leftIndex] . " for $dataTable:$measurementTime" );

        return $leftIndex;
    }

    $log->warn( "Failed to find next time of $dataTable:"
          . $allMeasurements[$measurementIndex]{measurementName}
          . ":$measurementTime" );
}

################################################################################
# Synchronize original/raw/source data from or to the database
{
    my $dbh = DBI->connect( "DBI:Pg:dbname=$sourceDB;host=$pgHost",
        $pgUser, $pgPassword, { 'AutoCommit' => 0, 'RaiseError' => 1 } );

    foreach my $sensorNode ( sort keys %allTables ) {
        foreach my $tableName ( sort keys %{ $allTables{$sensorNode} } ) {
            syncFromDB( $dbh, "sn_original", $sensorNode, $tableName );
        }
    }

    $dbh->commit;
    $dbh->disconnect;
}

################################################################################
# Flag 5 minute intervals where the number of recorded tips during a five
# minute interval does not match the amount of rain recorded during the
# same five minute interval
{
    my $firstQueriedTimestamp = $fiveMinSamplingTime[0];

    my $dbh = DBI->connect( "DBI:Pg:dbname=$sourceDB;host=$pgHost", $pgUser, $pgPassword, { 'RaiseError' => 1 } );

    my $sth =
      $dbh->prepare( "SELECT sensor_node, measurement_time at time zone 'PST', num_tips"
          . " FROM sn.rain_gauge_tips WHERE measurement_time>=?"
          . " AND sensor_node NOT IN ('PruthDock','BuxtonEast')"
          . " ORDER BY 1,2" );
    $sth->execute("$firstQueriedTimestamp-0800");

    my $currentSensorNode = "unknown";
    my $currentMeasurementIndex;
    my $currentTimeIndex;
    my $currentNumTips;
    my $rows = [];    # cache for batches of rows
    while (
        my $row = (
            shift(@$rows) ||    # get row from cache, or reload cache:
              shift( @{ $rows = $sth->fetchall_arrayref( undef, 10000 ) || [] } )
        )
      )
    {
        my $sensorNode      = $row->[0];
        my $measurementTime = $row->[1];
        my $numTips         = $row->[2];

        # Process the first or a different sensor node
        if ( $currentSensorNode ne $sensorNode ) {
            $currentMeasurementIndex = getMeasurementIndex( $sensorNode, $fiveMinuteTable, "Rain" );
            next
              unless defined $currentMeasurementIndex
              && exists $allMeasurements[$currentMeasurementIndex]{lastTimestamp};

            $log->info("[$sensorNode.$fiveMinuteTable.Rain] Comparing five minute to five second rain measurements");

            $currentSensorNode = $sensorNode;
            $currentNumTips    = 0;
            undef $currentTimeIndex;
        }

        my $timeIndex = getNextTimeIndex( $currentMeasurementIndex, $measurementTime );
        next unless defined $timeIndex;

        if ( defined $currentTimeIndex && $currentTimeIndex < $timeIndex ) {

            my $fiveMinuteRain = 0.2 * $currentNumTips;

            if ( !exists $allData[$currentMeasurementIndex][$currentTimeIndex] ) {

                my $nextMeasurementTime = $fiveMinSamplingTime[$currentTimeIndex];

                if ( $nextMeasurementTime lt $allMeasurements[$currentMeasurementIndex]{lastTimestamp} ) {
                    $log->debug(
"[$currentSensorNode.$fiveMinuteTable.Rain:$nextMeasurementTime] Five minute rain set to $fiveMinuteRain"
                    );

                    $allData[$currentMeasurementIndex][$currentTimeIndex] = $fiveMinuteRain;
                }
            }
            elsif ( $allData[$currentMeasurementIndex][$currentTimeIndex] < ( $fiveMinuteRain - 0.1 ) ) {
                my $nextMeasurementTime = $fiveMinSamplingTime[$currentTimeIndex];

                $log->info( "[$sensorNode.$fiveMinuteTable.Rain:$nextMeasurementTime] Five minute rain increased from "
                      . $allData[$currentMeasurementIndex][$currentTimeIndex]
                      . " to $fiveMinuteRain" );

                $allData[$currentMeasurementIndex][$currentTimeIndex] = $fiveMinuteRain;

                my $qcMeasurementIndex = $allMeasurements[$currentMeasurementIndex]{qcMeasurementIndex};

                $qcData[$qcMeasurementIndex]{$currentTimeIndex} = "SV:Auto:increased based on five second data"
                  if defined $qcMeasurementIndex && !exists $qcData[$qcMeasurementIndex]{$currentTimeIndex};
            }

            $currentNumTips = 0;
        }

        $currentNumTips += $numTips;
        $currentTimeIndex = $timeIndex;
    }
}

################################################################################
# Apply manual QC records to the original dataset
sub applyManualQC {
    my ( $dbh, $dbSchema, $sensorNode, $dataTable ) = @_;

    my $databaseTable = $allTables{$sensorNode}{$dataTable}{databaseTable};
    my $fulldataTable = "$dbSchema.$databaseTable";

    my %allColumnNames;
    getCurrentDatabaseColumns( $dbh, $dbSchema, $databaseTable, \%allColumnNames );

    if ( !%allColumnNames ) {
        $log->debug("[$sourceDB:$fulldataTable] No manual QC data found, skipping");
        return;
    }

    my $firstSamplingTime = $fiveMinSamplingTime[0] . "-800";

    $log->info("[$sensorNode.$dataTable] Searching for QC records in $fulldataTable");

    my $sth =
      $dbh->prepare(
"SELECT measurement_name, measurement_time at time zone 'PST', quality_level, qc_flag, val, med, avg, min, max, std"
          . " FROM $fulldataTable WHERE measurement_time>=?" );
    $sth->execute($firstSamplingTime);

    my %currentQcData;
    my $rows = [];    # cache for batches of rows
    while (
        my $row = (
            shift(@$rows) ||    # get row from cache, or reload cache:
              shift( @{ $rows = $sth->fetchall_arrayref( undef, 10000 ) || [] } )
        )
      )
    {
        my $measurementName = $row->[0];
        my $measurementTime = $row->[1];

        $currentQcData{$measurementName}{$measurementTime}{qualityLevel} = $row->[2];
        $currentQcData{$measurementName}{$measurementTime}{qcFlag} = $row->[3] if defined $row->[3] && length $row->[3];
        $currentQcData{$measurementName}{$measurementTime}{val}    = $row->[4] if defined $row->[4] && length $row->[4];
        $currentQcData{$measurementName}{$measurementTime}{med}    = $row->[5] if defined $row->[5] && length $row->[5];
        $currentQcData{$measurementName}{$measurementTime}{avg}    = $row->[6] if defined $row->[6] && length $row->[6];
        $currentQcData{$measurementName}{$measurementTime}{min}    = $row->[7] if defined $row->[7] && length $row->[7];
        $currentQcData{$measurementName}{$measurementTime}{max}    = $row->[8] if defined $row->[8] && length $row->[8];
        $currentQcData{$measurementName}{$measurementTime}{std}    = $row->[9] if defined $row->[9] && length $row->[9];
    }

    my $samplingTimeIndex;
    if ( $dataTable eq $oneHourTable ) {
        $samplingTimeIndex = \%hourlySamplingTimeIndex;
    }
    elsif ( $dataTable eq $fiveMinuteTable ) {
        $samplingTimeIndex = \%fiveMinSamplingTimeIndex;
    }
    elsif ( $dataTable eq $oneMinuteTable ) {
        $samplingTimeIndex = \%oneMinSamplingTimeIndex;
    }

    foreach my $measurementName ( sort keys %currentQcData ) {

        my $fullMeasurementName = "$sensorNode.$dataTable.$measurementName";

        my (
            $qlMeasurementIndex,  $qcMeasurementIndex,  $valMeasurementIndex, $medMeasurementIndex,
            $avgMeasurementIndex, $minMeasurementIndex, $maxMeasurementIndex, $stdMeasurementIndex
        );
        $qlMeasurementIndex = getMeasurementIndex( lc "${fullMeasurementName}_QL" );
        $qcMeasurementIndex = getMeasurementIndex( lc "${fullMeasurementName}_QC" );
        next unless defined $qlMeasurementIndex && defined $qcMeasurementIndex;

        $valMeasurementIndex = getMeasurementIndex( lc "$fullMeasurementName" );
        $medMeasurementIndex = getMeasurementIndex( lc "${fullMeasurementName}_Med" );
        $avgMeasurementIndex = getMeasurementIndex( lc "${fullMeasurementName}_Avg" );
        $minMeasurementIndex = getMeasurementIndex( lc "${fullMeasurementName}_Min" );
        $maxMeasurementIndex = getMeasurementIndex( lc "${fullMeasurementName}_Max" );
        $stdMeasurementIndex = getMeasurementIndex( lc "${fullMeasurementName}_Std" );

        my $firstMeasurementTime;
        my $lastMeasurementTime;
        my %numAppliedValues;
        my %numRemovedValues;

        foreach my $measurementTime ( sort keys %{ $currentQcData{$measurementName} } ) {

            my $timeIndex = $samplingTimeIndex->{$measurementTime};
            next unless defined $timeIndex;

            $firstMeasurementTime = $measurementTime unless defined $firstMeasurementTime;
            $lastMeasurementTime = $measurementTime;

            $qcData[$qlMeasurementIndex]{$timeIndex} = $currentQcData{$measurementName}{$measurementTime}{qualityLevel};
            $qcData[$qcMeasurementIndex]{$timeIndex} = $currentQcData{$measurementName}{$measurementTime}{qcFlag}
              if exists $currentQcData{$measurementName}{$measurementTime}{qcFlag};

            if ( defined $valMeasurementIndex ) {

                if ( exists $currentQcData{$measurementName}{$measurementTime}{val} ) {

                    if (
                        !exists $allData[$valMeasurementIndex][$timeIndex]
                        || abs(
                            $allData[$valMeasurementIndex][$timeIndex] -
                              $currentQcData{$measurementName}{$measurementTime}{val}
                        ) > 0.0001
                      )
                    {
                        $allData[$valMeasurementIndex][$timeIndex] =
                          $currentQcData{$measurementName}{$measurementTime}{val};

                        $numAppliedValues{val}++;
                    }
                }
                elsif ( defined $allData[$valMeasurementIndex][$timeIndex] ) {
                    $allData[$valMeasurementIndex][$timeIndex] = undef;

                    $numRemovedValues{val}++;
                }
            }

            if ( defined $medMeasurementIndex ) {

                if ( exists $currentQcData{$measurementName}{$measurementTime}{med} ) {
                    if (
                        !exists $allData[$medMeasurementIndex][$timeIndex]
                        || abs(
                            $allData[$medMeasurementIndex][$timeIndex] -
                              $currentQcData{$measurementName}{$measurementTime}{med}
                        ) > 0.0001
                      )
                    {
                        $allData[$medMeasurementIndex][$timeIndex] =
                          $currentQcData{$measurementName}{$measurementTime}{med};

                        $numAppliedValues{med}++;
                    }
                }
                elsif ( defined $allData[$medMeasurementIndex][$timeIndex] ) {
                    $allData[$medMeasurementIndex][$timeIndex] = undef;

                    $numRemovedValues{med}++;
                }
            }

            if ( defined $avgMeasurementIndex ) {

                if ( exists $currentQcData{$measurementName}{$measurementTime}{avg} ) {
                    if (
                        !exists $allData[$avgMeasurementIndex][$timeIndex]
                        || abs(
                            $allData[$avgMeasurementIndex][$timeIndex] -
                              $currentQcData{$measurementName}{$measurementTime}{avg}
                        ) > 0.0001
                      )
                    {
                        $allData[$avgMeasurementIndex][$timeIndex] =
                          $currentQcData{$measurementName}{$measurementTime}{avg};

                        $numAppliedValues{avg}++;
                    }
                }
                elsif ( defined $allData[$avgMeasurementIndex][$timeIndex] ) {
                    $allData[$avgMeasurementIndex][$timeIndex] = undef;

                    $numRemovedValues{avg}++;
                }
            }

            if ( defined $minMeasurementIndex ) {

                if ( exists $currentQcData{$measurementName}{$measurementTime}{min} ) {
                    if (
                        !exists $allData[$minMeasurementIndex][$timeIndex]
                        || abs(
                            $allData[$minMeasurementIndex][$timeIndex] -
                              $currentQcData{$measurementName}{$measurementTime}{min}
                        ) > 0.0001
                      )
                    {
                        $allData[$minMeasurementIndex][$timeIndex] =
                          $currentQcData{$measurementName}{$measurementTime}{min};

                        $numAppliedValues{min}++;
                    }
                }
                elsif ( defined $allData[$minMeasurementIndex][$timeIndex] ) {
                    $allData[$minMeasurementIndex][$timeIndex] = undef;

                    $numRemovedValues{min}++;
                }
            }

            if ( defined $maxMeasurementIndex ) {

                if ( exists $currentQcData{$measurementName}{$measurementTime}{max} ) {
                    if (
                        !exists $allData[$maxMeasurementIndex][$timeIndex]
                        || abs(
                            $allData[$maxMeasurementIndex][$timeIndex] -
                              $currentQcData{$measurementName}{$measurementTime}{max}
                        ) > 0.0001
                      )
                    {
                        $allData[$maxMeasurementIndex][$timeIndex] =
                          $currentQcData{$measurementName}{$measurementTime}{max};

                        $numAppliedValues{max}++;
                    }
                }
                elsif ( defined $allData[$maxMeasurementIndex][$timeIndex] ) {
                    $allData[$maxMeasurementIndex][$timeIndex] = undef;

                    $numRemovedValues{max}++;
                }
            }

            if ( defined $stdMeasurementIndex ) {

                if ( exists $currentQcData{$measurementName}{$measurementTime}{std} ) {
                    if (
                        !exists $allData[$stdMeasurementIndex][$timeIndex]
                        || abs(
                            $allData[$stdMeasurementIndex][$timeIndex] -
                              $currentQcData{$measurementName}{$measurementTime}{std}
                        ) > 0.0001
                      )
                    {
                        $allData[$stdMeasurementIndex][$timeIndex] =
                          $currentQcData{$measurementName}{$measurementTime}{std};

                        $numAppliedValues{std}++;
                    }
                }
                elsif ( defined $allData[$stdMeasurementIndex][$timeIndex] ) {
                    $allData[$stdMeasurementIndex][$timeIndex] = undef;

                    $numRemovedValues{std}++;
                }
            }
        }

        my @appliedCounts;
        my @removedCounts;
        foreach my $type ( 'val', 'med', 'avg', 'min', 'max', 'std' ) {
            push @appliedCounts, $numAppliedValues{$type} . " $type" if exists $numAppliedValues{$type};
            push @removedCounts, $numRemovedValues{$type} . " $type" if exists $numRemovedValues{$type};
        }

        $log->info( "[$fullMeasurementName] applied "
              . join( ", ", @appliedCounts )
              . " between $firstMeasurementTime and $lastMeasurementTime" )
          if @appliedCounts;
        $log->info( "[$fullMeasurementName] removed "
              . join( ", ", @removedCounts )
              . " between $firstMeasurementTime and $lastMeasurementTime" )
          if @removedCounts;

        my @allMeasurementIndexes;
        push @allMeasurementIndexes, $valMeasurementIndex
          if exists $numAppliedValues{val} || exists $numRemovedValues{val};
        push @allMeasurementIndexes, $medMeasurementIndex
          if exists $numAppliedValues{med} || exists $numRemovedValues{med};
        push @allMeasurementIndexes, $avgMeasurementIndex
          if exists $numAppliedValues{avg} || exists $numRemovedValues{avg};
        push @allMeasurementIndexes, $minMeasurementIndex
          if exists $numAppliedValues{min} || exists $numRemovedValues{min};
        push @allMeasurementIndexes, $maxMeasurementIndex
          if exists $numAppliedValues{max} || exists $numRemovedValues{max};
        push @allMeasurementIndexes, $stdMeasurementIndex
          if exists $numAppliedValues{std} || exists $numRemovedValues{std};

        # Update the first and last timestamps
        foreach my $measurementIndex (@allMeasurementIndexes) {
            updateFirstTimestamp( $measurementIndex, $firstMeasurementTime );
            updateLastTimestamp( $measurementIndex, $lastMeasurementTime );
        }
    }
}

################################################################################
# Import the manual QC flags and updated values from the database
{
    my $dbh = DBI->connect( "DBI:Pg:dbname=$sourceDB;host=$pgHost", $pgUser, $pgPassword, { 'RaiseError' => 1 } );

    foreach my $sensorNode ( sort keys %allTables ) {
        foreach my $tableName ( sort keys %{ $allTables{$sensorNode} } ) {
            applyManualQC( $dbh, "sn_qc", $sensorNode, $tableName );
        }
    }
}

################################################################################
# Inheirit QC flags to derived measurements
sub inheiritQCFlag {
    my ( $sourceMeasurementIndex, $destMeasurementIndex, $timeIndex ) = @_;

    return
         unless exists $allMeasurements[$sourceMeasurementIndex]{qcMeasurementIndex}
      && exists $allMeasurements[$sourceMeasurementIndex]{qlMeasurementIndex}
      && exists $allMeasurements[$destMeasurementIndex]{qcMeasurementIndex}
      && exists $allMeasurements[$destMeasurementIndex]{qlMeasurementIndex};

    my $sourceQCIndex = $allMeasurements[$sourceMeasurementIndex]{qcMeasurementIndex};

    return unless defined $qcData[$sourceQCIndex]{$timeIndex};

    my $destQCIndex = $allMeasurements[$destMeasurementIndex]{qcMeasurementIndex};

    $qcData[$destQCIndex]{$timeIndex} = $qcData[$sourceQCIndex]{$timeIndex}
      unless defined $qcData[$destQCIndex]{$timeIndex};

    # Also inheirit the Quality level if the Quality flag was set
    my $sourceQLIndex = $allMeasurements[$sourceMeasurementIndex]{qlMeasurementIndex};

    return unless defined $qcData[$sourceQLIndex]{$timeIndex};

    my $destQLIndex = $allMeasurements[$destMeasurementIndex]{qlMeasurementIndex};

    $qcData[$destQLIndex]{$timeIndex} = $qcData[$sourceQLIndex]{$timeIndex}
      unless defined $qcData[$destQLIndex]{$timeIndex};
}

################################################################################
# Support both implicit and custom calculations
my @pendingCalculations;

################################################################################
# Detect, clip and flag rain gauge calibrations
{
    foreach my $currentMeasurement (@allMeasurements) {
        next
          unless $currentMeasurement->{dataTable} eq $fiveMinuteTable
          && $currentMeasurement->{measurementName} eq "Rain"
          && $currentMeasurement->{sensorNode} ne "Lookout";

        my $sensorNode         = $currentMeasurement->{sensorNode};
        my $measurementIndex   = $currentMeasurement->{measurementIndex};
        my $qlMeasurementIndex = $currentMeasurement->{qlMeasurementIndex};
        my $qcMeasurementIndex = $currentMeasurement->{qcMeasurementIndex};

        my $startTimeIndex = 0;
        while ( !defined $allData[$measurementIndex][$startTimeIndex] && $startTimeIndex < $fiveMinSamplingTimes ) {
            $startTimeIndex++;
        }

        my $endTimeTimex = $fiveMinSamplingTimes;
        while ( !defined $allData[$measurementIndex][$endTimeTimex] && $endTimeTimex > 0 ) {
            $endTimeTimex--;
        }

        # Don't remove in the most recent week (2016 five minute intervals)
        $endTimeTimex -= 2016;
        next if $endTimeTimex - $startTimeIndex < 12;

        my $intervalStartIndex;
        my $intervalTotalRain;
        my $intervalMaxRain;

        foreach my $timeIndex ( $startTimeIndex .. $endTimeTimex ) {

            my $rainAmount = $allData[$measurementIndex][$timeIndex];

            if ( defined $rainAmount && $rainAmount > 0 ) {

                # Manually apply calibration factor to BuxtonEast recorded measurements
                if (   $sensorNode eq "BuxtonEast"
                    && $fiveMinSamplingTime[$timeIndex] lt "2017-02-04"
                    && !defined $qcData[$qlMeasurementIndex]{$timeIndex} )
                {
                    $rainAmount *= 1.9;
                    $allData[$measurementIndex][$timeIndex] = $rainAmount;
                }

                if ( defined $intervalStartIndex ) {
                    $intervalTotalRain += $rainAmount;    # Accumulating
                    $intervalMaxRain = $rainAmount if $rainAmount > $intervalMaxRain;
                }
                else {
                    $intervalStartIndex = $timeIndex;     # New interval
                    $intervalTotalRain  = $rainAmount;
                    $intervalMaxRain    = $rainAmount;
                }
            }
            elsif ( defined $intervalStartIndex ) {       # End of rain interval

                my $calibrationTarget = 22;
                $calibrationTarget = 13.5 if $sensorNode eq "BuxtonEast";

                my $intervalDuration = 5 * ( $timeIndex - $intervalStartIndex );

                # Look for continuous intervals one hour or less, where the total amount
                # of accumulated rain is between 90% and 100% of the target for a single
                # calibration event or two back-to-back calibration events
                if (
                    $intervalDuration <= 60
                    && (
                        (
                               $intervalTotalRain >= ( 0.9 * $calibrationTarget )
                            && $intervalTotalRain <= ( 1.1 * $calibrationTarget )
                        )
                        || (   $intervalTotalRain >= ( 1.8 * $calibrationTarget )
                            && $intervalTotalRain <= ( 2.2 * $calibrationTarget ) )
                    )
                  )
                {
                    foreach my $clippedTimeIndex ( $intervalStartIndex .. ( $timeIndex - 1 ) ) {
                        my $fiveMinValue = $allData[$measurementIndex][$clippedTimeIndex];

                        $allData[$measurementIndex][$clippedTimeIndex] = 0;
                        $qcData[$qcMeasurementIndex]{$clippedTimeIndex} =
"CE:Auto:originalValue=$fiveMinValue,calibrationAmount=$intervalTotalRain,calibrationDuration=$intervalDuration"
                          if defined $qcMeasurementIndex;
                    }

                    $log->info( "[$sensorNode] $intervalTotalRain mm of precipitation measured"
                          . " during potential calibration event ending at "
                          . $fiveMinSamplingTime[ $timeIndex - 1 ]
                          . " ($intervalDuration minutes long)"
                          . ", max $intervalMaxRain mm/5 min" );

                }
                undef $intervalStartIndex;
                undef $intervalTotalRain;
                undef $intervalMaxRain;
            }
        }

        my $lastHourlyTimestamp = $allTables{$sensorNode}{$oneHourTable}{lastTimestamp};
        my $lastDailyTimestamp = substr( $lastHourlyTimestamp, 0, 10 ) . " 00:00:00";

        # Calculate the 1 hour rain amounts from the five minute data
        my %hourlyRainCalculation;
        $hourlyRainCalculation{sensorNode}          = $sensorNode;
        $hourlyRainCalculation{tableName}           = $oneHourTable;
        $hourlyRainCalculation{measurementName}     = "Rain";
        $hourlyRainCalculation{measurementUnit}     = "mm";
        $hourlyRainCalculation{measurementFunction} = "1 hour total";
        $hourlyRainCalculation{calculation}         = "total";
        $hourlyRainCalculation{parameters} =
          "measurement=$sensorNode.$fiveMinuteTable.Rain,intervals=12,lastTimestamp=$lastHourlyTimestamp";

        push @pendingCalculations, \%hourlyRainCalculation;

        my %dailyRainCalculation;
        $dailyRainCalculation{sensorNode}          = $sensorNode;
        $dailyRainCalculation{tableName}           = $oneDayTable;
        $dailyRainCalculation{measurementName}     = "24hourRain";
        $dailyRainCalculation{measurementUnit}     = "mm";
        $dailyRainCalculation{measurementFunction} = "1 day total";
        $dailyRainCalculation{calculation}         = "total";
        $dailyRainCalculation{parameters} =
          "measurement=$sensorNode.$oneHourTable.Rain,intervals=24,lastTimestamp=$lastDailyTimestamp";

        push @pendingCalculations, \%dailyRainCalculation;
    }
}

################################################################################
# Flag 5 minute intervals where one or more 5 second interval had 3 or more tips
{
    my $firstQueriedTimestamp = $fiveMinSamplingTime[0];

    my $dbh = DBI->connect( "DBI:Pg:dbname=$sourceDB;host=$pgHost", $pgUser, $pgPassword, { 'RaiseError' => 1 } );

    my $sth =
      $dbh->prepare( "SELECT sensor_node, measurement_time at time zone 'PST', num_tips, wind_speed"
          . " FROM sn.rain_gauge_tips WHERE measurement_time>=? AND num_tips>=3 ORDER BY num_tips DESC, wind_speed DESC"
      );
    $sth->execute("$firstQueriedTimestamp-0800");

    my $rows = [];    # cache for batches of rows
    while (
        my $row = (
            shift(@$rows) ||    # get row from cache, or reload cache:
              shift( @{ $rows = $sth->fetchall_arrayref( undef, 10000 ) || [] } )
        )
      )
    {
        my $sensorNode      = $row->[0];
        my $measurementTime = $row->[1];
        my $numTips         = $row->[2];
        my $windSpeed       = $row->[3];

        my $measurementIndex = getMeasurementIndex( $sensorNode, $fiveMinuteTable, "Rain" );
        next unless defined $measurementIndex;

        my $qcMeasurementIndex = $allMeasurements[$measurementIndex]{qcMeasurementIndex};
        next unless defined $qcMeasurementIndex;

        my $timeIndex = getNextTimeIndex( $measurementIndex, $measurementTime );
        next unless defined $timeIndex;

        my $qcFlag = "SV:Auto:$numTips tips counted during 5 second interval";
        $qcFlag .= ",windSpeed=$windSpeed" if defined $windSpeed;

        $log->debug("[$sensorNode.$fiveMinuteTable.Rain:$measurementTime] $qcFlag");

        next if exists $qcData[$qcMeasurementIndex]{$timeIndex};

        $qcData[$qcMeasurementIndex]{$timeIndex} = $qcFlag;
    }
}

################################################################################
# Parse clipped measurements
my $clippedMeasurementsWorksheet = $specificationWorkbook->worksheet('ClippedMeasurements');
if ( defined $clippedMeasurementsWorksheet ) {
    my @columnNames;
    my ( undef, $maxCols ) = $clippedMeasurementsWorksheet->col_range();
    my ( undef, $maxRows ) = $clippedMeasurementsWorksheet->row_range();

    for my $columnNum ( 0 .. $maxCols ) {
        my $cellValue = getCellValue( $clippedMeasurementsWorksheet, 0, $columnNum );
        $columnNames[$columnNum] = $cellValue if defined $cellValue;
    }

    for my $rowNum ( 1 .. $maxRows ) {
        my $sensorNode;
        my $measurementNames;
        my $firstTimestamp;
        my $lastTimestamp;
        my $qcFlag;
        my $qcComment;

        for my $columnNum ( 0 .. $maxCols ) {
            next unless defined $columnNames[$columnNum];

            my $cellValue = getCellValue( $clippedMeasurementsWorksheet, $rowNum, $columnNum );
            next unless defined $cellValue;

            if ( uc( $columnNames[$columnNum] ) eq "STATIONNAME" ) {
                $sensorNode = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "MEASUREMENTNAMES" ) {
                $measurementNames = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "FIRSTTIMESTAMP" ) {
                $firstTimestamp = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "LASTTIMESTAMP" ) {
                $lastTimestamp = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "QCFLAG" ) {
                $qcFlag = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "COMMENT/DESCRIPTION" ) {
                $qcComment = $cellValue;
            }
            else {
                $log->debug( "WARNING: Unrecognized column heading "
                      . $columnNames[$columnNum]
                      . " found in CalculatedMeasurements worksheet" );
            }
        }

        next
          unless defined $sensorNode
          && defined $measurementNames
          && defined $firstTimestamp;

        $qcFlag .= ":Auto" if defined $qcFlag && index( uc($qcFlag), "AUTO" ) < 0;
        $qcFlag .= ":$qcComment" if defined $qcFlag && defined $qcComment;

        foreach my $measurementName ( split /,/, $measurementNames ) {
            my $parameters = "firstTimestamp=$firstTimestamp";
            $parameters .= ",lastTimestamp=$lastTimestamp" if defined $lastTimestamp;
            $parameters .= ",qcFlag=$qcFlag" if defined $qcFlag;

            foreach my $tableName ( "$oneHourTable", "$fiveMinuteTable" ) {
                my %currentCalculation = (
                    sensorNode      => $sensorNode,
                    tableName       => $tableName,
                    measurementName => $measurementName,
                    calculation     => "clip",
                    parameters      => $parameters
                );

                $log->info("[$sensorNode.$tableName.$measurementName] Clipping measurements $parameters");

                push @pendingCalculations, \%currentCalculation;
            }
        }
    }
}

################################################################################
# Add implicit calculations
{
    my $lastCalculatedParent;
    foreach my $currentMeasurement (@allMeasurements) {
        next
          unless defined $currentMeasurement->{isReferenced} && exists $currentMeasurement->{clipCalculation};

        next
          if defined $currentMeasurement->{parentMeasurementIndex}
          && defined $lastCalculatedParent
          && $currentMeasurement->{parentMeasurementIndex} == $lastCalculatedParent;

        push @pendingCalculations, $currentMeasurement->{clipCalculation};
        $lastCalculatedParent = $currentMeasurement->{parentMeasurementIndex};
    }
    foreach my $currentMeasurement (@allMeasurements) {
        next unless defined $currentMeasurement->{isReferenced} && exists $currentMeasurement->{airPressureCalculation};

        push @pendingCalculations, $currentMeasurement->{airPressureCalculation};
        $lastCalculatedParent = $currentMeasurement->{parentMeasurementIndex};
    }
    foreach my $currentMeasurement (@allMeasurements) {
        next unless defined $currentMeasurement->{isReferenced} && exists $currentMeasurement->{rangeClipCalculation};

        next
          if defined $currentMeasurement->{parentMeasurementIndex}
          && defined $lastCalculatedParent
          && $currentMeasurement->{parentMeasurementIndex} == $lastCalculatedParent;

        push @pendingCalculations, $currentMeasurement->{rangeClipCalculation};
        $lastCalculatedParent = $currentMeasurement->{parentMeasurementIndex};
    }
}

################################################################################
# Parse custom calculations
my $calculatedMeasurementsWorksheet = $specificationWorkbook->worksheet('CalculatedMeasurements');
if ( defined $calculatedMeasurementsWorksheet ) {
    my @columnNames;
    my ( undef, $maxCols ) = $calculatedMeasurementsWorksheet->col_range();
    my ( undef, $maxRows ) = $calculatedMeasurementsWorksheet->row_range();

    for my $columnNum ( 0 .. $maxCols ) {
        my $cellValue = getCellValue( $calculatedMeasurementsWorksheet, 0, $columnNum );
        $columnNames[$columnNum] = $cellValue if defined $cellValue;
    }

    for my $rowNum ( 1 .. $maxRows ) {
        my %currentCalculation;
        my $sensorNode;
        my $tableName;
        my $measurementName;
        my $measurementUnit;
        my $measurementFunction;
        my $calculation;
        my $parameters;
        my %parameterValues;

        for my $columnNum ( 0 .. $maxCols ) {
            next unless defined $columnNames[$columnNum];

            my $cellValue = getCellValue( $calculatedMeasurementsWorksheet, $rowNum, $columnNum );
            next unless length $cellValue;

            if ( uc( $columnNames[$columnNum] ) eq "STATIONNAME" ) {
                $currentCalculation{sensorNode} = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "TABLENAME" ) {
                $currentCalculation{tableName} = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "MEASUREMENTNAME" ) {
                $currentCalculation{measurementName} = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "MEASUREMENTUNIT" ) {
                $currentCalculation{measurementUnit} = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "MEASUREMENTFUNCTION" ) {
                $currentCalculation{measurementFunction} = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "CALCULATION" ) {
                $currentCalculation{calculation} = $cellValue;
            }
            elsif ( uc( $columnNames[$columnNum] ) eq "PARAMETERS" ) {
                $currentCalculation{parameters} = $cellValue;
            }
            else {
                $log->debug( "WARNING: Unrecognized column heading "
                      . $columnNames[$columnNum]
                      . " found in CalculatedMeasurements worksheet" );
            }
        }

        push @pendingCalculations, \%currentCalculation;
    }
}

################################################################################
# Clip and flag all associated measurements
sub clipAndFlagMeasurement {
    my ( $measurementIndex, $timeIndex, $qcFlag ) = @_;

    my $parentMeasurementIndex = $allMeasurements[$measurementIndex]{parentMeasurementIndex};
    return unless defined $parentMeasurementIndex;

    my @clippedMeasurements;
    foreach my $childMeasurementIndex ( @{ $allMeasurements[$parentMeasurementIndex]{childMeasurementIndexes} } ) {
        next
          unless exists $allData[$childMeasurementIndex][$timeIndex]
          && defined $allData[$childMeasurementIndex][$timeIndex];

        push @clippedMeasurements, $allMeasurements[$childMeasurementIndex]{measurementName} . "="
          . $allData[$childMeasurementIndex][$timeIndex];

        $allData[$childMeasurementIndex][$timeIndex] = undef;
    }

    return unless @clippedMeasurements;

    my $qcMeasurementIndex = $allMeasurements[$measurementIndex]{qcMeasurementIndex};
    return unless defined $qcMeasurementIndex;

    $qcData[$qcMeasurementIndex]{$timeIndex} = "$qcFlag:" . join( ",", @clippedMeasurements )
      unless exists $qcData[$qcMeasurementIndex]{$timeIndex};

    $qcData[ $allMeasurements[$measurementIndex]{uqlMeasurementIndex} ]{$timeIndex} = 4;    # UNESCO Failed
}

################################################################################
# Aggregate hourly measurements from 5 minute measurements
sub aggregateMeasurement {
    my ( $destMeasurementIndex, $sourceMeasurementIndex, $measurementsAdded ) = @_;

    my $destSampleInterval = $allTables{ $allMeasurements[$destMeasurementIndex]{sensorNode} }
      { $allMeasurements[$destMeasurementIndex]{dataTable} }{sampleInterval};
    my $sourceSampleInterval = $allTables{ $allMeasurements[$sourceMeasurementIndex]{sensorNode} }
      { $allMeasurements[$sourceMeasurementIndex]{dataTable} }{sampleInterval};

    return unless $destSampleInterval > $sourceSampleInterval;
    my $samplesPerInterval = $destSampleInterval / $sourceSampleInterval;

    my $destSamplingTime;
    if ( $destSampleInterval == 1 ) {
        $destSamplingTime = \@oneMinSamplingTime;
    }
    elsif ( $destSampleInterval == 5 ) {
        $destSamplingTime = \@fiveMinSamplingTime;
    }
    elsif ( $destSampleInterval == 60 ) {
        $destSamplingTime = \@hourlySamplingTime;
    }
    elsif ( $destSampleInterval == 1440 ) {
        $destSamplingTime = \@dailySamplingTime;
    }
    else {
        $log->warn("WARNING: Ignoring unsupported sample interval $destSampleInterval");
        return;
    }

    my $sensorNode      = $allMeasurements[$destMeasurementIndex]{sensorNode};
    my $dataTable       = $allMeasurements[$destMeasurementIndex]{dataTable};
    my $measurementName = $allMeasurements[$destMeasurementIndex]{measurementName};

    # Don't calculate average and median dischage rates and volumes
    if ( index( lc($measurementName), "discharge" ) == 0 ) {
        $log->warn("[$sensorNode.$dataTable.$measurementName] WARNING: Not aggregating discharge measurements");

        return;
    }

    $destMeasurementIndex = $allMeasurements[$destMeasurementIndex]{parentMeasurementIndex}
      if exists $allMeasurements[$destMeasurementIndex]{parentMeasurementIndex};

    my $destQlMeasurementIndex = $allMeasurements[$destMeasurementIndex]{qlMeasurementIndex};
    my $destQcMeasurementIndex = $allMeasurements[$destMeasurementIndex]{qcMeasurementIndex};
    my $avgValueMeasurementIndex;
    my $medValueMeasurementIndex;
    my $minValueMeasurementIndex;
    my $maxValueMeasurementIndex;

    foreach my $childMeasurementIndex ( @{ $allMeasurements[$destMeasurementIndex]{childMeasurementIndexes} } ) {
        $avgValueMeasurementIndex = $childMeasurementIndex
          if index( $allMeasurements[$childMeasurementIndex]{measurementName}, "_Avg" ) > 0;
        $medValueMeasurementIndex = $childMeasurementIndex
          if index( $allMeasurements[$childMeasurementIndex]{measurementName}, "_Med" ) > 0;
        $minValueMeasurementIndex = $childMeasurementIndex
          if index( $allMeasurements[$childMeasurementIndex]{measurementName}, "_Min" ) > 0;
        $maxValueMeasurementIndex = $childMeasurementIndex
          if index( $allMeasurements[$childMeasurementIndex]{measurementName}, "_Max" ) > 0;
    }

    $sourceMeasurementIndex = $allMeasurements[$sourceMeasurementIndex]{parentMeasurementIndex}
      if exists $allMeasurements[$sourceMeasurementIndex]{parentMeasurementIndex};

    my $sourceQlMeasurementIndex = $allMeasurements[$sourceMeasurementIndex]{qlMeasurementIndex};
    my $sourceQcMeasurementIndex = $allMeasurements[$sourceMeasurementIndex]{qcMeasurementIndex};
    my $sourceAvgMeasurementIndex;
    my $sourceMedMeasurementIndex;
    my $sourceMinMeasurementIndex;
    my $sourceMaxMeasurementIndex;

    my $minFirstSourceTimestamp;
    my $maxLastSourceTimestamp;

    foreach my $childMeasurementIndex ( @{ $allMeasurements[$sourceMeasurementIndex]{childMeasurementIndexes} } ) {

        next unless $allMeasurements[$childMeasurementIndex]{isReferenced};

        my $firstTimestamp = $allMeasurements[$childMeasurementIndex]{firstTimestamp};
        my $lastTimestamp  = $allMeasurements[$childMeasurementIndex]{lastTimestamp};
        next unless $firstTimestamp && $lastTimestamp;

        $minFirstSourceTimestamp = $firstTimestamp
          if !defined $minFirstSourceTimestamp || $firstTimestamp lt $minFirstSourceTimestamp;
        $maxLastSourceTimestamp = $lastTimestamp
          if !defined $maxLastSourceTimestamp || $lastTimestamp gt $maxLastSourceTimestamp;

        $sourceAvgMeasurementIndex = $childMeasurementIndex
          if index( $allMeasurements[$childMeasurementIndex]{measurementName}, "_Avg" ) > 0;
        $sourceMedMeasurementIndex = $childMeasurementIndex
          if index( $allMeasurements[$childMeasurementIndex]{measurementName}, "_Med" ) > 0;
        $sourceMinMeasurementIndex = $childMeasurementIndex
          if index( $allMeasurements[$childMeasurementIndex]{measurementName}, "_Min" ) > 0;
        $sourceMaxMeasurementIndex = $childMeasurementIndex
          if index( $allMeasurements[$childMeasurementIndex]{measurementName}, "_Max" ) > 0;
    }

    $sourceMeasurementIndex = undef unless $allMeasurements[$sourceMeasurementIndex]{isReferenced};
    $sourceMeasurementIndex = $sourceAvgMeasurementIndex unless $sourceMeasurementIndex;

    return unless $sourceMeasurementIndex && $minFirstSourceTimestamp && $maxLastSourceTimestamp;

    $sourceAvgMeasurementIndex = $sourceMeasurementIndex    unless $sourceAvgMeasurementIndex;
    $sourceMedMeasurementIndex = $sourceAvgMeasurementIndex unless $sourceMedMeasurementIndex;
    $sourceMinMeasurementIndex = $sourceAvgMeasurementIndex unless $sourceMinMeasurementIndex;
    $sourceMaxMeasurementIndex = $sourceAvgMeasurementIndex unless $sourceMaxMeasurementIndex;

    my $firstDestTimeIndex = getNextTimeIndex( $destMeasurementIndex, $minFirstSourceTimestamp );
    my $lastDestTimeIndex  = getNextTimeIndex( $destMeasurementIndex, $maxLastSourceTimestamp );

    $log->info( "[$sensorNode.$dataTable.$measurementName] Aggregating measurements between "
          . $destSamplingTime->[$firstDestTimeIndex] . " and "
          . $destSamplingTime->[$lastDestTimeIndex] );

    ${$measurementsAdded} = 0;
    my $firstTimeIndex;
    my $lastTimeIndex;
    foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

        my $sourceTimeIndex = $destTimeIndex * $samplesPerInterval;

        my $qualityLevel;
        my $qualityLevels = 0;
        my $flaggedValues = 0;
        my $foundValues   = 0;
        my $currentTotal  = 0;
        my @currentMedianValues;
        my $destValue;
        my $minValue;
        my $maxValue;
        my $commonQcFlag;
        my %qcFlags;

        foreach my $index ( 0 .. ( $samplesPerInterval - 1 ) ) {

            if ( defined $sourceQlMeasurementIndex
                && exists $qcData[$sourceQlMeasurementIndex]{$sourceTimeIndex} )
            {
                $qualityLevels++;

                # Set the aggregated quality level to the minimum of the source quality levels
                $qualityLevel = $qcData[$sourceQlMeasurementIndex]{$sourceTimeIndex}
                  if !defined $qualityLevel || $qualityLevel > $qcData[$sourceQlMeasurementIndex]{$sourceTimeIndex};
            }

            if ( defined $sourceQcMeasurementIndex
                && exists $qcData[$sourceQcMeasurementIndex]{$sourceTimeIndex} )
            {
                # Handle common case where all QC flags are identical
                if ( defined $commonQcFlag ) {
                    $commonQcFlag = "none" if $commonQcFlag ne $qcData[$sourceQcMeasurementIndex]{$sourceTimeIndex};
                }
                else {
                    $commonQcFlag = $qcData[$sourceQcMeasurementIndex]{$sourceTimeIndex};
                }

                $flaggedValues++;
                foreach my $qcFlag ( split /:/, $qcData[$sourceQcMeasurementIndex]{$sourceTimeIndex} ) {
                    next unless ( length($qcFlag) == 2 || length($qcFlag) == 3 ) && $qcFlag eq uc $qcFlag;
                    $qcFlags{$qcFlag}++;
                }
            }

            if ( $index <= 1 && !defined $destValue ) {
                $destValue = $allData[$sourceMeasurementIndex][$sourceTimeIndex];
                $destValue = $allData[$sourceAvgMeasurementIndex][$sourceTimeIndex] if !defined $destValue;
            }

            if ( defined $allData[$sourceAvgMeasurementIndex][$sourceTimeIndex] ) {
                $currentTotal += $allData[$sourceAvgMeasurementIndex][$sourceTimeIndex];
                $foundValues++;
            }

            if ( defined $allData[$sourceMedMeasurementIndex][$sourceTimeIndex] ) {
                push @currentMedianValues, $allData[$sourceMedMeasurementIndex][$sourceTimeIndex];
            }
            elsif ( defined $allData[$sourceAvgMeasurementIndex][$sourceTimeIndex] ) {
                push @currentMedianValues, $allData[$sourceAvgMeasurementIndex][$sourceTimeIndex];
            }

            if ( defined $allData[$sourceMinMeasurementIndex][$sourceTimeIndex] ) {
                $minValue = $allData[$sourceMinMeasurementIndex][$sourceTimeIndex]
                  unless defined $minValue && $minValue < $allData[$sourceMinMeasurementIndex][$sourceTimeIndex];
            }
            elsif ( defined $allData[$sourceAvgMeasurementIndex][$sourceTimeIndex] ) {
                $minValue = $allData[$sourceAvgMeasurementIndex][$sourceTimeIndex]
                  unless defined $minValue && $minValue < $allData[$sourceAvgMeasurementIndex][$sourceTimeIndex];
            }

            if ( defined $allData[$sourceMaxMeasurementIndex][$sourceTimeIndex] ) {
                $maxValue = $allData[$sourceMaxMeasurementIndex][$sourceTimeIndex]
                  unless defined $maxValue && $maxValue > $allData[$sourceMaxMeasurementIndex][$sourceTimeIndex];
            }
            elsif ( defined $allData[$sourceAvgMeasurementIndex][$sourceTimeIndex] ) {
                $maxValue = $allData[$sourceAvgMeasurementIndex][$sourceTimeIndex]
                  unless defined $maxValue && $maxValue > $allData[$sourceAvgMeasurementIndex][$sourceTimeIndex];
            }

            $sourceTimeIndex--;
        }

        next
          unless defined $destValue || defined $minValue || defined $maxValue || $flaggedValues > 0 || $foundValues > 0;

        # Skip last destination measurement if there are not enough source measurements
        next if $foundValues < $samplesPerInterval && $destTimeIndex == $lastDestTimeIndex;

        if ( !defined $firstTimeIndex ) {
            $firstTimeIndex = $destTimeIndex;

            # Pre-allocated memory for the new values
            maxSizeMeasurementArray($destMeasurementIndex);
            maxSizeMeasurementArray($avgValueMeasurementIndex) if defined $avgValueMeasurementIndex;
            maxSizeMeasurementArray($medValueMeasurementIndex) if defined $medValueMeasurementIndex;
            maxSizeMeasurementArray($minValueMeasurementIndex) if defined $minValueMeasurementIndex;
            maxSizeMeasurementArray($maxValueMeasurementIndex) if defined $maxValueMeasurementIndex;
        }

        $lastTimeIndex = $destTimeIndex;

        ${$measurementsAdded}++;

        $allData[$destMeasurementIndex][$destTimeIndex] = $destValue
          if defined $destValue && !defined $allData[$destMeasurementIndex][$destTimeIndex];
        $allData[$medValueMeasurementIndex][$destTimeIndex] = median(@currentMedianValues)
          if defined $medValueMeasurementIndex
          && scalar(@currentMedianValues) > 0
          && !defined $allData[$medValueMeasurementIndex][$destTimeIndex];
        $allData[$avgValueMeasurementIndex][$destTimeIndex] = $currentTotal / $foundValues
          if defined $avgValueMeasurementIndex
          && $foundValues > 0
          && !defined $allData[$avgValueMeasurementIndex][$destTimeIndex];
        $allData[$minValueMeasurementIndex][$destTimeIndex] = $minValue
          if defined $minValueMeasurementIndex
          && defined $minValue
          && !defined $allData[$minValueMeasurementIndex][$destTimeIndex];
        $allData[$maxValueMeasurementIndex][$destTimeIndex] = $maxValue
          if defined $maxValueMeasurementIndex
          && defined $maxValue
          && !defined $allData[$maxValueMeasurementIndex][$destTimeIndex];

        $qcData[$destQlMeasurementIndex]{$destTimeIndex} = $qualityLevel
          if defined $qualityLevel && defined $destQlMeasurementIndex && $qualityLevels == $samplesPerInterval;

        my $missingMeasurements = $samplesPerInterval - $foundValues;
        $qcFlags{MV} += $missingMeasurements if $missingMeasurements;
        if ( $destQcMeasurementIndex && %qcFlags ) {

            my $aggregateFlag;
            if ( defined $commonQcFlag && $commonQcFlag ne "none" && $destSampleInterval <= 60 ) {
                $aggregateFlag = $commonQcFlag;
            }
            else {
                $aggregateFlag = join( ":", sort keys %qcFlags );
            }

            $aggregateFlag .= ":$missingMeasurements of $samplesPerInterval missing" if $missingMeasurements;
            $aggregateFlag .= ":$flaggedValues of $samplesPerInterval flagged"
              if $flaggedValues && $flaggedValues < $samplesPerInterval;

            $qcData[$destQcMeasurementIndex]{$destTimeIndex} = $aggregateFlag;
        }
    }

    return unless ${$measurementsAdded} > 0;

    updateFirstTimestamp( $destMeasurementIndex, $destSamplingTime->[$firstTimeIndex] );
    updateLastTimestamp( $destMeasurementIndex, $destSamplingTime->[$lastTimeIndex] );
    $allMeasurements[$destMeasurementIndex]{measurementCalculation} =
      getMeasurementCalculation($sourceMeasurementIndex);

    if ( defined $avgValueMeasurementIndex ) {
        updateFirstTimestamp( $avgValueMeasurementIndex, $destSamplingTime->[$firstTimeIndex] );
        updateLastTimestamp( $avgValueMeasurementIndex, $destSamplingTime->[$lastTimeIndex] );
        $allMeasurements[$avgValueMeasurementIndex]{measurementCalculation} =
          "avg(" . getMeasurementCalculation($sourceAvgMeasurementIndex) . ")";
    }

    if ( defined $medValueMeasurementIndex ) {
        updateFirstTimestamp( $medValueMeasurementIndex, $destSamplingTime->[$firstTimeIndex] );
        updateLastTimestamp( $medValueMeasurementIndex, $destSamplingTime->[$lastTimeIndex] );
        $allMeasurements[$medValueMeasurementIndex]{measurementCalculation} =
          "median(" . getMeasurementCalculation($sourceAvgMeasurementIndex) . ")";
    }

    if ( defined $minValueMeasurementIndex ) {
        updateFirstTimestamp( $minValueMeasurementIndex, $destSamplingTime->[$firstTimeIndex] );
        updateLastTimestamp( $minValueMeasurementIndex, $destSamplingTime->[$lastTimeIndex] );
        $allMeasurements[$minValueMeasurementIndex]{measurementCalculation} =
          "min(" . getMeasurementCalculation($sourceAvgMeasurementIndex) . ")";
    }

    if ( defined $maxValueMeasurementIndex ) {
        updateFirstTimestamp( $maxValueMeasurementIndex, $destSamplingTime->[$firstTimeIndex] );
        updateLastTimestamp( $maxValueMeasurementIndex, $destSamplingTime->[$lastTimeIndex] );
        $allMeasurements[$maxValueMeasurementIndex]{measurementCalculation} =
          "max(" . getMeasurementCalculation($sourceAvgMeasurementIndex) . ")";
    }
}

################################################################################
# Load a rating curve from the Excel specification file
sub loadRatingCurve {
    my ( $rcWorksheetName, $ratingCurveRef ) = @_;

    my $ratingCurveWorksheet = $specificationWorkbook->worksheet($rcWorksheetName);
    if ( !defined $ratingCurveWorksheet ) {
        $log->warn("WARNING: Failed to find rating curve worksheet \"$rcWorksheetName\"");
        return;
    }

    my @columnNames;
    my ( undef, $maxCols ) = $ratingCurveWorksheet->col_range();
    my ( undef, $maxRows ) = $ratingCurveWorksheet->row_range();

    my $startCol;
    my $stageCol;
    my $dischargeCol;
    my $minDischargeCol;
    my $maxDischargeCol;

    for my $columnNum ( 0 .. $maxCols ) {
        my $columnHeading = getCellValue( $ratingCurveWorksheet, 0, $columnNum );

        if ( uc($columnHeading) eq "START" ) {
            $startCol = $columnNum;
        }
        elsif ( uc($columnHeading) eq "STAGE" ) {
            $stageCol = $columnNum;
        }
        elsif ( uc($columnHeading) eq "DISCHARGE" ) {
            $dischargeCol = $columnNum;
        }
        elsif ( uc($columnHeading) eq "MINDISCHARGE" ) {
            $minDischargeCol = $columnNum;
        }
        elsif ( uc($columnHeading) eq "MAXDISCHARGE" ) {
            $maxDischargeCol = $columnNum;
        }
        else {
            $log->debug("Skipping unrecognized column \"$columnHeading\" in \"$rcWorksheetName\"");
        }
    }

    if (   !defined $startCol
        || !defined $stageCol
        || !defined $dischargeCol
        || !defined $minDischargeCol
        || !defined $maxDischargeCol )
    {
        $log->warn("WARNING: rating curve in worksheet \"$rcWorksheetName\" does not include required columns");
        return;
    }

    foreach my $rowNum ( 1 .. $maxRows ) {
        my $startTime    = getCellValue( $ratingCurveWorksheet, $rowNum, $startCol );
        my $stage        = getCellValue( $ratingCurveWorksheet, $rowNum, $stageCol );
        my $discharge    = getCellValue( $ratingCurveWorksheet, $rowNum, $dischargeCol );
        my $minDischarge = getCellValue( $ratingCurveWorksheet, $rowNum, $minDischargeCol );
        my $maxDischarge = getCellValue( $ratingCurveWorksheet, $rowNum, $maxDischargeCol );

        next
          unless defined $startTime
          && defined $stage
          && defined $discharge
          && defined $minDischarge
          && defined $maxDischarge;

        if ( length($startTime) < 18 && looks_like_number($startTime) ) {
            if ( !exists $convertedExcelTime{$startTime} ) {
                my $excelParser = DateTime::Format::Excel->new();
                my $excelDT     = $excelParser->parse_datetime($startTime);

                $convertedExcelTime{$startTime} = $excelDT->ymd('-') . ' ' . $excelDT->hms(':');

                $log->info( "[$rcWorksheetName] Converted Excel $startTime to " . $convertedExcelTime{$startTime} );
            }

            $startTime = $convertedExcelTime{$startTime};
        }

        my $mmStage = int( $stage * 10.0 + 0.5 );

        $ratingCurveRef->[$mmStage]{$startTime}{discharge}    = $discharge;
        $ratingCurveRef->[$mmStage]{$startTime}{minDischarge} = $minDischarge;
        $ratingCurveRef->[$mmStage]{$startTime}{maxDischarge} = $maxDischarge;
    }
}

################################################################################
# Apply all calculations
while ( my $currentCalculation = shift(@pendingCalculations) ) {

    my $sensorNode          = $currentCalculation->{sensorNode};
    my $tableName           = $currentCalculation->{tableName};
    my $measurementName     = $currentCalculation->{measurementName};
    my $measurementUnit     = $currentCalculation->{measurementUnit};
    my $measurementFunction = $currentCalculation->{measurementFunction};
    my $calculation         = $currentCalculation->{calculation};
    my $parameters          = $currentCalculation->{parameters};

    if ( !defined $sensorNode || !defined $tableName || !defined $measurementName || !defined $calculation ) {
        $log->warn("WARNING: Incomplete calculation specification found");
        next;
    }

    my $fullMeasurementName = "$sensorNode.$tableName.$measurementName";

    my $destMeasurementIndex = getMeasurementIndex( $sensorNode, $tableName, $measurementName );
    if ( !defined $destMeasurementIndex ) {
        $log->warn("WARNING: Ignoring calculation of recognized measurement \"$fullMeasurementName\"");
        next;
    }

    my $logMeasurementName = $fullMeasurementName;
    $logMeasurementName .= ":" . $allMeasurements[$destMeasurementIndex]{displayName}
      if defined $allMeasurements[$destMeasurementIndex]{displayName}
      && $allMeasurements[$destMeasurementIndex]{displayName} ne $measurementName;

    my %parameterValues;
    if ( length $parameters ) {
        foreach my $parameter ( split( /,/, $parameters ) ) {
            my ( $parameterName, $parameterValue ) =
              split( /=/, $parameter );
            if ( !defined $parameterName || !defined $parameterValue ) {
                $log->warn("WARNING: Ignoring unrecognized parameter $parameter");
                next;
            }

            $parameterValues{$parameterName} = $parameterValue;
        }
    }

    my $destSampleInterval = $allTables{$sensorNode}{$tableName}{sampleInterval};

    if ( !defined $destSampleInterval ) {
        $log->warn( "WARNING: Failed to find sample interval associated with $sensorNode.$tableName"
              . ", skipping calculation \"$fullMeasurementName\"" );
        next;
    }

    my $destSamplingTimeIndex;
    my $destSamplingTime;
    my $destSamplingTimes;
    if ( $destSampleInterval == 1 ) {
        $destSamplingTimeIndex = \%oneMinSamplingTimeIndex;
        $destSamplingTime      = \@oneMinSamplingTime;
        $destSamplingTimes     = $oneMinSamplingTimes;
    }
    elsif ( $destSampleInterval == 5 ) {
        $destSamplingTimeIndex = \%fiveMinSamplingTimeIndex;
        $destSamplingTime      = \@fiveMinSamplingTime;
        $destSamplingTimes     = $fiveMinSamplingTimes;
    }
    elsif ( $destSampleInterval == 60 ) {
        $destSamplingTimeIndex = \%hourlySamplingTimeIndex;
        $destSamplingTime      = \@hourlySamplingTime;
        $destSamplingTimes     = $hourlySamplingTimes;
    }
    elsif ( $destSampleInterval == 1440 ) {
        $destSamplingTimeIndex = \%dailySamplingTimeIndex;
        $destSamplingTime      = \@dailySamplingTime;
        $destSamplingTimes     = $dailySamplingTimes;
    }
    else {
        $log->warn("WARNING: Ignoring unsupported sample interval $destSampleInterval");
        next;
    }

    maxSizeMeasurementArray($destMeasurementIndex);

    my $firstDestTimestamp = $allTables{$sensorNode}{$tableName}{firstTimestamp};
    my $lastDestTimestamp  = $allTables{$sensorNode}{$tableName}{lastTimestamp};

    # Support expanding the destination table if performing calculations based on
    # measurements from a different table.  This was initially added to handle the
    # case of TSN1 where hourly measurements are initially calculated from five
    # minute measurements
    if ( exists $parameterValues{measurement} ) {
        my ( $sourceSensorNode, $sourceTableName, $sourceMeasurementName ) =
          split( /\./, $parameterValues{measurement} );
        if ( "$sourceSensorNode.$sourceTableName" ne "$sensorNode.$tableName" ) {

            if ( !defined $firstDestTimestamp
                || $firstDestTimestamp gt $allTables{$sourceSensorNode}{$sourceTableName}{firstTimestamp} )
            {
                my $nextTimeIndex = getNextTimeIndex( $destMeasurementIndex,
                    $allTables{$sourceSensorNode}{$sourceTableName}{firstTimestamp} );
                $firstDestTimestamp = $destSamplingTime->[$nextTimeIndex];
            }

            if ( !defined $lastDestTimestamp
                || $lastDestTimestamp lt $allTables{$sourceSensorNode}{$sourceTableName}{lastTimestamp} )
            {
                my $nextTimeIndex = getNextTimeIndex( $destMeasurementIndex,
                    $allTables{$sourceSensorNode}{$sourceTableName}{lastTimestamp} );
                $lastDestTimestamp = $destSamplingTime->[$nextTimeIndex];
            }
        }
    }

    if ( defined $parameterValues{lastTimestamp} ) {
        if ( $parameterValues{lastTimestamp} lt $destSamplingTime->[0] ) {
            $log->debug( "Skipping calculation $fullMeasurementName=$calculation, as "
                  . $parameterValues{lastTimestamp}
                  . " is before first sample time of "
                  . $destSamplingTime->[0] );
            next;
        }

        my $lastDestTimeIndex = getNextTimeIndex( $destMeasurementIndex, $parameterValues{lastTimestamp} );

        $lastDestTimestamp = $destSamplingTime->[$lastDestTimeIndex];
        if ( $lastDestTimestamp lt $firstDestTimestamp ) {
            $log->info( "WARNING: Skipping calculation $fullMeasurementName=$calculation"
                  . ", as $lastDestTimestamp is before first sample time of $firstDestTimestamp" );
            next;
        }

        $allTables{$sensorNode}{$tableName}{lastTimestamp} = $lastDestTimestamp
          if !exists $allTables{$sensorNode}{$tableName}{lastTimestamp}
          || $allTables{$sensorNode}{$tableName}{lastTimestamp} lt $lastDestTimestamp;
    }

    if ( defined $parameterValues{firstTimestamp} ) {
        my $firstDestTimeIndex = getNextTimeIndex( $destMeasurementIndex, $parameterValues{firstTimestamp} );
        $firstDestTimeIndex = 0 unless defined $firstDestTimeIndex;
        $firstDestTimestamp = $destSamplingTime->[$firstDestTimeIndex];

        $allTables{$sensorNode}{$tableName}{firstTimestamp} = $firstDestTimestamp
          if $allTables{$sensorNode}{$tableName}{firstTimestamp} gt $firstDestTimestamp;
    }

    next unless defined $firstDestTimestamp && defined $lastDestTimestamp;

    my $firstDestTimeIndex = $destSamplingTimeIndex->{$firstDestTimestamp};
    my $lastDestTimeIndex  = $destSamplingTimeIndex->{$lastDestTimestamp};

    if ( !defined $allMeasurements[$destMeasurementIndex]{isReferenced} ) {
        $log->debug( "Adding calculated measurement $fullMeasurementName"
              . " from $firstDestTimestamp to $lastDestTimestamp"
              . " using calcuation $calculation" );
    }
    else {
        $log->debug( "Updating calculated measurement $fullMeasurementName"
              . " from $firstDestTimestamp to $lastDestTimestamp"
              . " using calcuation $calculation" );
    }

    my $qlMeasurementIndex  = $allMeasurements[$destMeasurementIndex]{qlMeasurementIndex};
    my $qcMeasurementIndex  = $allMeasurements[$destMeasurementIndex]{qcMeasurementIndex};
    my $measurementsAdded   = 0;
    my $measurementsUpdated = 0;
    my $measurementsRemoved = 0;
    my $calculationString;
    if ( uc $calculation eq "NORMALIZEAIRPRESSURE" ) {
        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {
            my $currentAirPressure = $allData[$destMeasurementIndex][$destTimeIndex];
            next unless defined $currentAirPressure;

            # Scale up if air pressure captured in different units
            if ( $currentAirPressure >= 80 && $currentAirPressure <= 130 ) {
                $currentAirPressure *= 10.0;

                $allData[$destMeasurementIndex][$destTimeIndex] = $currentAirPressure;

                $measurementsUpdated++;
            }
            elsif ( $currentAirPressure >= 0.8 && $currentAirPressure <= 1.3 ) {
                $currentAirPressure *= 1000.0;    # From bar to hPa

                $allData[$destMeasurementIndex][$destTimeIndex] = $currentAirPressure;

                $measurementsUpdated++;
            }

            # Remove out-of-range air pressure values
            if ( $currentAirPressure < 800 ) {
                $measurementsRemoved++;

                clipAndFlagMeasurement( $destMeasurementIndex, $destTimeIndex, "BR:Auto" );
            }
            elsif ( $currentAirPressure > 1300 ) {
                $measurementsRemoved++;

                clipAndFlagMeasurement( $destMeasurementIndex, $destTimeIndex, "AR:Auto" );
            }
        }
    }
    elsif ( uc $calculation eq "AIRPRESSURETOWATERLEVEL" ) {
        my $fullSourceMeasurementNames = $parameterValues{stationAirPressure};
        $fullSourceMeasurementNames = $parameterValues{stationAirPressures}
          unless length $fullSourceMeasurementNames;
        my $primaryAirPressureIndex;
        my @secondaryAirPressureIndexes;
        foreach my $fullSourceMeasurementName ( split /&/, $fullSourceMeasurementNames ) {
            my $sourceMeasurementIndex = getMeasurementIndex($fullSourceMeasurementName);
            if ( !defined $sourceMeasurementIndex ) {
                $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                      . " referencing unspecified measurement \"$fullSourceMeasurementName\"" );
                next;
            }

            if ( !defined $primaryAirPressureIndex ) {
                $primaryAirPressureIndex = $sourceMeasurementIndex;
            }
            else {
                push @secondaryAirPressureIndexes, $sourceMeasurementIndex;
            }
        }

        if ( !defined $primaryAirPressureIndex ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement(s) \"$fullSourceMeasurementNames\"" );
            next;
        }

        my $measurementPrecision = $parameterValues{precision};
        my $precisionMultiplier  = 10**$measurementPrecision
          if defined $measurementPrecision;

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {
            my $currentAirPressure = $allData[$primaryAirPressureIndex][$destTimeIndex];
            if ( !defined $currentAirPressure ) {
                foreach my $secondaryAirPressureIndex (@secondaryAirPressureIndexes) {
                    $currentAirPressure = $allData[$secondaryAirPressureIndex][$destTimeIndex];
                    last if defined $currentAirPressure;
                }
            }
            if ( !defined $currentAirPressure ) {
                inheiritQCFlag( $primaryAirPressureIndex, $destMeasurementIndex, $destTimeIndex );
                next;
            }

            # Convert kPa or hPa to m of H2O
            my $mH2O;
            if ( $currentAirPressure > 800 && $currentAirPressure < 1300 ) {
                $mH2O = $currentAirPressure * 0.010197442889221097;
            }
            elsif ( $currentAirPressure > 80 && $currentAirPressure < 130 ) {
                $mH2O = $currentAirPressure * 0.10197442889221097;
            }
            elsif ( $currentAirPressure < 8 ) {
                $measurementsRemoved++;

                clipAndFlagMeasurement( $destMeasurementIndex, $destTimeIndex, "BR:Auto" );
                next;
            }
            elsif ( $currentAirPressure > 13 ) {
                $measurementsRemoved++;

                clipAndFlagMeasurement( $destMeasurementIndex, $destTimeIndex, "AR:Auto" );
                next;
            }
            else {
                next;
            }
            $mH2O = int( $mH2O * $precisionMultiplier + 0.5 ) / $precisionMultiplier
              if defined $precisionMultiplier;

            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $mH2O;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $mH2O;
        }

        if ( scalar @secondaryAirPressureIndexes ) {
            $calculationString = "AirPressureToH2O($fullSourceMeasurementNames)";
        }
        else {
            $calculationString = "AirPressureToH2O(" . getMeasurementCalculation($primaryAirPressureIndex) . ")";
        }
    }
    elsif ( uc $calculation eq "WATERPRESSURETOWATERLEVEL" ) {
        my $waterPressureMeasurementName = $parameterValues{waterPressure};
        if ( !defined $waterPressureMeasurementName ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . ", as waterPressure measurement not specified" );
            next;
        }
        my $waterPressureMeasurementIndex = getMeasurementIndex($waterPressureMeasurementName);
        if ( !defined $waterPressureMeasurementIndex ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$waterPressureMeasurementName\"" );
            next;
        }

        my $measurementPrecision = $parameterValues{precision};
        my $precisionMultiplier  = 10**$measurementPrecision
          if defined $measurementPrecision;

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {
            my $currentWaterPressure = $allData[$waterPressureMeasurementIndex][$destTimeIndex];
            if ( !defined $currentWaterPressure ) {
                inheiritQCFlag( $waterPressureMeasurementIndex, $destMeasurementIndex, $destTimeIndex );
                next;
            }

            # Convert from decibar to m of H2O
            my $mH2O = $currentWaterPressure * 1.01972;
            $mH2O = int( $mH2O * $precisionMultiplier + 0.5 ) / $precisionMultiplier
              if defined $precisionMultiplier;

            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $mH2O;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $mH2O;
        }

        $calculationString = "WaterPressureToH2O(" . getMeasurementCalculation($waterPressureMeasurementIndex) . ")";
    }
    elsif ( uc $calculation eq "COPY" ) {
        my $fullSourceMeasurementName = $parameterValues{measurement};
        my ( $sourceSensorNode, $sourceTableName, $sourceMeasurementName ) =
          split( /\./, $fullSourceMeasurementName );
        my $sourceMeasurementIndex = getMeasurementIndex($fullSourceMeasurementName);
        if ( !defined $sourceMeasurementIndex ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$fullSourceMeasurementName\"" );
            next;
        }

        my $sourceSampleInterval = $allTables{$sourceSensorNode}{$sourceTableName}{sampleInterval};
        my $sourceSamplingTimeIndex;
        if ( $sourceSampleInterval == 1440 ) {
            $sourceSamplingTimeIndex = \%dailySamplingTimeIndex;
        }
        elsif ( $sourceSampleInterval == 60 ) {
            $sourceSamplingTimeIndex = \%hourlySamplingTimeIndex;
        }
        elsif ( $sourceSampleInterval == 5 ) {
            $sourceSamplingTimeIndex = \%fiveMinSamplingTimeIndex;
        }
        elsif ( $sourceSampleInterval == 1 ) {
            $sourceSamplingTimeIndex = \%oneMinSamplingTimeIndex;
        }

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            # Don't calculate measurement values which have been QC flagged
            next if defined $qlMeasurementIndex && defined $qcData[$qlMeasurementIndex]{$destTimeIndex};

            my $sourceTimeIndex = $destTimeIndex;
            $sourceTimeIndex = $sourceSamplingTimeIndex->{ $destSamplingTime->[$destTimeIndex] }
              if $sourceSampleInterval < $destSampleInterval;

            my $sourceValue = $allData[$sourceMeasurementIndex][$sourceTimeIndex];
            next unless defined $sourceValue;

            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $sourceValue;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $sourceValue;
        }

        $calculationString = getMeasurementCalculation($sourceMeasurementIndex);
    }
    elsif ( uc $calculation eq "SCALE" ) {
        my $fullSourceMeasurementName = $parameterValues{measurement};
        my $sourceMeasurementIndex    = getMeasurementIndex($fullSourceMeasurementName);
        if ( !defined $sourceMeasurementIndex ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$fullSourceMeasurementName\"" );
            next;
        }

        my $measurementOffset     = $parameterValues{offset};
        my $measurementMultiplier = $parameterValues{multiplier};
        my $measurementDivisor    = $parameterValues{divisor};
        my $measurementPrecision  = $parameterValues{precision};
        my $precisionMultiplier   = 10**$measurementPrecision if $measurementPrecision;
        my $minimumValue          = $parameterValues{minimumValue};
        my $maximumValue          = $parameterValues{maximumValue};
        my $qcFlag                = $parameterValues{qcFlag};

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            # Don't calculate measurement values which have been QC flagged
            next if defined $qlMeasurementIndex && defined $qcData[$qlMeasurementIndex]{$destTimeIndex};

            inheiritQCFlag( $sourceMeasurementIndex, $destMeasurementIndex, $destTimeIndex );

            my $sourceMeasurementValue = $allData[$sourceMeasurementIndex][$destTimeIndex];
            next unless defined $sourceMeasurementValue;

            my $newMeasurementValue = $sourceMeasurementValue;
            $newMeasurementValue = $newMeasurementValue * $measurementMultiplier if defined $measurementMultiplier;
            $newMeasurementValue = $newMeasurementValue / $measurementDivisor    if defined $measurementDivisor;
            $newMeasurementValue += $measurementOffset if defined $measurementOffset;
            $newMeasurementValue = int( $newMeasurementValue * $precisionMultiplier + 0.5 ) / $precisionMultiplier
              if defined $precisionMultiplier;

            $newMeasurementValue = $minimumValue
              if defined $minimumValue && $newMeasurementValue < $minimumValue;

            $newMeasurementValue = $maximumValue
              if defined $maximumValue && $newMeasurementValue > $maximumValue;

            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $newMeasurementValue;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $newMeasurementValue;

            # Set the QC flag if passed in
            $qcData[$qcMeasurementIndex]{$destTimeIndex} = $qcFlag if defined $qcMeasurementIndex && defined $qcFlag;
        }

        $calculationString = "scale(" . getMeasurementCalculation($sourceMeasurementIndex);
        $calculationString .= ",offset=$measurementOffset"
          if defined $measurementOffset;
        $calculationString .= ",multiplier=$measurementMultiplier"
          if defined $measurementMultiplier;
        $calculationString .= ",divisor=$measurementDivisor"
          if defined $measurementDivisor;
        $calculationString .= ")";
    }
    elsif ( uc $calculation eq "DIFFERENCE" ) {
        my $fullSourceMeasurementName1 = $parameterValues{measurement1};
        my $sourceMeasurementIndex1    = getMeasurementIndex($fullSourceMeasurementName1);
        if ( !defined $sourceMeasurementIndex1 ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$fullSourceMeasurementName1\"" );
            next;
        }

        my $fullSourceMeasurementName2 = $parameterValues{measurement2};
        my $sourceMeasurementIndex2    = getMeasurementIndex($fullSourceMeasurementName2);
        if ( !defined $sourceMeasurementIndex2 ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$fullSourceMeasurementName2\"" );
            next;
        }

        my $measurementPrecision = $parameterValues{precision};
        my $precisionMultiplier  = 10**$measurementPrecision
          if defined $measurementPrecision;

        my $minimumValue = $parameterValues{minimumValue};
        my $maximumValue = $parameterValues{maximumValue};

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            # Don't calculate measurement values which have been QC flagged
            next if defined $qlMeasurementIndex && defined $qcData[$qlMeasurementIndex]{$destTimeIndex};

            my $measurementValue1 = $allData[$sourceMeasurementIndex1][$destTimeIndex];
            my $measurementValue2 = $allData[$sourceMeasurementIndex2][$destTimeIndex];
            if ( !defined $measurementValue1 || !defined $measurementValue2 ) {
                inheiritQCFlag( $sourceMeasurementIndex1, $destMeasurementIndex, $destTimeIndex );
                inheiritQCFlag( $sourceMeasurementIndex2, $destMeasurementIndex, $destTimeIndex );
                next;
            }

            my $measurementDifference = $measurementValue1 - $measurementValue2;
            $measurementDifference = int( $measurementDifference * $precisionMultiplier + 0.5 ) / $precisionMultiplier
              if defined $precisionMultiplier;

            $measurementDifference = $minimumValue
              if defined $minimumValue && $measurementDifference < $minimumValue;

            $measurementDifference = $maximumValue
              if defined $maximumValue && $measurementDifference > $maximumValue;

            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $measurementDifference;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $measurementDifference;
        }

        $calculationString =
            getMeasurementCalculation($sourceMeasurementIndex1) . " - "
          . getMeasurementCalculation($sourceMeasurementIndex2);
    }
    elsif (uc $calculation eq "TOTAL"
        || uc $calculation eq "SUM"
        || uc $calculation eq "MEDIAN"
        || uc $calculation eq "AVG"
        || uc $calculation eq "AVERAGE"
        || uc $calculation eq "MEAN"
        || uc $calculation eq "MIN"
        || uc $calculation eq "MINIMUM"
        || uc $calculation eq "MAX"
        || uc $calculation eq "MAXIMUM" )
    {
        my $fullSourceMeasurementName = $parameterValues{measurement};
        my ( $sourceSensorNode, $sourceTableName, $sourceMeasurementName ) =
          split( /\./, $fullSourceMeasurementName );

        my $sourceMeasurementIndex = getMeasurementIndex($fullSourceMeasurementName);
        if ( !defined $sourceMeasurementIndex ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$fullSourceMeasurementName\"" );
            next;
        }

        my $sourceQlMeasurementIndex = $allMeasurements[$sourceMeasurementIndex]{qlMeasurementIndex};
        my $sourceQcMeasurementIndex = $allMeasurements[$sourceMeasurementIndex]{qcMeasurementIndex};

        my $minRecentMeasurements  = $parameterValues{intervals};
        my $maxMissingMeasurements = int( $minRecentMeasurements / 4 );

        my $sourceSampleInterval = $allTables{$sourceSensorNode}{$sourceTableName}{sampleInterval};
        my $sourceSamplingTimeIndex;
        my $sourceSamplingTime;
        my $sourceSamplingTimes;
        if ( $sourceSampleInterval == 1440 ) {
            $sourceSamplingTimeIndex = \%dailySamplingTimeIndex;
            $sourceSamplingTime      = \@dailySamplingTime;
            $sourceSamplingTimes     = $dailySamplingTimes;
        }
        elsif ( $sourceSampleInterval == 60 ) {
            $sourceSamplingTimeIndex = \%hourlySamplingTimeIndex;
            $sourceSamplingTime      = \@hourlySamplingTime;
            $sourceSamplingTimes     = $hourlySamplingTimes;
        }
        elsif ( $sourceSampleInterval == 5 ) {
            $sourceSamplingTimeIndex = \%fiveMinSamplingTimeIndex;
            $sourceSamplingTime      = \@fiveMinSamplingTime;
            $sourceSamplingTimes     = $fiveMinSamplingTimes;
        }
        elsif ( $sourceSampleInterval == 1 ) {
            $sourceSamplingTimeIndex = \%oneMinSamplingTimeIndex;
            $sourceSamplingTime      = \@oneMinSamplingTime;
            $sourceSamplingTimes     = $oneMinSamplingTimes;
        }

        # Don't allow measurement aggregation to extend the size of a data table
        if ( exists $allTables{$sensorNode}{$tableName}{lastTimestamp}
            && $allTables{$sensorNode}{$tableName}{lastTimestamp} lt $lastDestTimestamp )
        {
            $log->info( "[$fullMeasurementName] Not extending time series to $lastDestTimestamp"
                  . ", but keeping at "
                  . $allTables{$sensorNode}{$tableName}{lastTimestamp} );

            $lastDestTimestamp = $allTables{$sensorNode}{$tableName}{lastTimestamp};
            $lastDestTimeIndex = $destSamplingTimeIndex->{$lastDestTimestamp};
        }

        my $useRunningTotal;
        my $currentFunction;
        if ( uc $calculation eq "TOTAL" || uc $calculation eq "SUM" ) {
            $allMeasurements[$destMeasurementIndex]{displayName} .= " (95% CI)"
              if index( $measurementName, "DischargeVolume_M" ) == 0
              && exists $allMeasurements[$destMeasurementIndex]{displayName}
              && index( $allMeasurements[$destMeasurementIndex]{displayName}, "(95% CI)" ) < 0;

            $currentFunction = \&sum;
            $useRunningTotal = 1 if index( lc($sourceMeasurementName), "rain" ) >= 0;

            # Sum up as much as we can, but flag if 1 or more missing
            $maxMissingMeasurements = $minRecentMeasurements - 1;
        }
        elsif ( uc $calculation eq "MEDIAN" ) {
            $currentFunction = \&median;
        }
        elsif (uc $calculation eq "AVG"
            || uc $calculation eq "AVERAGE"
            || uc $calculation eq "MEAN" )
        {
            $allMeasurements[$destMeasurementIndex]{displayName} .= " (95% CI)"
              if index( $measurementName, "DischargeRate_M" ) == 0
              && exists $allMeasurements[$destMeasurementIndex]{displayName}
              && index( $allMeasurements[$destMeasurementIndex]{displayName}, "(95% CI)" ) < 0;

            $currentFunction = \&mean;
        }
        elsif ( uc $calculation eq "MIN" || uc $calculation eq "MINIMUM" ) {
            $currentFunction = \&min;
        }
        elsif ( uc $calculation eq "MAX" || uc $calculation eq "MAXIMUM" ) {
            $currentFunction = \&max;
        }

        my $currentIndex = 0;
        my @measurementValues;
        my $computeQualityFields;
        $computeQualityFields = 1 if defined $qlMeasurementIndex && defined $sourceQlMeasurementIndex;
        my @qualityLevels;
        my @qualityFlags;
        my $numRecentMeasurements = 0;
        my $runningTotal          = 0;
        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            if ( $sourceSampleInterval < $destSampleInterval ) {
                my $lastSourceTimeIndex = $sourceSamplingTimeIndex->{ $destSamplingTime->[$destTimeIndex] };
                my $firstSourceTimeIndex = $lastSourceTimeIndex - ( $destSampleInterval / $sourceSampleInterval ) + 1;

                next if $firstSourceTimeIndex < 0;    # Skip to the next destination time index

                foreach my $sourceTimeIndex ( $firstSourceTimeIndex .. $lastSourceTimeIndex ) {

                    my $currentMeasurementValue = $allData[$sourceMeasurementIndex][$sourceTimeIndex];

                    if ( defined $currentMeasurementValue ) {
                        $numRecentMeasurements++;
                        $runningTotal += $currentMeasurementValue;
                    }
                    else {
                        $numRecentMeasurements = 0;
                        next unless @measurementValues;  # Skip ahead until we hit the first non-null source measurement
                    }

                    if ($computeQualityFields) {
                        if ( defined $qcData[$sourceQlMeasurementIndex]{$sourceTimeIndex} ) {
                            $qualityLevels[$currentIndex] = $qcData[$sourceQlMeasurementIndex]{$sourceTimeIndex};
                        }
                        else {
                            $qualityLevels[$currentIndex] = undef;
                        }

                        if ( defined $qcData[$sourceQcMeasurementIndex]{$sourceTimeIndex} ) {
                            $qualityFlags[$currentIndex] = $qcData[$sourceQcMeasurementIndex]{$sourceTimeIndex};
                        }
                        else {
                            $qualityFlags[$currentIndex] = undef;
                        }
                    }

                    $runningTotal -= $measurementValues[$currentIndex] if defined $measurementValues[$currentIndex];
                    $runningTotal = 0 if $runningTotal < 0.05;    # Handle rounding/precision errors

                    $measurementValues[ $currentIndex++ ] = $currentMeasurementValue;
                    $currentIndex = 0 if $currentIndex >= $minRecentMeasurements;
                }
            }
            else {
                my $currentMeasurementValue = $allData[$sourceMeasurementIndex][$destTimeIndex];

                if ( defined $currentMeasurementValue ) {
                    $numRecentMeasurements++;
                    $runningTotal += $currentMeasurementValue;
                }
                else {
                    $numRecentMeasurements = 0;
                    next unless @measurementValues;    # Skip ahead until we hit the first non-null source measurement
                }

                if ($computeQualityFields) {
                    if ( defined $qcData[$sourceQlMeasurementIndex]{$destTimeIndex} ) {
                        $qualityLevels[$currentIndex] = $qcData[$sourceQlMeasurementIndex]{$destTimeIndex};
                    }
                    else {
                        $qualityLevels[$currentIndex] = undef;
                    }

                    if ( defined $qcData[$sourceQcMeasurementIndex]{$destTimeIndex} ) {
                        $qualityFlags[$currentIndex] = $qcData[$sourceQcMeasurementIndex]{$destTimeIndex};
                    }
                    else {
                        $qualityFlags[$currentIndex] = undef;
                    }
                }

                $runningTotal -= $measurementValues[$currentIndex] if defined $measurementValues[$currentIndex];
                $runningTotal = 0 if $runningTotal < 0.05;    # Handle rounding/precision errors

                $measurementValues[ $currentIndex++ ] = $currentMeasurementValue;
                $currentIndex = 0 if $currentIndex >= $minRecentMeasurements;
            }

            next if scalar @measurementValues < $minRecentMeasurements;

            # Don't calculate measurement values which have been QC flagged
            next
              if defined $allData[$destMeasurementIndex][$destTimeIndex]
              && defined $qlMeasurementIndex
              && defined $qcData[$qlMeasurementIndex]{$destTimeIndex};

            my $numMissingMeasurements = 0;
            if ( $numRecentMeasurements < $minRecentMeasurements ) {
                foreach my $currentValue (@measurementValues) {
                    $numMissingMeasurements++ unless defined $currentValue;
                }
            }

            if ( $numMissingMeasurements <= $maxMissingMeasurements ) {
                my $newValue;
                if ($useRunningTotal) {
                    $newValue = $runningTotal;
                }
                else {
                    $newValue = $currentFunction->(@measurementValues);
                }
                if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                    $measurementsUpdated++
                      if $allData[$destMeasurementIndex][$destTimeIndex] != $newValue;
                }
                else {
                    $measurementsAdded++;
                }
                $allData[$destMeasurementIndex][$destTimeIndex] = $newValue;
            }
            elsif ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $allData[$destMeasurementIndex][$destTimeIndex] = undef;
                $measurementsRemoved++;
            }

            if ($computeQualityFields) {

                my $minQualityLevel;
                foreach my $qualityLevel (@qualityLevels) {
                    if ( !defined $qualityLevel ) {
                        undef $minQualityLevel;
                        last;
                    }

                    next if defined $minQualityLevel && $minQualityLevel <= $qualityLevel;

                    $minQualityLevel = $qualityLevel;
                }

                $qcData[$qlMeasurementIndex]{$destTimeIndex} = $minQualityLevel if defined $minQualityLevel;

                my $commonQcFlag;
                my %qcFlags;
                foreach my $qualityFlag (@qualityFlags) {
                    if ( !defined $qualityFlag ) {
                        $commonQcFlag = "none";
                        next;
                    }

                    # Handle common case where all QC flags are identical
                    $commonQcFlag = $qualityFlag unless defined $commonQcFlag;
                    $commonQcFlag = "none" if $commonQcFlag ne $qualityFlag;

                    foreach my $qcFlag ( split /:/, $qualityFlag ) {
                        next unless ( length($qcFlag) == 2 || length($qcFlag) == 3 ) && $qcFlag eq uc $qcFlag;

                        $qcFlags{$qcFlag}++;
                    }
                }

                $qcFlags{MV} = $numMissingMeasurements if $numMissingMeasurements;

                if (%qcFlags) {

                    my $aggregateFlag;
                    if ( defined $commonQcFlag && $commonQcFlag ne "none" ) {
                        $aggregateFlag = $commonQcFlag;
                    }
                    else {
                        my @flagCounts;
                        foreach my $qcFlag ( sort keys %qcFlags ) {
                            next if $qcFlags{$qcFlag} == $minRecentMeasurements;

                            push @flagCounts, $qcFlags{$qcFlag} . "/$minRecentMeasurements $qcFlag";
                        }

                        $aggregateFlag = join( ":", sort keys %qcFlags );
                        $aggregateFlag .= ":" . join( ":", @flagCounts ) if @flagCounts;
                    }

                    $qcData[$qcMeasurementIndex]{$destTimeIndex} = $aggregateFlag;
                }
            }
        }

        $calculationString =
          "$calculation(" . getMeasurementCalculation($sourceMeasurementIndex) . ",$minRecentMeasurements)";
    }
    elsif ( uc $calculation eq "FILLGAPS" ) {

        # Support reading from a different column
        my $fullSourceMeasurementName = $parameterValues{measurement};
        $fullSourceMeasurementName = $fullMeasurementName
          unless defined $fullSourceMeasurementName;
        my $sourceMeasurementIndex = getMeasurementIndex($fullSourceMeasurementName);
        if ( !defined $sourceMeasurementIndex ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$fullSourceMeasurementName\"" );
            next;
        }

        my $isNewMeasurement;
        $isNewMeasurement = 1 if $fullSourceMeasurementName ne $fullMeasurementName;

        my $maxGapsFilled = $parameterValues{maxGapsFilled};
        $maxGapsFilled = 1 unless defined $maxGapsFilled;

        my $measurementPrecision = $parameterValues{precision};
        my $precisionMultiplier  = 10**$measurementPrecision
          if defined $measurementPrecision;

        my $clipRangeBefore = $parameterValues{clipRangeBefore};
        my $clipRangeAfter  = $parameterValues{clipRangeAfter};

        my $lastMeasurementValue;
        my $numGaps               = 0;
        my $numRecentMeasurements = 0;
        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            my $currentMeasurementValue = $allData[$sourceMeasurementIndex][$destTimeIndex];

            if ( defined $currentMeasurementValue ) {

                $allData[$destMeasurementIndex][$destTimeIndex] = $currentMeasurementValue
                  if defined $isNewMeasurement;

                if (   $numGaps > 0
                    && $numGaps <= $maxGapsFilled
                    && defined $lastMeasurementValue )
                {

                    my $startTimeIndex = $lastMeasurementIndex + 1;
                    my $endTimeIndex   = $destTimeIndex - 1;

                    my $preGapValue  = $lastMeasurementValue;
                    my $postGapValue = $currentMeasurementValue;

                    if ( defined $clipRangeBefore ) {
                        my $countDown = $clipRangeBefore;
                        while ( $countDown-- > 0 ) {
                            last
                              unless defined $allData[$sourceMeasurementIndex][ $startTimeIndex - 2 ];

                            $preGapValue = $allData[$sourceMeasurementIndex][ $startTimeIndex - 2 ];

                            $startTimeIndex--;
                        }
                    }
                    if ( defined $clipRangeAfter ) {
                        my $countDown = $clipRangeAfter;
                        while ( $countDown-- > 0 ) {
                            last unless defined $allData[$sourceMeasurementIndex][ $endTimeIndex + 2 ];

                            $postGapValue = $allData[$sourceMeasurementIndex][ $endTimeIndex + 2 ];

                            $endTimeIndex++;

                            # Push the stick ahead
                            $destTimeIndex++;
                            $lastMeasurementValue = $postGapValue;
                        }
                    }

                    my $currentMeasurementGap =
                      ( $postGapValue - $preGapValue ) / ( $endTimeIndex - $startTimeIndex + 2 );
                    my $currentGapNumber = 0;
                    for my $timeIndex ( $startTimeIndex .. $endTimeIndex ) {

                        $currentGapNumber++;
                        my $interpolatedMeasurement = $preGapValue + ( $currentMeasurementGap * $currentGapNumber );

                        $interpolatedMeasurement =
                          int( $interpolatedMeasurement * $precisionMultiplier + 0.5 ) / $precisionMultiplier
                          if defined $precisionMultiplier;

                        my $previousMeasurement = $allData[$destMeasurementIndex][$timeIndex];

                        $allData[$destMeasurementIndex][$timeIndex] = $interpolatedMeasurement;

                        if ( defined $qcMeasurementIndex ) {
                            my $previousFlag = $qcData[$qcMeasurementIndex]{$timeIndex};
                            if ( !defined $previousFlag ) {
                                $qcData[$qcMeasurementIndex]{$timeIndex} = "EV:Auto:$measurementName";
                                $qcData[$qcMeasurementIndex]{$timeIndex} .= " was $previousMeasurement"
                                  if defined $previousMeasurement;
                            }
                            elsif ( index( $previousFlag, "EV:Auto:" ) < 0 ) {
                                $qcData[$qcMeasurementIndex]{$timeIndex} = "EV:Auto:$previousFlag";
                            }
                        }

                        $measurementsAdded++;
                    }
                }
                $lastMeasurementIndex = $destTimeIndex;
                $lastMeasurementValue = $currentMeasurementValue;
                $numGaps              = 0;
            }
            else {
                $numGaps++;
            }
        }

        $calculationString = "fillGaps(" . getMeasurementCalculation($sourceMeasurementIndex);
        $calculationString .= ",maxGapsFilled=$maxGapsFilled)";
    }
    elsif ( uc $calculation eq "CLIP" ) {

        my $parentMeasurementIndex = $allMeasurements[$destMeasurementIndex]{parentMeasurementIndex};
        my @measurementIndexes;
        foreach my $childMeasurementIndex ( @{ $allMeasurements[$parentMeasurementIndex]{childMeasurementIndexes} } ) {
            push @measurementIndexes, $childMeasurementIndex
              if defined $allMeasurements[$childMeasurementIndex]{isReferenced};
        }

        if ( scalar(@measurementIndexes) == 0 ) {
            $log->debug("WARNING: Ignoring calculation of $calculation($fullMeasurementName)");
            next;
        }

        my $qcFlag = $parameterValues{qcFlag};

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            my @clippedMeasurements;
            foreach my $measurementIndex (@measurementIndexes) {
                next
                  unless exists $allData[$measurementIndex][$destTimeIndex]
                  && defined $allData[$measurementIndex][$destTimeIndex];

                push @clippedMeasurements, $allMeasurements[$measurementIndex]{measurementName} . "="
                  . $allData[$measurementIndex][$destTimeIndex];

                $allData[$measurementIndex][$destTimeIndex] = undef;
            }

            next unless @clippedMeasurements;

            $measurementsRemoved++;

            next unless defined $qcFlag && defined $qcMeasurementIndex;

            $qcData[$qcMeasurementIndex]{$destTimeIndex} = "$qcFlag:" . join( ",", @clippedMeasurements );
        }
    }
    elsif ( uc $calculation eq "RANGECLIP" ) {

        my $minimumValue = $parameterValues{minimumValue};
        my $maximumValue = $parameterValues{maximumValue};

        my $clipRangeBefore = $parameterValues{clipRangeBefore};
        my $clipRangeAfter  = $parameterValues{clipRangeAfter};

        if ( exists $parameterValues{clipRange} ) {
            $clipRangeBefore = $parameterValues{clipRange};
            $clipRangeAfter  = $parameterValues{clipRange};
        }

        $clipRangeBefore = 0 unless defined $clipRangeBefore;
        $clipRangeAfter  = 0 unless defined $clipRangeAfter;

        my $parentMeasurementIndex = $allMeasurements[$destMeasurementIndex]{parentMeasurementIndex};
        my @measurementIndexes;
        foreach my $childMeasurementIndex ( @{ $allMeasurements[$parentMeasurementIndex]{childMeasurementIndexes} } ) {
            next unless defined $allMeasurements[$childMeasurementIndex]{isReferenced};

            next
              if index( uc $measurementName,                                          "_STD" ) > 0
              && index( uc $allMeasurements[$childMeasurementIndex]{measurementName}, "_STD" ) < 0;

            next
              if index( uc $measurementName,                                          "_STD" ) < 0
              && index( uc $allMeasurements[$childMeasurementIndex]{measurementName}, "_STD" ) > 0;

            # Only clip fDOM, pCO2, Turbidity and SR50 measurements based on average or standard deviation
            next
              if ( index( uc $measurementName, "FDOM" ) == 0
                || index( uc $measurementName, "PCO2" ) == 0
                || index( uc $measurementName, "TURBIDITY" ) == 0
                || index( uc $measurementName, "SR50A_DISTANCE" ) == 0 )
              && index( uc $allMeasurements[$childMeasurementIndex]{measurementName}, "_AVG" ) < 0
              && index( uc $allMeasurements[$childMeasurementIndex]{measurementName}, "_STD" ) < 0;

            push @measurementIndexes, $childMeasurementIndex;
        }

        if ( scalar(@measurementIndexes) == 0 ) {
            $log->debug("WARNING: Ignoring calculation of $calculation($fullMeasurementName)");
            next;
        }

        my $qcMeasurementName = $allMeasurements[$parentMeasurementIndex]{measurementName};
        $qcMeasurementName = $measurementName if index( $measurementName, "_Std" ) > 0;

        my @linkedMeasurementIndexes;
        if ( length $parameterValues{linkedMeasurements} ) {
            foreach my $linkedMeasurementName ( split /&/, $parameterValues{linkedMeasurements} ) {
                my $linkedMeasurementIndex = getMeasurementIndex( $sensorNode, $tableName, $linkedMeasurementName );

                if ( !defined $linkedMeasurementIndex ) {
                    $log->warn(
"WARNING: unrecognized linked measurement $sensorNode.$tableName.$linkedMeasurementName, skipping"
                    );
                    next;
                }

                push @linkedMeasurementIndexes, $linkedMeasurementIndex;
            }
        }

        my $lastClipIndex;
        my $lastQCFlag;
        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            my $minMeasurementValue;
            my $maxMeasurementValue;
            foreach my $measurementIndex (@measurementIndexes) {
                my $measurementValue = $allData[$measurementIndex][$destTimeIndex];
                next unless defined $measurementValue;

                $minMeasurementValue = $measurementValue
                  if !defined $minMeasurementValue || $minMeasurementValue > $measurementValue;
                $maxMeasurementValue = $measurementValue
                  if !defined $maxMeasurementValue || $maxMeasurementValue < $measurementValue;
            }

            my $qcFlag;
            if ( defined $minimumValue && defined $minMeasurementValue && $minMeasurementValue < $minimumValue ) {
                $qcFlag = "BR:Auto:$qcMeasurementName<$minimumValue";
            }
            elsif ( defined $maximumValue && defined $maxMeasurementValue && $maxMeasurementValue > $maximumValue ) {
                $qcFlag = "AR:Auto:$qcMeasurementName>$maximumValue";
            }

            # NOTE: this code was fixed on October 6, 2016 to not clip 'future' measurement values until
            # they have been assessed to see if they also fall out of range.  Prior to fixing this, the
            # clip range was sometimes not extending long enough into the future
            my $firstClipIndex;
            if ( defined $qcFlag ) {
                $firstClipIndex = $destTimeIndex - $clipRangeBefore;

                $lastQCFlag = $qcFlag;
                $lastClipIndex = $destTimeIndex + $clipRangeAfter if $clipRangeAfter > 0;
            }
            elsif ( defined $lastClipIndex ) {
                if ( $destTimeIndex <= $lastClipIndex ) {
                    $firstClipIndex = $destTimeIndex;
                    $qcFlag         = $lastQCFlag;
                }
                else {
                    undef $lastClipIndex;
                }
            }

            if ( defined $firstClipIndex ) {
                for my $timeIndex ( $firstClipIndex .. $destTimeIndex ) {
                    my $numFound = 0;
                    foreach my $measurementIndex (@measurementIndexes) {
                        if ( defined $allData[$measurementIndex][$timeIndex] ) {
                            $numFound++;
                            last;
                        }
                    }
                    next unless $numFound > 0;

                    $measurementsRemoved++;

                    foreach my $measurementIndex (@measurementIndexes) {
                        next unless defined $allData[$measurementIndex][$timeIndex];

                        clipAndFlagMeasurement( $measurementIndex, $timeIndex, $qcFlag );
                    }

                    next unless @linkedMeasurementIndexes;
                    foreach my $linkedMeasurementIndex (@linkedMeasurementIndexes) {
                        clipAndFlagMeasurement( $linkedMeasurementIndex, $timeIndex, $qcFlag );
                    }
                }
            }
        }

        $calculationString = "rangeClip(";
        $calculationString .= "minimumValue=$minimumValue" if defined $minimumValue;
        $calculationString .= ","                          if defined $minimumValue && defined $maximumValue;
        $calculationString .= "maximumValue=$maximumValue" if defined $maximumValue;
        $calculationString .= ")";
    }
    elsif ( uc $calculation eq "CLIPWL" ) {

        my $minimumValue    = $parameterValues{minimumValue};
        my $clipRangeBefore = $parameterValues{clipRangeBefore};
        my $clipRangeAfter  = $parameterValues{clipRangeAfter};
        my $maxClipRange    = $parameterValues{maxClipRange};

        if ( exists $parameterValues{clipRange} ) {
            $clipRangeBefore = $parameterValues{clipRange};
            $clipRangeAfter  = $parameterValues{clipRange};
        }

        $minimumValue    = 0.01 unless defined $minimumValue;
        $clipRangeBefore = 1    unless defined $clipRangeBefore;
        $clipRangeAfter  = 1    unless defined $clipRangeAfter;
        $maxClipRange    = 144  unless defined $maxClipRange;

        my $parentMeasurementIndex = $allMeasurements[$destMeasurementIndex]{parentMeasurementIndex};
        my $avgMeasurementIndex;
        my $stdMeasurementIndex;
        foreach my $childMeasurementIndex ( @{ $allMeasurements[$parentMeasurementIndex]{childMeasurementIndexes} } ) {
            next unless defined $allMeasurements[$childMeasurementIndex]{isReferenced};

            $avgMeasurementIndex = $childMeasurementIndex
              if index( uc $allMeasurements[$childMeasurementIndex]{measurementName}, "_AVG" ) > 0;
            $stdMeasurementIndex = $childMeasurementIndex
              if index( uc $allMeasurements[$childMeasurementIndex]{measurementName}, "_STD" ) > 0;
        }

        if ( !defined $avgMeasurementIndex || !defined $stdMeasurementIndex ) {
            $log->warn("WARNING: Ignoring calculation of clipWL($fullMeasurementName)");
            next;
        }

        my @linkedMeasurementIndexes;
        if ( length $parameterValues{linkedMeasurements} ) {
            foreach my $linkedMeasurementName ( split /&/, $parameterValues{linkedMeasurements} ) {
                my $linkedMeasurementIndex = getMeasurementIndex( $sensorNode, $tableName, $linkedMeasurementName );

                if ( !defined $linkedMeasurementIndex ) {
                    $log->warn(
"WARNING: unrecognized linked measurement $sensorNode.$tableName.$linkedMeasurementName, skipping"
                    );
                    next;
                }

                push @linkedMeasurementIndexes, $linkedMeasurementIndex;
            }
        }

        my $lastValue;
        my $clippingStartIndex;
        my $clippingEndIndex;
        my $minIntervalValue;
        my $recentLevelChange;
        my $levelBeforeDrop;
        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            my $avgLevel = $allData[$avgMeasurementIndex][$destTimeIndex];
            my $std      = $allData[$stdMeasurementIndex][$destTimeIndex];

            next unless defined $avgLevel && defined $std;

            if ( defined $clippingStartIndex ) {
                my $targetLevel = $levelBeforeDrop;
                $targetLevel += $recentLevelChange * ( $destTimeIndex - $clippingStartIndex + 1 )
                  if defined $recentLevelChange && $recentLevelChange < 0;

                my $targetThreshold = 0.04 * ( $destTimeIndex - $clippingStartIndex ) / $maxClipRange;

                if ( $avgLevel > $minimumValue && ( $targetLevel - $avgLevel ) < $targetThreshold ) {
                    $clippingEndIndex = $destTimeIndex - 1;
                }
                else {
                    $minIntervalValue = $avgLevel if $avgLevel < $minIntervalValue;
                }

                if ( defined $clippingEndIndex ) {

                    my $cmDrop = int( 1000.0 * ( $levelBeforeDrop - $minIntervalValue ) + 0.5 ) / 10.0;

                    $log->info( "[$logMeasurementName] clipping $cmDrop cm drop around "
                          . $destSamplingTime->[$clippingStartIndex]
                          . ", returning to within "
                          . int( 10000.0 * abs( $avgLevel - $levelBeforeDrop ) + 0.5 ) / 100.0
                          . " cm of the original level in "
                          . ( $clippingEndIndex - $clippingStartIndex + 1 ) * 5
                          . " minutes, targetLevel=$targetLevel, targetThreshold=$targetThreshold" );

                    my $qcFlag = "SVD:Auto:Clipped temporary water level drop of $cmDrop cm";

                    $clippingStartIndex -= $clipRangeBefore;
                    $clippingEndIndex += $clipRangeAfter;

                    for my $timeIndex ( $clippingStartIndex .. $clippingEndIndex ) {
                        next unless defined $allData[$avgMeasurementIndex][$timeIndex];

                        $measurementsRemoved++;

                        clipAndFlagMeasurement( $avgMeasurementIndex, $timeIndex, $qcFlag );

                        next unless @linkedMeasurementIndexes;
                        foreach my $linkedMeasurementIndex (@linkedMeasurementIndexes) {
                            clipAndFlagMeasurement( $linkedMeasurementIndex, $timeIndex, $qcFlag );
                        }
                    }

                    undef $clippingStartIndex;
                    undef $clippingEndIndex;
                    undef $recentLevelChange;
                }
                elsif ( $avgLevel > $minimumValue && ( $destTimeIndex - $clippingStartIndex ) > $maxClipRange ) {

                    my $cmDrop = int( 1000.0 * ( $levelBeforeDrop - $minIntervalValue ) + 0.5 ) / 10.0;

                    $log->warn( "[$logMeasurementName] WARNING: ignoring $cmDrop cm drop around "
                          . $destSamplingTime->[$clippingStartIndex]
                          . " that did not within "
                          . ( $maxClipRange * 5 )
                          . " minutes, levelBeforeDrop=$levelBeforeDrop, avgLevel=$avgLevel"
                          . ", targetLevel=$targetLevel, targetThreshold=$targetThreshold" );

                    undef $clippingStartIndex;
                    undef $recentLevelChange;
                }
            }
            elsif ( defined $lastValue ) {

                my $levelChange = $avgLevel - $lastValue;

                # If the water level has dropped quickly or dropped below 1 cm
                if (
                    ( $levelChange < -0.01 && $std > 0.02 )    # Dropped more then 1 cm with variability
                    || ( $levelChange < -0.10 )                # Dropped more then 10 cm
                    || ( $avgLevel <= $minimumValue )          # Dropped below the minimum theshold
                  )
                {
                    $clippingStartIndex = $destTimeIndex;
                    $levelBeforeDrop    = $lastValue;
                    $minIntervalValue   = $avgLevel;
                    $minIntervalValue   = $levelBeforeDrop if $levelBeforeDrop < $minIntervalValue;

                    if ( defined $allData[$avgMeasurementIndex][ $clippingStartIndex - 2 ] ) {
                        $levelBeforeDrop = $allData[$avgMeasurementIndex][ $clippingStartIndex - 2 ];
                        $minIntervalValue = $levelBeforeDrop if $levelBeforeDrop < $minIntervalValue;
                    }

                    # Evaluate the recent change in the water level measurement
                    my @recentLevelChanges;
                    my $timeIndex = $destTimeIndex - 1;
                    while ( $timeIndex > 0 && scalar(@recentLevelChanges) < 6 ) {
                        push @recentLevelChanges,
                          $allData[$avgMeasurementIndex][$timeIndex] - $allData[$avgMeasurementIndex][ $timeIndex - 1 ]
                          if defined $allData[$avgMeasurementIndex][$timeIndex]
                          && defined $allData[$avgMeasurementIndex][ $timeIndex - 1 ];
                        $timeIndex--;
                    }

                    if ( scalar(@recentLevelChanges) < 6 ) {
                        $log->warn("[$logMeasurementName] WARNING: failed to calculate recent water level change");
                    }
                    else {
                        $recentLevelChange = mean(@recentLevelChanges);
                    }

                }
            }

            $lastValue = $avgLevel;
        }

        $calculationString = "clipWL(";
        $calculationString .= "minimumValue=$minimumValue" if defined $minimumValue;
        $calculationString .=
          ",clipRangeBefore=$clipRangeBefore,clipRangeAfter=$clipRangeAfter,maxClipRange=$maxClipRange";
        $calculationString .= ")";
    }
    elsif ( uc $calculation eq "CLIPPV" ) {

        my $maxRepeats = $parameterValues{maxRepeats};
        $maxRepeats = 10 unless defined $maxRepeats;

        my $clippedMeasurementName = getBaseName($measurementName);

        my @measurementIndexes;
        my $stdMeasurementIndex;
        for my $measurementIndex ( 0 .. $#allMeasurements ) {
            next
              unless $allMeasurements[$measurementIndex]{sensorNode} eq $sensorNode
              && $allMeasurements[$measurementIndex]{dataTable} eq $tableName
              && defined $allMeasurements[$measurementIndex]{isReferenced}
              && index( lc $allMeasurements[$measurementIndex]{measurementName}, lc $clippedMeasurementName ) == 0;

            my $currentMeasurementName = $allMeasurements[$measurementIndex]{measurementName};

            push @measurementIndexes, $measurementIndex
              if lc $clippedMeasurementName eq lc $currentMeasurementName
              || lc "${clippedMeasurementName}_Med" eq lc $currentMeasurementName
              || lc "${clippedMeasurementName}_Avg" eq lc $currentMeasurementName
              || lc "${clippedMeasurementName}_Min" eq lc $currentMeasurementName
              || lc "${clippedMeasurementName}_Max" eq lc $currentMeasurementName;
            $stdMeasurementIndex = $measurementIndex
              if lc "${clippedMeasurementName}_Std" eq lc $currentMeasurementName;
        }

        if ( scalar(@measurementIndexes) == 0 ) {
            $log->warn("WARNING: Ignoring calculation of clipPV($fullMeasurementName)");
            next;
        }
        if ( !defined $stdMeasurementIndex ) {
            $log->warn("WARNING: Measurement $fullMeasurementName does not have associated Std");
        }

        my $numRepeats = 0;
        my $persistentValue;
        my $persistentTimeIndex;
        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {
            my $currentPersistentValue;
            my $foundMultipleValues;
            foreach my $measurementIndex (@measurementIndexes) {
                my $measurementValue = $allData[$measurementIndex][$destTimeIndex];
                next unless defined $measurementValue;

                if ( !defined $currentPersistentValue ) {
                    $currentPersistentValue = $measurementValue;
                }
                elsif ( $measurementValue != $currentPersistentValue ) {
                    $foundMultipleValues = 1;
                    last;
                }
            }

            next unless defined $currentPersistentValue;    # No values found

            # All values must be identical and standard deviation must be 0
            if (
                defined $foundMultipleValues
                || (   defined $stdMeasurementIndex
                    && defined $allData[$stdMeasurementIndex][$destTimeIndex]
                    && $allData[$stdMeasurementIndex][$destTimeIndex] > 0 )
              )
            {
                $persistentValue     = undef;
                $persistentTimeIndex = undef;
                $numRepeats          = 0;
                next;
            }

            $persistentValue = $currentPersistentValue
              unless defined $persistentValue;
            $persistentTimeIndex = $destTimeIndex
              unless defined $persistentTimeIndex;

            if ( $persistentValue == $currentPersistentValue ) {
                $numRepeats++;
            }
            else {
                $persistentValue     = $currentPersistentValue;
                $persistentTimeIndex = $destTimeIndex;
                $numRepeats          = 1;
            }

            if ( $numRepeats >= $maxRepeats ) {
                my $qcFlag = "PV:Auto:$measurementName=$persistentValue";
                while ( $persistentTimeIndex <= $destTimeIndex ) {

                    # Don't calculate measurement values which have been QC flagged
                    next if defined $qcMeasurementIndex && defined $qcData[$qcMeasurementIndex]{$persistentTimeIndex};

                    my $numRemoved = 0;
                    foreach my $measurementIndex (@measurementIndexes) {
                        if ( defined $allData[$measurementIndex][$persistentTimeIndex] ) {
                            $allData[$measurementIndex][$persistentTimeIndex] = undef;
                            $numRemoved++;
                        }
                    }

                    if ($numRemoved) {
                        $measurementsRemoved++;

                        $qcData[$qcMeasurementIndex]{$persistentTimeIndex} = $qcFlag
                          if defined $qcMeasurementIndex
                          && !defined $qcData[$qcMeasurementIndex]{$persistentTimeIndex};
                    }

                    $persistentTimeIndex++;
                }
            }

            $log->debug( "[$logMeasurementName] Value $persistentValue repeated $numRepeats times"
                  . ", ending at time "
                  . $destSamplingTime->[$destTimeIndex] );
        }
    }
    elsif ( uc $calculation eq "SNOWDEPTH" ) {

        if (   !defined $parameterValues{measurement}
            || !defined $parameterValues{sensorHeight} )
        {
            $log->warn("WARNING: Insufficient parameters specified in calculation \"$calculation\"");
            next;
        }

        my $fullSourceMeasurementName = $parameterValues{measurement};
        my $sourceMeasurementIndex    = getMeasurementIndex($fullSourceMeasurementName);
        if ( !defined $sourceMeasurementIndex ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$fullSourceMeasurementName\"" );
            next;
        }

        my $sensorHeight = $parameterValues{sensorHeight};

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            # Don't calculate measurement values which have been QC flagged
            next if defined $qlMeasurementIndex && defined $qcData[$qlMeasurementIndex]{$destTimeIndex};

            my $distanceValue = $allData[$sourceMeasurementIndex][$destTimeIndex];
            next
              unless defined $distanceValue
              && $distanceValue > 0.10
              && $distanceValue < $sensorHeight;

            my $snowDepth = $sensorHeight - $distanceValue;
            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $snowDepth;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $snowDepth;
        }

        $calculationString = "snowDepth(" . getMeasurementCalculation($sourceMeasurementIndex);
        $calculationString .= ",sensorHeight=$sensorHeight)";
    }
    elsif ( uc $calculation eq "FDOM20" ) {

        my $fDOMmeasurementName          = $parameterValues{measurement};
        my $fDOMwaterTempMeasurementName = $parameterValues{waterTemp};
        my $fDOMtempCoefficient          = $parameterValues{tempCoefficient};

        if (   !defined $fDOMmeasurementName
            || !defined $fDOMwaterTempMeasurementName
            || !defined $fDOMtempCoefficient )
        {
            $log->warn("WARNING: Insufficient parameters specified in calculation \"$calculation\"");
            next;
        }

        my $fDOMmeasurementIndex = getMeasurementIndex($fDOMmeasurementName);
        if ( !defined $fDOMmeasurementIndex ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$fDOMmeasurementName\"" );
            next;
        }

        my $waterTempMeasurementIndex = getMeasurementIndex($fDOMwaterTempMeasurementName);
        if ( !defined $waterTempMeasurementIndex ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$fDOMwaterTempMeasurementName\"" );
            next;
        }

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            # Don't calculate measurement values which have been QC flagged
            next if defined $qlMeasurementIndex && defined $qcData[$qlMeasurementIndex]{$destTimeIndex};

            my $fDOMmeasurementValue = $allData[$fDOMmeasurementIndex][$destTimeIndex];
            next unless defined $fDOMmeasurementValue;

            my $fDOMwaterTempValue = $allData[$waterTempMeasurementIndex][$destTimeIndex];
            next unless defined $fDOMwaterTempValue;

            my $fDOM20measurementValue =
              int( $fDOMmeasurementValue / ( 1.0 + $fDOMtempCoefficient * ( $fDOMwaterTempValue - 20.0 ) ) + 0.5 );

            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $fDOM20measurementValue;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $fDOM20measurementValue;
        }

        $calculationString = "fDOM20(" . getMeasurementCalculation($fDOMmeasurementIndex);
        $calculationString .= ",waterTemp=$fDOMwaterTempMeasurementName,tempCoefficient=$fDOMtempCoefficient)";
    }
    elsif ( uc $calculation eq "TURBIDITY" ) {

        my $turbidityMeasurementName = $parameterValues{measurement};
        my $turbidityIntercept       = $parameterValues{intercept};
        my $turbiditySlope           = $parameterValues{slope};

        if (   !defined $turbidityMeasurementName
            || !defined $turbidityIntercept
            || !defined $turbiditySlope )
        {
            $log->warn("WARNING: Insufficient parameters specified in calculation \"$calculation\"");
            next;
        }

        my $turbidityMeasurementIndex = getMeasurementIndex($turbidityMeasurementName);
        if ( !defined $turbidityMeasurementIndex ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$turbidityMeasurementName\"" );
            next;
        }

        my $srcQlMeasurementIndex = $allMeasurements[$turbidityMeasurementIndex]{qlMeasurementIndex};
        my $srcQcMeasurementIndex = $allMeasurements[$turbidityMeasurementIndex]{qcMeasurementIndex};

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            # Don't re-calculate measurement values which have been QC flagged
            next
              if defined $qlMeasurementIndex
              && defined $qcData[$qlMeasurementIndex]{$destTimeIndex}
              && defined $allData[$destMeasurementIndex][$destTimeIndex];

            # Inherit the quality level and quality flag
            if ( defined $srcQlMeasurementIndex && defined $qlMeasurementIndex ) {
                $qcData[$qlMeasurementIndex]{$destTimeIndex} = $qcData[$srcQlMeasurementIndex]{$destTimeIndex}
                  if exists $qcData[$srcQlMeasurementIndex]{$destTimeIndex}
                  && !exists $qcData[$qlMeasurementIndex]{$destTimeIndex};
                $qcData[$qcMeasurementIndex]{$destTimeIndex} = $qcData[$srcQcMeasurementIndex]{$destTimeIndex}
                  if exists $qcData[$srcQcMeasurementIndex]{$destTimeIndex}
                  && !exists $qcData[$qcMeasurementIndex]{$destTimeIndex};
            }

            my $turbidityMeasurementValue = $allData[$turbidityMeasurementIndex][$destTimeIndex];
            next unless defined $turbidityMeasurementValue;

            my $turbidityMeasurementValueNTU =
              int( ( 100.0 * ( $turbidityMeasurementValue - $turbidityIntercept ) / $turbiditySlope ) + 50.0 ) / 100;

            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $turbidityMeasurementValueNTU;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $turbidityMeasurementValueNTU;
        }

        $calculationString = "turbidity(" . getMeasurementCalculation($turbidityMeasurementIndex);
        $calculationString .= ",intercept=$turbidityIntercept,slope=$turbiditySlope)";
    }
    elsif ( uc $calculation eq "TRANSFORM" ) {

        my $sourceMeasurementName = $parameterValues{measurement};
        my $slope                 = $parameterValues{slope};
        my $intercept             = $parameterValues{intercept};

        if ( !defined $sourceMeasurementName || !defined $slope || !defined $intercept ) {
            $log->warn("WARNING: Insufficient parameters specified in calculation \"$calculation\"");
            next;
        }

        my $precision = $parameterValues{precision};
        my $precisionMultiplier = 10**$precision if defined $precision;

        my $sourceMeasurementIndex = getMeasurementIndex($sourceMeasurementName);
        if ( !defined $sourceMeasurementIndex ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$sourceMeasurementName\"" );
            next;
        }

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            # Don't calculate measurement values which have been QC flagged
            next if defined $qlMeasurementIndex && defined $qcData[$qlMeasurementIndex]{$destTimeIndex};

            my $sourceMeasurementValue = $allData[$sourceMeasurementIndex][$destTimeIndex];
            next unless defined $sourceMeasurementValue;

            my $transformedMeasurementValue = $slope * $sourceMeasurementValue + $intercept;

            $transformedMeasurementValue =
              int( $transformedMeasurementValue * $precisionMultiplier + 0.5 ) / $precisionMultiplier
              if defined $precisionMultiplier;

            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $transformedMeasurementValue;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $transformedMeasurementValue;
        }

        $calculationString = "transform(" . getMeasurementCalculation($sourceMeasurementIndex);
        $calculationString .= ",slope=$slope,intercept=$intercept)";
    }
    elsif ( uc $calculation eq "DISCHARGE" ) {

        my $stageMeasurementName = $parameterValues{stage};
        my $rcWorksheet          = $parameterValues{lookupSheet};

        if ( !defined $stageMeasurementName || !defined $rcWorksheet ) {
            $log->warn("WARNING: Insufficient parameters specified in calculation \"$calculation\"");
            next;
        }

        my $stageMeasurementIndex = getMeasurementIndex($stageMeasurementName);
        if ( !defined $stageMeasurementIndex ) {
            $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                  . " referencing unspecified measurement \"$stageMeasurementName\"" );
            next;
        }

        my $dischargeRateMeasurementName;
        my $minDischargeRateMeasurementName;
        my $maxDischargeRateMeasurementName;

        my $dischargeVolumeMeasurementIndex;
        my $dischargeVolumeMeasurementName;
        my $minDischargeVolumeMeasurementIndex;
        my $minDischargeVolumeMeasurementName;
        my $maxDischargeVolumeMeasurementIndex;
        my $maxDischargeVolumeMeasurementName;
        if ( $tableName eq $fiveMinuteTable && index( $measurementName, "Rate" ) > 0 ) {
            $dischargeRateMeasurementName    = $measurementName;
            $minDischargeRateMeasurementName = $measurementName . "_Min";
            $maxDischargeRateMeasurementName = $measurementName . "_Max";

            $dischargeVolumeMeasurementName = $measurementName;
            $dischargeVolumeMeasurementName =~ s/Rate/Volume/;

            $minDischargeVolumeMeasurementName = $dischargeVolumeMeasurementName . "_Min";
            $maxDischargeVolumeMeasurementName = $dischargeVolumeMeasurementName . "_Max";

            $dischargeVolumeMeasurementIndex =
              getMeasurementIndex( $sensorNode, $tableName, $dischargeVolumeMeasurementName );
            $minDischargeVolumeMeasurementIndex =
              getMeasurementIndex( $sensorNode, $tableName, $minDischargeVolumeMeasurementName );
            $maxDischargeVolumeMeasurementIndex =
              getMeasurementIndex( $sensorNode, $tableName, $maxDischargeVolumeMeasurementName );
        }

        my $dischargeMeasurementIndex    = $destMeasurementIndex;
        my $minDischargeMeasurementName  = $fullMeasurementName . "_Min";
        my $maxDischargeMeasurementName  = $fullMeasurementName . "_Max";
        my $minDischargeMeasurementIndex = getMeasurementIndex($minDischargeMeasurementName);
        my $maxDischargeMeasurementIndex = getMeasurementIndex($maxDischargeMeasurementName);
        my $stageQLMeasurementIndex      = $allMeasurements[$stageMeasurementIndex]{qlMeasurementIndex};
        my $stageQCMeasurementIndex      = $allMeasurements[$stageMeasurementIndex]{qcMeasurementIndex};
        if (   !defined $minDischargeMeasurementIndex
            || !defined $maxDischargeMeasurementIndex
            || !defined $stageQLMeasurementIndex
            || !defined $stageQCMeasurementIndex )
        {
            $log->warn(
"WARNING: failed to find min or max measurement index associated with calculation of \"$fullMeasurementName\", skipping"
            );
            next;
        }

        my @ratingCurve;
        loadRatingCurve( $rcWorksheet, \@ratingCurve );
        if ( !@ratingCurve ) {
            $log->warn("WARNING: Failed to load rating curve from worksheet \"$rcWorksheet\"");
            next;
        }

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            my $discharge;
            my $minDischarge;
            my $maxDischarge;

            # Don't calculate discharge values which have been QC flagged
            if (   defined $qlMeasurementIndex
                && defined $qcData[$qlMeasurementIndex]{$destTimeIndex}
                && defined $allData[$dischargeMeasurementIndex][$destTimeIndex] )
            {
                $discharge    = $allData[$dischargeMeasurementIndex][$destTimeIndex];
                $minDischarge = $allData[$minDischargeMeasurementIndex][$destTimeIndex];
                $maxDischarge = $allData[$maxDischargeMeasurementIndex][$destTimeIndex];
            }
            else {

                my $stage = $allData[$stageMeasurementIndex][$destTimeIndex];
                next unless defined $stage;

                my $mmStage = int( $stage * 1000.0 + 0.5 );    # Convert from meters to millimeters of stage

                if ( !exists $ratingCurve[$mmStage] ) {
                    $qcData[$qcMeasurementIndex]{$destTimeIndex} = "MV:no discharge found at $mmStage mm of stage";
                    next;
                }

                my $stageDateTime = $destSamplingTime->[$destTimeIndex];
                my $bestDateTime;
                foreach my $currentDateTime ( keys %{ $ratingCurve[$mmStage] } ) {
                    next if $stageDateTime lt $currentDateTime;    # RC must start before the stage time

                    # Use the rating curve from most recently before the current measurement time
                    next if defined $bestDateTime && $bestDateTime gt $currentDateTime;

                    $discharge    = $ratingCurve[$mmStage]{$currentDateTime}{discharge};
                    $minDischarge = $ratingCurve[$mmStage]{$currentDateTime}{minDischarge};
                    $maxDischarge = $ratingCurve[$mmStage]{$currentDateTime}{maxDischarge};

                    $bestDateTime = $currentDateTime;
                }

                if ( !defined $discharge || !defined $minDischarge || !defined $maxDischarge ) {
                    $qcData[$qcMeasurementIndex]{$destTimeIndex} = "MV:no discharge found at $mmStage mm of stage";
                    next;
                }

                $allData[$dischargeMeasurementIndex][$destTimeIndex]    = $discharge;
                $allData[$minDischargeMeasurementIndex][$destTimeIndex] = $minDischarge;
                $allData[$maxDischargeMeasurementIndex][$destTimeIndex] = $maxDischarge;

                $measurementsAdded++;

                # Inherit the quality level and quality flag
                inheiritQCFlag( $stageMeasurementIndex, $dischargeMeasurementIndex, $destTimeIndex );
            }

            if ( defined $dischargeVolumeMeasurementIndex
                && !defined $allData[$dischargeVolumeMeasurementIndex][$destTimeIndex] )
            {
                $allData[$dischargeVolumeMeasurementIndex][$destTimeIndex]    = $discharge * 300.0;
                $allData[$minDischargeVolumeMeasurementIndex][$destTimeIndex] = $minDischarge * 300.0;
                $allData[$maxDischargeVolumeMeasurementIndex][$destTimeIndex] = $maxDischarge * 300.0;

                # Inherit the quality level and quality flag
                inheiritQCFlag( $dischargeMeasurementIndex, $dischargeVolumeMeasurementIndex, $destTimeIndex );
            }
        }

        # Make sure the min and max measurement values are also recorded
        updateFirstTimestamp( $minDischargeMeasurementIndex, $destSamplingTime->[$firstDestTimeIndex] );
        updateLastTimestamp( $minDischargeMeasurementIndex, $destSamplingTime->[$lastDestTimeIndex] );
        updateFirstTimestamp( $maxDischargeMeasurementIndex, $destSamplingTime->[$firstDestTimeIndex] );
        updateLastTimestamp( $maxDischargeMeasurementIndex, $destSamplingTime->[$lastDestTimeIndex] );

        $allMeasurements[$minDischargeMeasurementIndex]{displayName} .= " (95% CI)"
          if exists $allMeasurements[$minDischargeMeasurementIndex]{displayName}
          && index( $allMeasurements[$minDischargeMeasurementIndex]{displayName}, "(95% CI)" ) < 0;
        $allMeasurements[$maxDischargeMeasurementIndex]{displayName} .= " (95% CI)"
          if exists $allMeasurements[$maxDischargeMeasurementIndex]{displayName}
          && index( $allMeasurements[$maxDischargeMeasurementIndex]{displayName}, "(95% CI)" ) < 0;

        $allMeasurements[$dischargeMeasurementIndex]{measurementCalculation} =
          "discharge(" . getMeasurementCalculation($stageMeasurementIndex) . ")";
        $allMeasurements[$minDischargeMeasurementIndex]{measurementCalculation} =
          "minDischarge(" . getMeasurementCalculation($stageMeasurementIndex) . ")";
        $allMeasurements[$maxDischargeMeasurementIndex]{measurementCalculation} =
          "maxDischarge(" . getMeasurementCalculation($stageMeasurementIndex) . ")";

        my $lastHourlyTimestamp = $allTables{$sensorNode}{$oneHourTable}{lastTimestamp};
        my $lastDailyTimestamp = substr( $lastHourlyTimestamp, 0, 10 ) . " 00:00:00";

        if ($dischargeRateMeasurementName) {
            my %hourlyDischargeCalc;
            $hourlyDischargeCalc{sensorNode}          = $sensorNode;
            $hourlyDischargeCalc{tableName}           = $oneHourTable;
            $hourlyDischargeCalc{measurementName}     = $dischargeRateMeasurementName;
            $hourlyDischargeCalc{measurementUnit}     = "m3/s";
            $hourlyDischargeCalc{measurementFunction} = "1 hour average";
            $hourlyDischargeCalc{calculation}         = "avg";
            $hourlyDischargeCalc{parameters} =
              "measurement=$fullMeasurementName,intervals=12,lastTimestamp=$lastHourlyTimestamp";

            push @pendingCalculations, \%hourlyDischargeCalc;

            my %dailyDischargeCalc;
            $dailyDischargeCalc{sensorNode}          = $sensorNode;
            $dailyDischargeCalc{tableName}           = $oneDayTable;
            $dailyDischargeCalc{measurementName}     = $dischargeRateMeasurementName;
            $dailyDischargeCalc{measurementUnit}     = "m3/s";
            $dailyDischargeCalc{measurementFunction} = "1 day average";
            $dailyDischargeCalc{calculation}         = "avg";
            $dailyDischargeCalc{parameters} =
              "measurement=$fullMeasurementName,intervals=288,lastTimestamp=$lastDailyTimestamp";

            push @pendingCalculations, \%dailyDischargeCalc;
        }

        if ($minDischargeRateMeasurementName) {
            my %hourlyDischargeCalc;
            $hourlyDischargeCalc{sensorNode}          = $sensorNode;
            $hourlyDischargeCalc{tableName}           = $oneHourTable;
            $hourlyDischargeCalc{measurementName}     = $minDischargeRateMeasurementName;
            $hourlyDischargeCalc{measurementUnit}     = "m3/s";
            $hourlyDischargeCalc{measurementFunction} = "1 hour average";
            $hourlyDischargeCalc{calculation}         = "avg";
            $hourlyDischargeCalc{parameters} =
              "measurement=$minDischargeMeasurementName,intervals=12,lastTimestamp=$lastHourlyTimestamp";

            push @pendingCalculations, \%hourlyDischargeCalc;

            my %dailyDischargeCalc;
            $dailyDischargeCalc{sensorNode}          = $sensorNode;
            $dailyDischargeCalc{tableName}           = $oneDayTable;
            $dailyDischargeCalc{measurementName}     = $minDischargeRateMeasurementName;
            $dailyDischargeCalc{measurementUnit}     = "m3/s";
            $dailyDischargeCalc{measurementFunction} = "1 day average";
            $dailyDischargeCalc{calculation}         = "avg";
            $dailyDischargeCalc{parameters} =
              "measurement=$minDischargeMeasurementName,intervals=288,lastTimestamp=$lastDailyTimestamp";

            push @pendingCalculations, \%dailyDischargeCalc;
        }

        if ($maxDischargeRateMeasurementName) {
            my %hourlyDischargeCalc;
            $hourlyDischargeCalc{sensorNode}          = $sensorNode;
            $hourlyDischargeCalc{tableName}           = $oneHourTable;
            $hourlyDischargeCalc{measurementName}     = $maxDischargeRateMeasurementName;
            $hourlyDischargeCalc{measurementUnit}     = "m3/s";
            $hourlyDischargeCalc{measurementFunction} = "1 hour average";
            $hourlyDischargeCalc{calculation}         = "avg";
            $hourlyDischargeCalc{parameters} =
              "measurement=$maxDischargeMeasurementName,intervals=12,lastTimestamp=$lastHourlyTimestamp";

            push @pendingCalculations, \%hourlyDischargeCalc;

            my %dailyDischargeCalc;
            $dailyDischargeCalc{sensorNode}          = $sensorNode;
            $dailyDischargeCalc{tableName}           = $oneDayTable;
            $dailyDischargeCalc{measurementName}     = $maxDischargeRateMeasurementName;
            $dailyDischargeCalc{measurementUnit}     = "m3/s";
            $dailyDischargeCalc{measurementFunction} = "1 day average";
            $dailyDischargeCalc{calculation}         = "avg";
            $dailyDischargeCalc{parameters} =
              "measurement=$maxDischargeMeasurementName,intervals=288,lastTimestamp=$lastDailyTimestamp";

            push @pendingCalculations, \%dailyDischargeCalc;
        }

        if ( defined $dischargeVolumeMeasurementIndex ) {
            updateFirstTimestamp( $dischargeVolumeMeasurementIndex, $destSamplingTime->[$firstDestTimeIndex] );
            updateLastTimestamp( $dischargeVolumeMeasurementIndex, $destSamplingTime->[$lastDestTimeIndex] );

            $allMeasurements[$dischargeVolumeMeasurementIndex]{measurementCalculation} =
              "300*" . getMeasurementCalculation($dischargeMeasurementIndex);

            my %hourlyDischargeCalc;
            $hourlyDischargeCalc{sensorNode}          = $sensorNode;
            $hourlyDischargeCalc{tableName}           = $oneHourTable;
            $hourlyDischargeCalc{measurementName}     = $dischargeVolumeMeasurementName;
            $hourlyDischargeCalc{measurementUnit}     = "m3";
            $hourlyDischargeCalc{measurementFunction} = "1 hour total";
            $hourlyDischargeCalc{calculation}         = "total";
            $hourlyDischargeCalc{parameters} =
"measurement=$sensorNode.$fiveMinuteTable.$dischargeVolumeMeasurementName,intervals=12,lastTimestamp=$lastHourlyTimestamp";

            push @pendingCalculations, \%hourlyDischargeCalc;

            my %dailyDischargeCalc;
            $dailyDischargeCalc{sensorNode}          = $sensorNode;
            $dailyDischargeCalc{tableName}           = $oneDayTable;
            $dailyDischargeCalc{measurementName}     = $dischargeVolumeMeasurementName;
            $dailyDischargeCalc{measurementUnit}     = "m3";
            $dailyDischargeCalc{measurementFunction} = "1 day total";
            $dailyDischargeCalc{calculation}         = "total";
            $dailyDischargeCalc{parameters} =
"measurement=$sensorNode.$oneHourTable.$dischargeVolumeMeasurementName,intervals=24,lastTimestamp=$lastDailyTimestamp";

            push @pendingCalculations, \%dailyDischargeCalc;
        }

        if ( defined $minDischargeVolumeMeasurementIndex ) {
            updateFirstTimestamp( $minDischargeVolumeMeasurementIndex, $destSamplingTime->[$firstDestTimeIndex] );
            updateLastTimestamp( $minDischargeVolumeMeasurementIndex, $destSamplingTime->[$lastDestTimeIndex] );

            $allMeasurements[$minDischargeVolumeMeasurementIndex]{displayName} .= " (95% CI)"
              if exists $allMeasurements[$minDischargeVolumeMeasurementIndex]{displayName}
              && index( $allMeasurements[$minDischargeVolumeMeasurementIndex]{displayName}, "(95% CI)" ) < 0;

            $allMeasurements[$minDischargeVolumeMeasurementIndex]{measurementCalculation} =
              "300*" . getMeasurementCalculation($minDischargeMeasurementIndex);

            my %hourlyDischargeCalc;
            $hourlyDischargeCalc{sensorNode}          = $sensorNode;
            $hourlyDischargeCalc{tableName}           = $oneHourTable;
            $hourlyDischargeCalc{measurementName}     = $minDischargeVolumeMeasurementName;
            $hourlyDischargeCalc{measurementUnit}     = "m3";
            $hourlyDischargeCalc{measurementFunction} = "1 hour total";
            $hourlyDischargeCalc{calculation}         = "total";
            $hourlyDischargeCalc{parameters} =
"measurement=$sensorNode.$fiveMinuteTable.$minDischargeVolumeMeasurementName,intervals=12,lastTimestamp=$lastHourlyTimestamp";

            push @pendingCalculations, \%hourlyDischargeCalc;

            my %dailyDischargeCalc;
            $dailyDischargeCalc{sensorNode}          = $sensorNode;
            $dailyDischargeCalc{tableName}           = $oneDayTable;
            $dailyDischargeCalc{measurementName}     = $minDischargeVolumeMeasurementName;
            $dailyDischargeCalc{measurementUnit}     = "m3";
            $dailyDischargeCalc{measurementFunction} = "1 day total";
            $dailyDischargeCalc{calculation}         = "total";
            $dailyDischargeCalc{parameters} =
"measurement=$sensorNode.$oneHourTable.$minDischargeVolumeMeasurementName,intervals=24,lastTimestamp=$lastDailyTimestamp";

            push @pendingCalculations, \%dailyDischargeCalc;
        }

        if ( defined $maxDischargeVolumeMeasurementIndex ) {
            updateFirstTimestamp( $maxDischargeVolumeMeasurementIndex, $destSamplingTime->[$firstDestTimeIndex] );
            updateLastTimestamp( $maxDischargeVolumeMeasurementIndex, $destSamplingTime->[$lastDestTimeIndex] );

            $allMeasurements[$maxDischargeVolumeMeasurementIndex]{displayName} .= " (95% CI)"
              if exists $allMeasurements[$maxDischargeVolumeMeasurementIndex]{displayName}
              && index( $allMeasurements[$maxDischargeVolumeMeasurementIndex]{displayName}, "(95% CI)" ) < 0;

            $allMeasurements[$maxDischargeVolumeMeasurementIndex]{measurementCalculation} =
              "300*" . getMeasurementCalculation($maxDischargeMeasurementIndex);

            my %hourlyDischargeCalc;
            $hourlyDischargeCalc{sensorNode}          = $sensorNode;
            $hourlyDischargeCalc{tableName}           = $oneHourTable;
            $hourlyDischargeCalc{measurementName}     = $maxDischargeVolumeMeasurementName;
            $hourlyDischargeCalc{measurementUnit}     = "m3";
            $hourlyDischargeCalc{measurementFunction} = "1 hour total";
            $hourlyDischargeCalc{calculation}         = "total";
            $hourlyDischargeCalc{parameters} =
"measurement=$sensorNode.$fiveMinuteTable.$maxDischargeVolumeMeasurementName,intervals=12,lastTimestamp=$lastHourlyTimestamp";

            push @pendingCalculations, \%hourlyDischargeCalc;

            my %dailyDischargeCalc;
            $dailyDischargeCalc{sensorNode}          = $sensorNode;
            $dailyDischargeCalc{tableName}           = $oneDayTable;
            $dailyDischargeCalc{measurementName}     = $maxDischargeVolumeMeasurementName;
            $dailyDischargeCalc{measurementUnit}     = "m3";
            $dailyDischargeCalc{measurementFunction} = "1 day total";
            $dailyDischargeCalc{calculation}         = "total";
            $dailyDischargeCalc{parameters} =
"measurement=$sensorNode.$oneHourTable.$maxDischargeVolumeMeasurementName,intervals=24,lastTimestamp=$lastDailyTimestamp";

            push @pendingCalculations, \%dailyDischargeCalc;
        }
    }
    elsif ( uc $calculation eq "WATERTABLEDEPTH" ) {

        my $fullSensorMeasurementNames = $parameterValues{sensorMeasurement};
        $fullSensorMeasurementNames = $parameterValues{sensorMeasurements}
          unless length $fullSensorMeasurementNames;
        $fullSensorMeasurementNames = $parameterValues{waterLevelSensor}
          unless length $fullSensorMeasurementNames;
        $fullSensorMeasurementNames = $parameterValues{waterLevelSensors}
          unless length $fullSensorMeasurementNames;
        my @waterLevelIndexes;
        foreach my $fullSensorMeasurementName ( split /&/, $fullSensorMeasurementNames ) {
            my $sourceMeasurementIndex = getMeasurementIndex($fullSensorMeasurementName);
            if ( !defined $sourceMeasurementIndex ) {
                $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                      . " referencing unspecified measurement \"$fullSensorMeasurementName\"" );
                next;
            }

            push @waterLevelIndexes, $sourceMeasurementIndex;
        }

        my $deploymentDepth = $parameterValues{deploymentDepth};
        my $riserHeight     = $parameterValues{riserHeight};
        if ( !defined $deploymentDepth || !defined $riserHeight ) {
            $log->warn("WARNING: Unspecified deployment depht or riser height in calculation \"$calculation\"");
            next;
        }

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            # Don't calculate measurement values which have been QC flagged
            next if defined $qlMeasurementIndex && defined $qcData[$qlMeasurementIndex]{$destTimeIndex};

            foreach my $sourceMeasurementIndex (@waterLevelIndexes) {
                inheiritQCFlag( $sourceMeasurementIndex, $destMeasurementIndex, $destTimeIndex );
            }

            my $waterHeightAboveSensor;
            foreach my $sourceMeasurementIndex (@waterLevelIndexes) {
                $waterHeightAboveSensor = $allData[$sourceMeasurementIndex][$destTimeIndex];
                last if defined $waterHeightAboveSensor;
            }
            next unless defined $waterHeightAboveSensor;

            my $waterTableDepth = $deploymentDepth - $riserHeight - $waterHeightAboveSensor;

            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $waterTableDepth;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $waterTableDepth;
        }

        my $waterLevelCalculations;
        foreach my $sourceMeasurementIndex (@waterLevelIndexes) {
            $waterLevelCalculations .= "&" if length $waterLevelCalculations;
            $waterLevelCalculations .= getMeasurementCalculation($sourceMeasurementIndex);
        }

        $calculationString = "waterTableDepth($waterLevelCalculations";
        $calculationString .= ",deploymentDepth=$deploymentDepth,riserHeight=$riserHeight)";
    }
    elsif ( uc $calculation eq "SEAFETPHINT" ) {

        # Realculate SeaFET internal pH based on measurements recorded by the SeaBird CTD

        my $tempIndex = getMeasurementIndex( $parameterValues{temp} );
        my $vIntIndex = getMeasurementIndex( $parameterValues{vInt} );
        my $k2I       = $parameterValues{k2_i};
        my $koI       = $parameterValues{ko_i};

        if (   !defined $tempIndex
            || !defined $vIntIndex
            || !defined $k2I
            || !defined $koI )
        {
            $log->warn("WARNING: Missing one or more required parameters to $calculation($parameters)");
            next;
        }

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            # Don't calculate measurement values which have been QC flagged
            next if defined $qlMeasurementIndex && defined $qcData[$qlMeasurementIndex]{$destTimeIndex};

            my $waterTemp = $allData[$tempIndex][$destTimeIndex];
            my $vInt      = $allData[$vIntIndex][$destTimeIndex];

            next unless defined $waterTemp && defined $vInt;

            # %(3)Temp
            # %(4)Salinity
            # %(5)Depth
            # %(6)Chlorophyll
            # %(7)SeaFET Internal Voltage
            # %(8)SeaFET External Voltage

            # From Katie:
            # Snerst = (($waterTemp+273.15).*(8.314472*log(10)))/96485.3415;
            # pH_int = (data(ck1,7)- cal_data(1) - (($waterTemp+273.15).*cal_data(2)))./Snerst;

            my $Snerst = ( ( $waterTemp + 273.15 ) * ( 8.314472 * 2.30258509299 ) ) / 96485.3415;
            my $calculatedPH = ( $vInt - $koI - ( ( $waterTemp + 273.15 ) * $k2I ) ) / $Snerst;

          # $log->info("$calculation $calculatedPH=(waterTemp=$waterTemp,vInt=$vInt,Snerst=$Snerst,k2I=$k2I,koI=$koI)");

            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $calculatedPH;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $calculatedPH;
        }

        $calculationString = "$calculation(k2I=$k2I,koI=$koI)";
    }
    elsif ( uc $calculation eq "SEAFETPHEXT" ) {

        # Realculate SeaFET internal pH based on measurements recorded by the SeaBird CTD

        my $tempIndex     = getMeasurementIndex( $parameterValues{temp} );
        my $salinityIndex = getMeasurementIndex( $parameterValues{salinity} );
        my $vExtIndex     = getMeasurementIndex( $parameterValues{vExt} );
        my $k2E           = $parameterValues{k2_e};
        my $koE           = $parameterValues{ko_e};

        if (   !defined $tempIndex
            || !defined $salinityIndex
            || !defined $vExtIndex
            || !defined $k2E
            || !defined $koE )
        {
            $log->warn("WARNING: Missing one or more required parameters to $calculation($parameters)");
            next;
        }

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            # Don't calculate measurement values which have been QC flagged
            next if defined $qlMeasurementIndex && defined $qcData[$qlMeasurementIndex]{$destTimeIndex};

            my $waterTemp = $allData[$tempIndex][$destTimeIndex];
            my $salinity  = $allData[$salinityIndex][$destTimeIndex];
            my $vExt      = $allData[$vExtIndex][$destTimeIndex];

            next
              unless defined $waterTemp
              && defined $salinity
              && defined $vExt;

            # %(3)Temp
            # %(4)Salinity
            # %(5)Depth
            # %(6)Chlorophyll
            # %(7)SeaFET Internal Voltage
            # %(8)SeaFET External Voltage

# From Katie:
# Cl = ($salinity./1.80655).*(0.99889/35.453); %total chloride
# Adh = (($waterTemp.^2).*0.00000343) + ($waterTemp.*0.00067524) + 0.49172143; %Debye-Huckel constant for activity of HCl
# I = ($salinity.*19.924)./(1000-($salinity.*1.005)); %Ionic strength
# St = ($salinity./1.80655).*(0.1400/96.062); %total sulfate
# Ks = (1-($salinity.*0.001005)).*exp((-4276.1./($waterTemp+273.15))+141.328-(23.093.*log(($waterTemp+273.15)))...
#    +(-13856./($waterTemp+273.15) + 324.57 - 47.986.*log(($waterTemp+273.15))).*sqrt(I)...
#    +((35474./($waterTemp+273.15)-771.54+114.723.*log($waterTemp+273.15)).*I)-((2698./($waterTemp+273.15)).*(I.^1.5))...
#    +((1776./($waterTemp+273.15)).*(I.^2))); %acid dissociation constant of HSO4-
# HCL = ((-Adh.*sqrt(I))./(1+1.394.*sqrt(I)))+(0.08885-0.000111.*$waterTemp).*I; %HCL = log10(HCL); %logarithm of HCL activity coefficient
# pH_ext = (($vExt-$koE-($k2E.*($waterTemp+273.15)))./Snerst)+log10(Cl)+(2.*HCL)-(log10(1+(St./Ks)));

            my $Snerst = ( ( $waterTemp + 273.15 ) * ( 8.314472 * 2.30258509299 ) ) / 96485.3415;
            my $sfCl = ( $salinity / 1.80655 ) * ( 0.99889 / 35.453 );    # total chloride
            my $sfAdh =
              ( ( $waterTemp**2 ) * 0.00000343 ) +
              ( $waterTemp * 0.00067524 ) + 0.49172143;                   # Debye-Huckel constant for activity of HCl
            my $sfI = ( $salinity * 19.924 ) / ( 1000 - ( $salinity * 1.005 ) );    # Ionic strength
            my $sfSt = ( $salinity / 1.80655 ) * ( 0.1400 / 96.062 );               # total sulfate
            my $sfKs =
              ( 1 - ( $salinity * 0.001005 ) ) *
              exp( ( -4276.1 / ( $waterTemp + 273.15 ) ) + 141.328 -
                  ( 23.093 * log( ( $waterTemp + 273.15 ) ) ) +
                  ( -13856 / ( $waterTemp + 273.15 ) + 324.57 - 47.986 * log( ( $waterTemp + 273.15 ) ) ) * sqrt($sfI)
                  + ( ( 35474 / ( $waterTemp + 273.15 ) - 771.54 + 114.723 * log( $waterTemp + 273.15 ) ) * $sfI ) -
                  ( ( 2698 / ( $waterTemp + 273.15 ) ) * ( $sfI**1.5 ) ) +
                  ( ( 1776 / ( $waterTemp + 273.15 ) ) * ( $sfI**2 ) ) );           # acid dissociation constant of HSO4
            my $sfHCL =
              ( ( -$sfAdh * sqrt($sfI) ) / ( 1 + 1.394 * sqrt($sfI) ) ) +
              ( 0.08885 - 0.000111 * $waterTemp ) * $sfI;    # HCL = log10(HCL); logarithm of HCL activity coefficient
            my $calculatedPH =
              ( ( $vExt - $koE - ( $k2E * ( $waterTemp + 273.15 ) ) ) / $Snerst ) +
              log10($sfCl) +
              ( 2 * $sfHCL ) -
              ( log10( 1 + ( $sfSt / $sfKs ) ) );

          # $log->info("$calculation $calculatedPH=(waterTemp=$waterTemp,vExt=$vExt,Snerst=$Snerst,k2E=$k2E,koE=$koE)");

            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $calculatedPH;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $calculatedPH;
        }

        $calculationString = "$calculation(k2E=$k2E,koE=$koE)";
    }
    elsif ( uc $calculation eq "SBE63OXYGEN" ) {

        # Calculate Oxygen values from raw CTD and SBE63 values
				# Define calibration coefficients for  SBE 63 Instrument 
				# A0 = 1.051300e+000
				# A1 = -1.500000e-003
				# A2 = 3.774741e-001
				# B0 = -2.352398e-001
				# B1 = 1.600563e+000
				# C0 = 1.027603e-001
				# C1 = 4.401757e-003
				# C2 = 6.224479e-005
				# TA0 = 7.004465e-004
				# TA1 = 2.538167e-004
				# TA2 = 3.551182e-007
				# TA3 = 1.081593e-007
				# pcor = 1.100000e-002


				# # Constant coeficients used for the salinity correction
				# SolB0 = -6.24523e-3
				# SolB1 = -7.37614e-3
				# SolB2 = -1.03410e-2
				# SolB3 = -8.17083e-3
				# SolC0 = -4.88682e-7
				
				# Input parameters from database
				# P = 0 
				# S = 0 
				# Vt = 0.83017 # SBE 63 temperature volt (~12.000C)
				# U = 25.57 # Phase delay (us)

        my $vTempIndex = getMeasurementIndex( $parameterValues{vTemp} );
        my $phaseDelayIndex = getMeasurementIndex( $parameterValues{phaseDelay} );
		my $pressureIndex = getMeasurementIndex( $parameterValues{pressure} );
		my $salinityIndex = getMeasurementIndex( $parameterValues{salinity} );
        my $A0       = $parameterValues{A0};
		my $A1       = $parameterValues{A1};
		my $A2       = $parameterValues{A2};
		my $B0       = $parameterValues{B0};
		my $B1       = $parameterValues{B1};
		my $B2       = $parameterValues{B2};
		my $C0       = $parameterValues{C0};
		my $C1       = $parameterValues{C1};
		my $C2       = $parameterValues{C2};
		my $TA0       = $parameterValues{TA0};
		my $TA1       = $parameterValues{TA1};
		my $TA2       = $parameterValues{TA2};
		my $TA3       = $parameterValues{TA3};
		my $pcor       = $parameterValues{pcor};
		my $SolB0       = $parameterValues{SolB0};
		my $SolB1       = $parameterValues{SolB1};
		my $SolB2       = $parameterValues{SolB2};
		my $SolB3       = $parameterValues{SolB3};
		my $SolC0       = $parameterValues{SolC0};


        if (   !defined $tempIndex
            || !defined $phaseDelayIndex
            || !defined $pressureIndex
			|| !defined $salinityIndex
			|| !defined $A0
			|| !defined $A1
			|| !defined $A2
			|| !defined $B0
			|| !defined $B1
			|| !defined $B2
			|| !defined $C0
			|| !defined $C1
			|| !defined $C2
			|| !defined $TA0
			|| !defined $TA1
			|| !defined $TA2
			|| !defined $TA3
			|| !defined $pcor
			|| !defined $SolB0
			|| !defined $SolB1
			|| !defined $SolB2
			|| !defined $SolB3
            || !defined $SolC0 )
        {
            $log->warn("WARNING: Missing one or more required parameters to $calculation($parameters)");
            next;
        }

        foreach my $destTimeIndex ( $firstDestTimeIndex .. $lastDestTimeIndex ) {

            # Don't calculate measurement values which have been QC flagged
            next if defined $qlMeasurementIndex && defined $qcData[$qlMeasurementIndex]{$destTimeIndex};

            my $vTemp = $allData[$tempIndex][$destTimeIndex];
            my $phaseDelay = $allData[$phaseDelayIndex][$destTimeIndex];
			my $pressure = $allData[$pressureIndex][$destTimeIndex];
			my $salinity = $allData[$salinityIndex][$destTimeIndex];

            next
              unless defined $vTemp
              && defined $phaseDelay
			  && defined $pressure
              && defined $salinity;



            # From Jessy: in python code
            # # Temperature from Voltage (Vt)
			# L = math.log(100000*Vt/(3.3-Vt))
			# T = 1/(TA0 + (TA1 * L) + (TA2 * L**2) + (TA3 * L**3)) - 273.15
			# print("{0} deg_celsius".format(T))

			# V = U/39.457071
			# E = pcor
			# # Ts
			# Ts = math.log((298.15 - T)/(273.15 + T))
			# Scor = math.exp(S * (SolB0 + SolB1 * Ts + SolB2 * Ts**2 + SolB3 * Ts**3)+SolC0 * S**2)
			# O2 = (((A0+A1*T + A2*V**2)/(B0+ B1*V) - 1)/(C0+C1*T + C2*T**2))*Scor*math.exp(E*P/(T+273.15))
			# print("{0} mL/L".format(O2))

			my $L = log(100000*$vTemp/(3.3-$vTemp))
			my $T = 1/($TA0 + ($TA1 * L) + ($TA2 * L**2) + ($TA3 * L**3)) - 273.15
			my $V = $phaseDelay/39.457071
			my $Ts = log((298.15 - $T)/(273.15 + $T))
			my $Scor = exp($salinity * ($SolB0 + $SolB1 * $Ts + $SolB2 * $Ts**2 + $SolB3 * $Ts**3)+$SolC0 * $salinity**2)
			my $O2 = ((($A0+$A1*$T + $A2*V**2)/($B0+ $B1*V) - 1)/($C0+$C1*$T + $C2*$T**2))*$Scor*exp($pcor*$pressure/($T+273.15))


          # $log->info("$calculation $calculatedPH=(waterTemp=$waterTemp,vInt=$vInt,Snerst=$Snerst,k2I=$k2I,koI=$koI)");

            if ( defined $allData[$destMeasurementIndex][$destTimeIndex] ) {
                $measurementsUpdated++
                  if $allData[$destMeasurementIndex][$destTimeIndex] != $O2;
            }
            else {
                $measurementsAdded++;
            }

            $allData[$destMeasurementIndex][$destTimeIndex] = $O2;
        }
		#What does this do?
        $calculationString = "$calculation(k2I=$k2I,koI=$koI)";
    }
	elsif ( uc $calculation eq "AGGREGATE" ) {
        my $sourceMeasurementIndex;
        if ( defined $parameterValues{measurement} ) {
            $sourceMeasurementIndex = getMeasurementIndex( $parameterValues{measurement} );
            if ( !defined $sourceMeasurementIndex ) {
                $log->warn( "WARNING: Ignoring calculation of \"$fullMeasurementName\""
                      . " referencing unspecified measurement \""
                      . $parameterValues{measurement}
                      . "\"" );
                next;
            }
        }
        else {
            $sourceMeasurementIndex = getMeasurementIndex( $sensorNode, $fiveMinuteTable, $measurementName );
            if ( !defined $sourceMeasurementIndex ) {
                $log->warn(
                    "WARNING: failed to find five minute measurement $sensorNode.$fiveMinuteTable.$measurementName");
                next;
            }
        }

        aggregateMeasurement( $destMeasurementIndex, $sourceMeasurementIndex, \$measurementsAdded );
    }
    else {
        $log->warn( "WARNING: Unrecognized calculation $calculation" . " found in CalculatedMeasurements worksheet" );
    }

    my $potentialCalculations = $lastDestTimeIndex - $firstDestTimeIndex + 1;
    my $logString             = "of $potentialCalculations values via $fullMeasurementName=";
    if ( defined $calculationString ) {
        $logString .= $calculationString;
    }
    else {
        $logString .= $calculation;
    }
    $logString .= " from $firstDestTimestamp" if defined $parameterValues{firstTimestamp};
    $logString .= " to $lastDestTimestamp"    if defined $parameterValues{lastTimestamp};

    if ( $measurementsAdded || $measurementsUpdated || $measurementsRemoved ) {

        if ( index( uc $calculation, "CLIP" ) < 0 ) {
            $allMeasurements[$destMeasurementIndex]{isReferenced} = 1;
            $allMeasurements[$qlMeasurementIndex]{isReferenced}   = 1 if defined $qlMeasurementIndex;
            $allMeasurements[$qcMeasurementIndex]{isReferenced}   = 1 if defined $qcMeasurementIndex;

            # Update the first and last measurement time of calculated measurements
            my $firstTimeIndex = 0;
            while ( $firstTimeIndex <= $lastDestTimeIndex ) {
                if ( defined $allData[$destMeasurementIndex][$firstTimeIndex] ) {
                    updateFirstTimestamp( $destMeasurementIndex, $destSamplingTime->[$firstTimeIndex] );
                    last;
                }

                $firstTimeIndex++;
            }

            my $lastTimeIndex = $lastDestTimeIndex;
            while ( $lastTimeIndex >= 0 ) {
                if ( defined $allData[$destMeasurementIndex][$lastTimeIndex] ) {
                    updateLastTimestamp( $destMeasurementIndex, $destSamplingTime->[$lastTimeIndex] );
                    last;
                }

                $lastTimeIndex--;
            }

        }

        $allMeasurements[$destMeasurementIndex]{measurementCalculation} = $calculationString
          if length $calculationString;

        $log->info("Added $measurementsAdded $logString")
          if $measurementsAdded;
        $log->info("Updated $measurementsUpdated $logString")
          if $measurementsUpdated;
        $log->info("Removed $measurementsRemoved $logString")
          if $measurementsRemoved;

        # Apply range clipping to calculated measurement values
        if (   ( $measurementsAdded || $measurementsUpdated )
            && index( uc $calculation, "CLIP" ) < 0
            && index( uc $calculation, "FILLGAPS" ) < 0
            && index( uc $calculation, "AGGREGATE" ) < 0
            && defined $allMeasurements[$destMeasurementIndex]{rangeClipCalculation} )
        {
            $log->info("Range clipping calculated measurement $fullMeasurementName");

            unshift @pendingCalculations, $allMeasurements[$destMeasurementIndex]{rangeClipCalculation};
        }
    }
    elsif ( uc $calculation eq "NORMALIZEAIRPRESSURE" || uc $calculation eq "RANGECLIP" ) {
        $log->debug("Updated zero $logString");
    }
    else {
        $log->warn("WARNING: Updated zero $logString");
    }
}

################################################################################
# Calculate hourly measurements from fiveMinute measurements
my %addedHourlyMeasurements;
$log->info("About to calculate hourly measurements from five minute measurements");
foreach my $currentMeasurement (@allMeasurements) {
    next
      unless $currentMeasurement->{isReferenced}
      && $currentMeasurement->{dataTable} eq $fiveMinuteTable
      && $currentMeasurement->{aggregateFiveMin} == 1
      && defined $currentMeasurement->{parentMeasurementIndex};

    my $fiveMinuteMeasurementIndex = $currentMeasurement->{parentMeasurementIndex};
    next if exists $addedHourlyMeasurements{$fiveMinuteMeasurementIndex};

    my $sensorNode      = $allMeasurements[$fiveMinuteMeasurementIndex]{sensorNode};
    my $dataTable       = $allMeasurements[$fiveMinuteMeasurementIndex]{dataTable};
    my $measurementName = $allMeasurements[$fiveMinuteMeasurementIndex]{measurementName};

    my $hourlyMeasurementIndex = getMeasurementIndex( $sensorNode, $oneHourTable, $measurementName );
    next unless defined $hourlyMeasurementIndex;

    $allTables{$sensorNode}{$oneHourTable}{sampleInterval} = 60;

    my $measurementsAdded = 0;
    aggregateMeasurement( $hourlyMeasurementIndex, $fiveMinuteMeasurementIndex, \$measurementsAdded );

    $addedHourlyMeasurements{$fiveMinuteMeasurementIndex}{fiveMinuteMeasurementIndex} = $fiveMinuteMeasurementIndex;
    $addedHourlyMeasurements{$fiveMinuteMeasurementIndex}{hourlyMeasurementIndex}     = $hourlyMeasurementIndex;
    $addedHourlyMeasurements{$fiveMinuteMeasurementIndex}{measurementsAdded}          = $measurementsAdded;
}
$log->info( "Finished calculating "
      . scalar( keys %addedHourlyMeasurements )
      . " hourly measurements from five minute measurements" );

################################################################################
# Calculate daily measurements from hourly measurements
my %addedDailyMeasurements;
$log->info("About to calculate daily measurements from hourly measurements");
foreach my $currentMeasurement (@allMeasurements) {
    next
      unless $currentMeasurement->{isReferenced}
      && $currentMeasurement->{dataTable} eq $oneHourTable
      && defined $currentMeasurement->{parentMeasurementIndex};

    my $hourlyMeasurementIndex = $currentMeasurement->{parentMeasurementIndex};
    next if exists $addedDailyMeasurements{$hourlyMeasurementIndex};

    my $sensorNode      = $allMeasurements[$hourlyMeasurementIndex]{sensorNode};
    my $dataTable       = $allMeasurements[$hourlyMeasurementIndex]{dataTable};
    my $measurementName = $allMeasurements[$hourlyMeasurementIndex]{measurementName};

    next if index( lc $measurementName, "winddir" ) >= 0    # Skip wind direction
      || index( $measurementName, "Rain" ) == 0             # Hourly rain amounts
      || index( $measurementName, "DischargeRate" ) == 0    # Hourly discharge rate
      || index( $measurementName, "DischargeVolume" ) == 0  # Hourly discharge volume
      || index( $measurementName, "HourlyTotal" ) >= 0      # Hourly totals (PAR, Solar, UVRad)
      || $measurementName =~ /\d+hour/;                     # Accumulated hourly measurements

    my $dailyMeasurementIndex = getMeasurementIndex( $sensorNode, $oneDayTable, $measurementName );
    next unless defined $dailyMeasurementIndex;

    $allTables{$sensorNode}{$oneDayTable}{sampleInterval} = 1440;

    my $measurementsAdded = 0;
    aggregateMeasurement( $dailyMeasurementIndex, $hourlyMeasurementIndex, \$measurementsAdded );

    $addedDailyMeasurements{$hourlyMeasurementIndex}{hourlyMeasurementIndex} = $hourlyMeasurementIndex;
    $addedDailyMeasurements{$hourlyMeasurementIndex}{dailyMeasurementIndex}  = $dailyMeasurementIndex;
    $addedDailyMeasurements{$hourlyMeasurementIndex}{measurementsAdded}      = $measurementsAdded;
}
$log->info(
    "Finished calculating " . scalar( keys %addedDailyMeasurements ) . " daily measurements from hourly measurements" );

################################################################################
# Log some statistical information
foreach my $sensorNode ( sort keys %allTables ) {
    foreach my $tableName ( sort keys %{ $allTables{$sensorNode} } ) {
        my $firstTimestamp      = $allTables{$sensorNode}{$tableName}{firstTimestamp};
        my $lastTimestamp       = $allTables{$sensorNode}{$tableName}{lastTimestamp};
        my $sampleInterval      = $allTables{$sensorNode}{$tableName}{sampleInterval};
        my $totalDataRecords    = 0;
        my $maximumDailyRecords = 0;

        if ( !defined $sampleInterval ) {
            $log->warn("WARNING: Failed to find sample interval associated with $sensorNode.$tableName, skipping");
            next;
        }

        if ( !defined $firstTimestamp || !defined $lastTimestamp ) {
            $log->warn("WARNING: Failed to find timestamps associated with $sensorNode.$tableName, skipping")
              unless $sensorNode =~ /^QuadraFTS/
              || $sensorNode =~ /^SA_/
              || $sensorNode eq "Portable"
              || $sensorNode eq "PruthMooring"
              || $sensorNode eq "QU5_Mooring"
              || $sensorNode eq "WTS693Lake";
            next;
        }

        # Skip daily stats, as they are all derived from the hourly data
        next if $sampleInterval == 1440;

        foreach my $dataDate ( keys %{ $allTables{$sensorNode}{$tableName}{dailyRecordCounts} } ) {
            my $dailyRecordCount =
              $allTables{$sensorNode}{$tableName}{dailyRecordCounts}{$dataDate};
            $maximumDailyRecords = $dailyRecordCount
              if $dailyRecordCount > $maximumDailyRecords;
            $totalDataRecords += $dailyRecordCount;
        }

        $log->debug( "Found $totalDataRecords records in $sensorNode:$tableName"
              . ", from $firstTimestamp to $lastTimestamp"
              . ", sampling interval of $sampleInterval minutes"
              . ", and $maximumDailyRecords records per day" );
    }
}

# Database views that span more then one sensor node/database table
my %jointDatabaseViews = (
    "QuadraLimpetCombined:5minuteSamples" => [
        {
            sensorNode => "QuadraLimpet",
            tableName  => "5minuteSamples"
        },
        {
            sensorNode => "QuadraLimpetSeaFET",
            tableName  => "5minuteSamples"
        }
    ],
    "KCBuoyCombined:1hourSamples" => [
        {
            sensorNode => "KCBuoy",
            tableName  => "1hourSamples"
        },
        {
            sensorNode => "KCSeaology",
            tableName  => "1hourSamples"
        }
    ]
);

################################################################################
# Update the database as required
if ( $syncDB || $compareDB || $rebuildDB ) {
    my $dbh = DBI->connect( "DBI:Pg:dbname=$destinationDB;host=$pgHost",
        $pgUser, $pgPassword, { 'AutoCommit' => 0, 'RaiseError' => 1 } );

    # Synchronize measurement information to the database
    foreach my $sensorNode ( sort keys %allTables ) {
        foreach my $tableName ( sort keys %{ $allTables{$sensorNode} } ) {

            next if lc $tableName eq $diagnosticsTable;

            syncToDB( $dbh, "sn", $sensorNode, $tableName );
        }
    }

    # Create database views that span more then one sensor node
    foreach my $dbViewName ( keys %jointDatabaseViews ) {

        my $viewNeeded;
        foreach my $dataTable ( @{ $jointDatabaseViews{$dbViewName} } ) {
            next
              unless exists $updatedDatabaseViews{ $dataTable->{sensorNode} }
              && exists $updatedDatabaseViews{ $dataTable->{sensorNode} }{ $dataTable->{tableName} };

            $viewNeeded = 1;
            last;
        }
        next unless $viewNeeded;

        my @viewColumnNames;
        my @tableColumnNames;

        my $firstDatabaseTable;
        my @otherDatabaseTables;

        foreach my $dataTable ( @{ $jointDatabaseViews{$dbViewName} } ) {

            my $tableAdded;

            for my $measurementIndex ( 0 .. $#allMeasurements ) {
                next
                  unless exists $allMeasurements[$measurementIndex]{isReferenced}
                  && exists $allMeasurements[$measurementIndex]{databaseTable}
                  && exists $allMeasurements[$measurementIndex]{databaseColumn}
                  && $allMeasurements[$measurementIndex]{sensorNode} eq $dataTable->{sensorNode}
                  && $allMeasurements[$measurementIndex]{dataTable} eq $dataTable->{tableName};

                my $databaseTable = "sn." . $allMeasurements[$measurementIndex]{databaseTable};

                if ( !$tableAdded ) {
                    if ( !defined $firstDatabaseTable ) {
                        $firstDatabaseTable = $databaseTable;
                    }
                    else {
                        push @otherDatabaseTables, $databaseTable;
                    }

                    $tableAdded = 1;
                }

                next if uc( $allMeasurements[$measurementIndex]{measurementName} ) eq "RECORD";  # Exclude RECORD number

                # Swap QC and QL columns in the database view
                push @viewColumnNames, $allMeasurements[$measurementIndex]{sensorNode} . ":"
                  . $allMeasurements[$measurementIndex]{measurementName};

                # Convert lat and long from Degrees Minutes (DM) to Decimal Degrees
                my $tableColumnName = $databaseTable . "." . $allMeasurements[$measurementIndex]{databaseColumn};
                if ( index( uc( $allMeasurements[$measurementIndex]{measurementName} ), "LATITUDE" ) >= 0 ) {
                    $tableColumnName =
                      "TRUNC($tableColumnName/100)+($tableColumnName-100*TRUNC($tableColumnName/100))/60";
                }
                elsif ( index( uc( $allMeasurements[$measurementIndex]{measurementName} ), "LONGITUDE" ) >= 0 ) {
                    $tableColumnName =
                      "-1*(TRUNC($tableColumnName/100)+($tableColumnName-100*TRUNC($tableColumnName/100))/60)";
                }
                elsif ( $tableColumnName =~ /_uql$/ ) {
                    $tableColumnName = "COALESCE($tableColumnName,2)";
                }

                push @tableColumnNames, $tableColumnName;
            }
        }

        my $sql =
            "CREATE VIEW sn.\"$dbViewName\" (\"measurementTime\",\""
          . join( "\",\"", @viewColumnNames )
          . "\") AS SELECT $firstDatabaseTable.measurement_time,"
          . join( ",", @tableColumnNames )
          . " FROM $firstDatabaseTable";

        foreach my $databaseTable (@otherDatabaseTables) {
            $sql .=
" LEFT OUTER JOIN $databaseTable on ($databaseTable.measurement_time=$firstDatabaseTable.measurement_time)";
        }

        $dbh->do("DROP VIEW IF EXISTS  sn.\"$dbViewName\"");
        $dbh->do($sql);
        $dbh->do("ALTER VIEW sn.\"$dbViewName\" OWNER TO hakai_admin");
        $dbh->do("GRANT ALL ON sn.\"$dbViewName\" TO hakai_admin WITH GRANT OPTION");
        $dbh->do("GRANT SELECT ON sn.\"$dbViewName\" TO hakai_read_only");
        $dbh->do("GRANT SELECT ON sn.\"$dbViewName\" TO hakai_read_write");
        $dbh->commit;

        $log->info("[$destinationDB:$dbViewName] Created database view $dbViewName");
    }

    # Add or update measurements to the database
    my @dbRows;
    my $addedMeasurements            = 0;
    my $updatedFirstMeasurementTimes = 0;
    foreach my $currentMeasurement (@allMeasurements) {
        next unless $currentMeasurement->{isReferenced};

        my $sensorNode             = $currentMeasurement->{sensorNode};
        my $dataTable              = $currentMeasurement->{dataTable};
        my $measurementName        = $currentMeasurement->{measurementName};
        my $standardName           = $currentMeasurement->{standardName};
        my $displayName            = $currentMeasurement->{displayName};
        my $measurementType        = $currentMeasurement->{measurementType};
        my $function               = $currentMeasurement->{function};
        my $units                  = $currentMeasurement->{units};
        my $elevation              = $currentMeasurement->{elevation};
        my $sensorType             = $currentMeasurement->{sensorType};
        my $serialNumber           = $currentMeasurement->{serialNumber};
        my $sensorDescription      = $currentMeasurement->{sensorDescription};
        my $sensorDocumentation    = $currentMeasurement->{sensorDocumentation};
        my $comments               = $currentMeasurement->{comments};
        my $measurementCalculation = $currentMeasurement->{measurementCalculation};
        my $firstMeasurementTime   = $currentMeasurement->{firstTimestamp};
        my $databaseTable          = $currentMeasurement->{databaseTable};
        my $databaseColumn         = $currentMeasurement->{databaseColumn};

        my $createdTime = DateTime->now( time_zone => 'UTC' );
        $createdTime->subtract( hours => 8 );
        my $createdTimeString = $createdTime . "-0800";
        $createdTimeString =~ s/T/ /g;

        my $importFlag = 'f';
        $importFlag = 't' if $currentMeasurement->{importFlag} == 1;
        my $exportFlag = 'f';
        $exportFlag = 't' if $currentMeasurement->{exportFlag} == 1;

        my $measurementCalculationDB = $currentMeasurement->{measurementCalculation};
        my $firstMeasurementTimeDB   = $currentMeasurement->{firstTimestamp};
        $firstMeasurementTimeDB .= "-0800" if defined $firstMeasurementTimeDB;

        $standardName             = "\\N" unless defined $standardName;
        $displayName              = "\\N" unless defined $displayName;
        $measurementType          = "\\N" unless defined $measurementType;
        $function                 = "\\N" unless defined $function;
        $units                    = "\\N" unless defined $units;
        $elevation                = "\\N" unless defined $elevation;
        $sensorType               = "\\N" unless defined $sensorType;
        $serialNumber             = "\\N" unless defined $serialNumber;
        $sensorDescription        = "\\N" unless defined $sensorDescription;
        $sensorDocumentation      = "\\N" unless defined $sensorDocumentation;
        $comments                 = "\\N" unless defined $comments;
        $measurementCalculationDB = "\\N" unless defined $measurementCalculationDB;
        $firstMeasurementTimeDB   = "\\N" unless defined $firstMeasurementTimeDB;
        $databaseTable            = "\\N" unless defined $databaseTable;
        $databaseColumn           = "\\N" unless defined $databaseColumn;

        my $dbRow =
            "$sensorNode\t$dataTable\t$measurementName"
          . "\t$standardName\t$displayName\t$measurementType\t$function\t$units\t$elevation\t$sensorType\t$serialNumber"
          . "\t$sensorDescription\t$sensorDocumentation\t$comments\t$measurementCalculationDB"
          . "\t$createdTimeString\t$firstMeasurementTimeDB\t$databaseTable"
          . "\t$databaseColumn\t$importFlag\t$exportFlag";

        my $fullMeasurementName = "$sensorNode.$dataTable.$measurementName";
        my $measurementKey      = lc $fullMeasurementName;
        if ( $rebuildDB || !exists $dbMeasurements{$measurementKey} ) {
            push @dbRows, $dbRow;

            $addedMeasurements++;

            $log->info("[$fullMeasurementName] Added measurement to the database") unless $rebuildDB;
        }
        else {
            my $currentFirstMeasurementTime = $dbMeasurements{$measurementKey}{firstMeasurementTime};
            if ( defined $firstMeasurementTime
                && ( !defined $currentFirstMeasurementTime || $currentFirstMeasurementTime gt $firstMeasurementTime ) )
            {
                $dbh->do(
                    "UPDATE sn.measurements SET first_measurement_time=?,measurement_calculation=?"
                      . " WHERE sensor_node=? AND data_table=? AND measurement_name=?",
                    undef,
                    $firstMeasurementTimeDB,
                    $measurementCalculation,
                    $sensorNode,
                    $dataTable,
                    $measurementName
                );

                $dbh->commit;

                $updatedFirstMeasurementTimes++;

                $measurementCalculation = "" if !defined $measurementCalculation;

                if ( defined $currentFirstMeasurementTime ) {
                    $log->info( "[$fullMeasurementName] First measurement time changed"
                          . " from \"$currentFirstMeasurementTime\" to \"$firstMeasurementTime\""
                          . " and measurement calculation set to \"$measurementCalculation\"" );
                }
                else {
                    $log->info( "[$fullMeasurementName] First measurement time set to \"$firstMeasurementTime\""
                          . " and measurement calculation set to \"$measurementCalculation\"" );
                }
            }

            my @measurementUpdates;
            foreach my $propertyName (
                'standardName',      'displayName',         'measurementType', 'function',
                'units',             'elevation',           'sensorType',      'serialNumber',
                'sensorDescription', 'sensorDocumentation', 'comments'
              )
            {
                my $dbValue;
                $dbValue = $dbMeasurements{$measurementKey}{$propertyName}
                  if defined $dbMeasurements{$measurementKey}{$propertyName}
                  && length $dbMeasurements{$measurementKey}{$propertyName};

                my $newValue;
                $newValue = $currentMeasurement->{$propertyName}
                  if defined $currentMeasurement->{$propertyName} && length $currentMeasurement->{$propertyName};

                if ( !defined $dbValue && defined $newValue ) {
                    push @measurementUpdates, "setting $propertyName to $newValue";
                    $dbMeasurements{$measurementKey}{$propertyName} = $newValue;
                }
                elsif ( defined $dbValue && !defined $newValue ) {
                    push @measurementUpdates, "removing $propertyName";
                    undef $dbMeasurements{$measurementKey}{$propertyName};
                }
                elsif ( defined $dbValue && defined $newValue && $dbValue ne $newValue ) {
                    push @measurementUpdates, "changing $propertyName from $dbValue to $newValue";
                    $dbMeasurements{$measurementKey}{$propertyName} = $newValue;
                }
            }

            if (@measurementUpdates) {

                $log->info( "[$fullMeasurementName] " . join( ", ", @measurementUpdates ) );

                $dbh->do(
"UPDATE sn.measurements SET standard_name=?,display_name=?,measurement_type=?,measurement_function=?,measurement_units=?"
                      . ",elevation=?,sensor_type=?,serial_number=?,sensor_description=?,sensor_documentation=?,sensor_comments=?"
                      . " WHERE sensor_node=? AND data_table=? AND measurement_name=?",
                    undef,
                    $currentMeasurement->{standardName},
                    $currentMeasurement->{displayName},
                    $currentMeasurement->{measurementType},
                    $currentMeasurement->{function},
                    $currentMeasurement->{units},
                    $currentMeasurement->{elevation},
                    $currentMeasurement->{sensorType},
                    $currentMeasurement->{serialNumber},
                    $currentMeasurement->{sensorDescription},
                    $currentMeasurement->{sensorDocumentation},
                    $currentMeasurement->{comments},
                    $sensorNode,
                    $dataTable,
                    $measurementName
                );

                $dbh->commit;
            }
        }
    }

    if ( scalar(@dbRows) > 0 ) {
        if ($rebuildDB) {
            $dbh->do("TRUNCATE TABLE sn.measurements");

            $log->info("Removed all existing measurement records");
        }

        $dbh->do( "COPY sn.measurements (sensor_node, data_table, measurement_name"
              . ", standard_name, display_name, measurement_type, measurement_function, measurement_units"
              . ", elevation, sensor_type, serial_number, sensor_description, sensor_documentation, sensor_comments"
              . ", measurement_calculation, created_time, first_measurement_time"
              . ", database_table, database_column, import_flag, export_flag) FROM STDIN" );

        foreach my $dbRow (@dbRows) {
            $dbh->pg_putcopydata("$dbRow\n");
        }

        $dbh->pg_putcopyend();
        $dbh->commit;
    }

    $log->info("Added $addedMeasurements measurement records to database") if $addedMeasurements;
    $log->info("Updated $updatedFirstMeasurementTimes first measurement times in database")
      if $updatedFirstMeasurementTimes;

    $dbh->disconnect;
}

################################################################################
# Process sensor network alerts
my %allAlerts;

my $dbh = DBI->connect( "DBI:Pg:dbname=$destinationDB;host=$pgHost",
    $pgUser, $pgPassword, { 'AutoCommit' => 1, 'RaiseError' => 1 } );

sub updateAlert {
    my ( $alertID, $alertSignature, $startedDT, $alertPriority, $alertTitle, $alertDescription, $alertMetadata ) = @_;

    return if exists $allAlerts{$alertSignature}{inserted} || exists $allAlerts{$alertSignature}{updated};

    my $jsonAlertMetadata = to_json( \%{$alertMetadata} );
    $startedDT .= "-0800" if defined $startedDT;

    $dbh->do(
"UPDATE sn.alerts SET dt_updated=current_timestamp, dt_started=?, priority=?, title=?, description=?, metadata=? WHERE alert_id=?",
        undef, $startedDT, $alertPriority, $alertTitle, $alertDescription, $jsonAlertMetadata, $alertID
    );

    $log->info("[SN alerts] Updated [$alertSignature] $alertTitle ($alertPriority)");

    $allAlerts{$alertSignature}{updated}           = 1;
    $allAlerts{$alertSignature}{startedDT}         = $startedDT;
    $allAlerts{$alertSignature}{alertPriority}     = $alertPriority;
    $allAlerts{$alertSignature}{alertTitle}        = $alertTitle;
    $allAlerts{$alertSignature}{alertDescription}  = $alertDescription;
    $allAlerts{$alertSignature}{jsonAlertMetadata} = $jsonAlertMetadata;
}

sub saveAlert {
    my ( $alertSignature, $startedDT, $alertPriority, $alertTitle, $alertDescription, $alertMetadata ) = @_;

    # Update existing alert, if already exists
    return updateAlert( $allAlerts{$alertSignature}{alertID},
        $alertSignature, $startedDT, $alertPriority, $alertTitle, $alertDescription, $alertMetadata )
      if exists $allAlerts{$alertSignature};

    my $jsonAlertMetadata = to_json( \%{$alertMetadata} );
    $startedDT .= "-0800" if defined $startedDT;

    $dbh->do(
        "INSERT INTO sn.alerts (dt_opened, dt_updated, signature, dt_started, priority, title, description, metadata)"
          . " VALUES (current_timestamp,current_timestamp,?,?,?,?,?,?)",
        undef, $alertSignature, $startedDT, $alertPriority, $alertTitle, $alertDescription, $jsonAlertMetadata
    );

    $log->info("[SN alerts] Added [$alertSignature] $alertTitle ($alertPriority)");

    $allAlerts{$alertSignature}{inserted}          = 1;
    $allAlerts{$alertSignature}{alertSignature}    = $alertSignature;
    $allAlerts{$alertSignature}{startedDT}         = $startedDT;
    $allAlerts{$alertSignature}{alertPriority}     = $alertPriority;
    $allAlerts{$alertSignature}{alertTitle}        = $alertTitle;
    $allAlerts{$alertSignature}{alertDescription}  = $alertDescription;
    $allAlerts{$alertSignature}{jsonAlertMetadata} = $jsonAlertMetadata;
}

sub closeAlert {
    my ( $alertID, $alertSignature ) = @_;

    $dbh->do( "UPDATE sn.alerts SET dt_closed=current_timestamp WHERE alert_id=?", undef, $alertID );

    my $alertTitle = $allAlerts{$alertSignature}{alertTitle};

    $log->info("[SN alerts] Closed [$alertSignature] $alertTitle");

    $allAlerts{$alertSignature}{closed} = 1;
}

my $sth =
  $dbh->prepare(
"SELECT alert_id, signature, dt_started, dt_dismissed, priority, title, description, metadata FROM sn.alerts WHERE dt_closed IS NULL"
  );
$sth->execute();
my ( $alertID, $alertSignature, $startedDT, $dismissedDT, $alertPriority, $alertTitle, $alertDescription,
    $jsonAlertMetadata );

$sth->bind_columns( \$alertID, \$alertSignature, \$startedDT, \$dismissedDT, \$alertPriority, \$alertTitle,
    \$alertDescription, \$jsonAlertMetadata );

while ( $sth->fetch ) {

    if ( exists $allAlerts{$alertSignature} ) {
        $log->warn("WARNING: found more then one open alert with signature $alertSignature, ignoring");
        next;
    }

    $allAlerts{$alertSignature}{alertID}           = $alertID;
    $allAlerts{$alertSignature}{alertSignature}    = $alertSignature;
    $allAlerts{$alertSignature}{startedDT}         = $startedDT;
    $allAlerts{$alertSignature}{dismissedDT}       = $dismissedDT if defined $dismissedDT;
    $allAlerts{$alertSignature}{alertTitle}        = $alertTitle;
    $allAlerts{$alertSignature}{alertPriority}     = $alertPriority;
    $allAlerts{$alertSignature}{alertDescription}  = $alertDescription;
    $allAlerts{$alertSignature}{jsonAlertMetadata} = $jsonAlertMetadata;
}
$log->info( "Found " . scalar( keys %allAlerts ) . " existing/open alerts in the database" );

my @sensorNodes;
my $maxTimeIndex;
my $maxTimestamp;

# Find the latest timestamp across all sensor nodes and keep a list of sensor nodes with data
foreach my $sensorNode ( sort keys %allTables ) {
    next
      unless exists $allTables{$sensorNode}{$oneHourTable}
      && exists $allTables{$sensorNode}{$oneHourTable}{lastTimestamp};

    push @sensorNodes, $sensorNode;

    my $lastTimestamp = $allTables{$sensorNode}{$oneHourTable}{lastTimestamp};
    my $lastTimeIndex = $hourlySamplingTimeIndex{$lastTimestamp};

    next if defined $maxTimestamp && $maxTimestamp ge $lastTimestamp;

    $maxTimestamp = $lastTimestamp;
    $maxTimeIndex = $lastTimeIndex;
}

# Calculate stats across all sensor nodes and store in the database
my %sensorNodeStats;
foreach my $sensorNode (@sensorNodes) {

    next
      unless exists $allTables{$sensorNode}{$oneHourTable}
      && exists $allTables{$sensorNode}{$oneHourTable}{lastTimestamp};

    $sensorNodeStats{$sensorNode}{sensorNode}    = $sensorNode;
    $sensorNodeStats{$sensorNode}{lastTimestamp} = $allTables{$sensorNode}{$oneHourTable}{lastTimestamp};

    foreach my $measurementName (
        'BattVolt_Avg',   'DumpVolt_Avg',  'PanelTemp_Avg',  'EnclosureTemp_Avg',
        'AirTemp_Avg',    'WaterTemp_Avg', 'LithiumBattery', 'SkippedScan',
        'WatchdogErrors', 'pCO2_uatm_Avg', 'TSG_T_Avg',      'TSG_S_Avg',
        'calcpH_Avg'
      )
    {
        my $measurementIndex = getMeasurementIndex( $sensorNode, $diagnosticsTable, $measurementName );
        $measurementIndex = getMeasurementIndex( $sensorNode, $oneHourTable, $measurementName )
          if !defined $measurementIndex;

        next unless defined $measurementIndex;

        $sensorNodeStats{$sensorNode}{lastMeasurements}{$measurementName}{index} = $measurementIndex;
    }

    my $westBeachVoltMeasurementIndex = getMeasurementIndex( $sensorNode, $oneHourTable, "WestBeachVolt_Avg" );
    if ( defined $westBeachVoltMeasurementIndex ) {
        my $westBeachNodeKey = "WestBeach";

        $sensorNodeStats{$westBeachNodeKey}{sensorNode}    = $sensorNode;
        $sensorNodeStats{$westBeachNodeKey}{lastTimestamp} = $sensorNodeStats{$sensorNode}{lastTimestamp};
        $sensorNodeStats{$westBeachNodeKey}{lastMeasurements}{BattVolt_Avg}{index} = $westBeachVoltMeasurementIndex;
    }
}

foreach my $sensorNodeKey ( sort keys %sensorNodeStats ) {
    next unless exists $sensorNodeStats{$sensorNodeKey}{lastMeasurements};

    my %lastMeasurements;
    foreach my $measurementID ( sort keys %{ $sensorNodeStats{$sensorNodeKey}{lastMeasurements} } ) {
        my $measurementIndex = $sensorNodeStats{$sensorNodeKey}{lastMeasurements}{$measurementID}{index};
        next unless defined $measurementIndex;

        my $timeIndex = $hourlySamplingTimeIndex{ $sensorNodeStats{$sensorNodeKey}{lastTimestamp} };
        while ( $timeIndex >= 0 && !defined $allData[$measurementIndex][$timeIndex] ) {
            $timeIndex--;
        }
        next unless $timeIndex > 0;

        my $sensorNode       = $allMeasurements[$measurementIndex]{sensorNode};
        my $dataTable        = $allMeasurements[$measurementIndex]{dataTable};
        my $measurementName  = $allMeasurements[$measurementIndex]{measurementName};
        my $measurementTime  = $hourlySamplingTime[$timeIndex];
        my $measurementValue = $allData[$measurementIndex][$timeIndex];

        $log->debug("[$sensorNodeKey] $sensorNode.$dataTable.$measurementName [$measurementTime]=$measurementValue");

        $lastMeasurements{$measurementID}{sensorNode}      = $sensorNode;
        $lastMeasurements{$measurementID}{dataTable}       = $dataTable;
        $lastMeasurements{$measurementID}{measurementName} = $measurementName;
        $lastMeasurements{$measurementID}{units}           = $allMeasurements[$measurementIndex]{units}
          if exists $allMeasurements[$measurementIndex]{units};
        $lastMeasurements{$measurementID}{displayName} = $allMeasurements[$measurementIndex]{displayName}
          if exists $allMeasurements[$measurementIndex]{displayName};
        $lastMeasurements{$measurementID}{measurementTime}  = $measurementTime;
        $lastMeasurements{$measurementID}{measurementValue} = $measurementValue;
    }

    my $sensorNode           = $sensorNodeStats{$sensorNodeKey}{sensorNode};
    my $dtLastseen           = $sensorNodeStats{$sensorNodeKey}{lastTimestamp} . "-0800";
    my $jsonLastMeasurements = to_json( \%lastMeasurements );

    $dbh->do(
        "INSERT INTO sn.sensor_node_status (sensor_node, dt_updated, dt_lastseen, last_measurements)"
          . " VALUES (\$1,current_timestamp,\$2,\$3) ON CONFLICT (sensor_node)"
          . " DO UPDATE SET dt_updated=current_timestamp, dt_lastseen=\$2, last_measurements=\$3",
        undef, $sensorNodeKey, $dtLastseen, $jsonLastMeasurements
    );

    $log->info( "[$sensorNodeKey] Added/updated sensor node status in db, lastTimestamp="
          . $sensorNodeStats{$sensorNodeKey}{lastTimestamp} );
}

# Make it easier to ignore specific sensor nodes
my %ignoredSensorNodes;
my %onlineSensorNodes;
my %offlineSensorNodes;

#$ignoredSensorNodes{BuxtonEast}{reason}         = "Node offline";
$ignoredSensorNodes{Portable}{reason}     = "Decommissioned in September 2017";
$ignoredSensorNodes{QuadraFTS}{reason}    = "FTSs offline indefinitely";
$ignoredSensorNodes{QuadraFTS2}{reason}   = "FTSs offline indefinitely";
$ignoredSensorNodes{KetchikanBoL}{reason} = "Ketchikan BoL indefinitely";

$ignoredSensorNodes{QuadraLimpet}{reason}       = "Problems being investigated";
$ignoredSensorNodes{QuadraLimpetSeaFET}{reason} = "Temporarily offline";

#$ignoredSensorNodes{QuadraBoL}{reason}      = "Ignoring Burke-o-Lators";
#$ignoredSensorNodes{SewardBoL}{reason}      = "Ignoring Burke-o-Lators";
#$ignoredSensorNodes{KetchikanBoL}{reason}   = "Ignoring Burke-o-Lators";
#$ignoredSensorNodes{BaynesSoundBoL}{reason} = "Ignoring Burke-o-Lators";
#$ignoredSensorNodes{SitkaBoL}{reason}       = "Ignoring Burke-o-Lators";
$ignoredSensorNodes{PruthMooring}{reason} = "Ignoring non-networked sensors nodes";
$ignoredSensorNodes{QU5_Mooring}{reason}  = "Ignoring non-networked sensors nodes";
$ignoredSensorNodes{WTS693Lake}{reason}   = "Ignoring non-networked sensors nodes";

# Raise alert for all nodes that are more then 4 hours behind the most recent
foreach my $sensorNode (@sensorNodes) {

    $onlineSensorNodes{$sensorNode}{sensorNode} = $sensorNode;

    # Skip sensor nodes that are currently being ignored
    next if exists $ignoredSensorNodes{$sensorNode};

    # Skip the Standalone nodes
    next if index( $sensorNode, "SA_" ) == 0;

    my $lastTimestamp = $allTables{$sensorNode}{$oneHourTable}{lastTimestamp};
    my $lastTimeIndex = $hourlySamplingTimeIndex{$lastTimestamp};

    next unless defined $lastTimeIndex;

    # OK if falling 4 hours or less behind the most recent node
    my $hoursBehind = $maxTimeIndex - $lastTimeIndex;
    next if $hoursBehind <= 4;

    $offlineSensorNodes{$sensorNode}{hoursBehind} = $hoursBehind;

    my $alertType = "Node offline";

    my $alertPriority = 1;    # Medium priority
    $alertPriority = 2 if $hoursBehind >= 7;     # High priority if offline for 7 hours or more
    $alertPriority = 3 if $hoursBehind >= 10;    # Critical priority if offline for 10 hours or more

    $alertPriority = 0 if $sensorNode =~ /BoL$/; # All BoL alerts are low priority

    # KCSeaology may fall further behind
    $alertPriority = 0 if ( $sensorNode eq "KCSeaology" || $sensorNode eq "KCBuoy" ) && $hoursBehind < 24;

    my $alertSignature   = "$alertType.$sensorNode";
    my $alertTitle       = "$sensorNode is falling behind";
    my $alertDescription = "$sensorNode has fallen $hoursBehind hours behind other sensor nodes";

    my %alertMetadata;
    $alertMetadata{alertType}     = $alertType;
    $alertMetadata{sensorNode}    = $sensorNode;
    $alertMetadata{dataTable}     = $oneHourTable;
    $alertMetadata{lastTimestamp} = $lastTimestamp;
    $alertMetadata{hoursBehind}   = $hoursBehind;

    saveAlert( $alertSignature, $lastTimestamp, $alertPriority, $alertTitle, $alertDescription, \%alertMetadata );
}

# Raise alert for any meaurements that have reported data in the last 4 weeks, but not in the last fours hours
foreach my $currentMeasurement (@allMeasurements) {
    next
      unless ( $currentMeasurement->{dataTable} eq $oneHourTable && defined $currentMeasurement->{isRead} )
      || ( $currentMeasurement->{dataTable} eq $fiveMinuteTable
        && defined $currentMeasurement->{isRead}
        && ( $currentMeasurement->{aggregateFiveMin} == 1 || $currentMeasurement->{measurementName} eq "Rain" ) );

    my $sensorNode       = $currentMeasurement->{sensorNode};
    my $dataTable        = $currentMeasurement->{dataTable};
    my $measurementName  = $currentMeasurement->{measurementName};
    my $measurementIndex = $currentMeasurement->{measurementIndex};

    # Skip sensor nodes that are currently being ignored
    next if exists $ignoredSensorNodes{$sensorNode};

    # Skip sensor nodes that are not online
    next unless exists $onlineSensorNodes{$sensorNode};

    # Don't handle these particular alerts if the node has been offline for more then 10 days
    next if exists $offlineSensorNodes{$sensorNode} && $offlineSensorNodes{$sensorNode}{hoursBehind} > 240;

    # Skip the Standalone nodes
    next if index( $sensorNode, "SA_" ) == 0;

    # Skip SSN819 PLS sensor, as per note from Shawn
    next if $sensorNode eq "SSN819" && $measurementName =~ /^PLS_/;

    next if $measurementName =~ /^SR50A_TC_Distance/;    # Skip temperature corrected SR50 measurements
    next if $measurementName =~ /^SnowDepth/;            # Skip calculated snow depth measurements
    next if $measurementName =~ /_wtd$/;                 # Skip calculated water table depths
    next if $measurementName =~ /^cmWell/;               # Skip scaled well water levels
    next if $measurementName =~ /^cmPLS/;                # Skip scaled stream water levels
    next if $measurementName =~ /^WBJ/;                  # Skip charge controller related measurements
    next if $measurementName =~ /^Kid/;                  # Skip charge controller related measurements

    # Added based on email from Shawn in September, 2019.  These sensor have been removed
    next
      if ( $sensorNode =~ /^SSN708/ || $sensorNode =~ /^SSN819/ )
      && ( $measurementName =~ /^fDOM/ || $measurementName =~ /^pCO2/ || $measurementName =~ /^Turbidity/ );
    next if $sensorNode eq "SSN819US" && $measurementName =~ /^PLS_/;

    # Jan, 2020 Ignore Hecate air temp and rh measurements, due to broken sensor
    next if $sensorNode eq "Hecate" && $measurementName =~ /^AirTemp/;
    next if $sensorNode eq "Hecate" && $measurementName =~ /^RH/;

    my $measurementsPerHour = 1;
    my $samplingTimeIndex;
    my $samplingTime;
    if ( $dataTable eq $oneHourTable ) {
        $samplingTimeIndex = \%hourlySamplingTimeIndex;
        $samplingTime      = \@hourlySamplingTime;
    }
    else {
        $samplingTimeIndex   = \%fiveMinSamplingTimeIndex;
        $samplingTime        = \@fiveMinSamplingTime;
        $measurementsPerHour = 12;
    }

    my $lastTimestamp = $allTables{$sensorNode}{$dataTable}{lastTimestamp};
    my $timeIndex     = $samplingTimeIndex->{$lastTimestamp};

    my $numCheckedMeasurements = 0;
    my $numMissingMeasurements = 0;
    while ( $timeIndex >= 0 && $numCheckedMeasurements < ( 672 * $measurementsPerHour ) ) {
        $numCheckedMeasurements++;

        last if defined $allData[$measurementIndex][$timeIndex];

        $numMissingMeasurements++;
        $timeIndex--;
    }

    next
      unless $numMissingMeasurements > ( 4 * $measurementsPerHour )
      && $numMissingMeasurements < $numCheckedMeasurements;

    my @associatedMeasurementNames;
    my $parentMeasurementIndex = $currentMeasurement->{parentMeasurementIndex};
    if ( defined $parentMeasurementIndex ) {
        $measurementName = $allMeasurements[$parentMeasurementIndex]{measurementName};

        foreach my $childMeasurementIndex ( @{ $allMeasurements[$parentMeasurementIndex]{childMeasurementIndexes} } ) {
            next unless defined $allMeasurements[$childMeasurementIndex]{isRead};

            push @associatedMeasurementNames,
              $sensorNode . "." . $allMeasurements[$childMeasurementIndex]{measurementName};
        }
    }
    push @associatedMeasurementNames, "$sensorNode.$measurementName" unless @associatedMeasurementNames;

    my $lastRecordedTimestamp = $samplingTime->[$timeIndex];

    my $measurementURL =
        "https://hecate.hakai.org/sn/p/viewsndata.pl?dataTable=$dataTable&measurements="
      . join( ",", @associatedMeasurementNames )
      . "&dateRange=last4weeks";

    my $alertType     = "Sensor offline";
    my $alertPriority = 2;                  # High priority

    if ( $sensorNode eq "MarnaLab" ) {

        # All Marna lab alerts are low priority
        $alertPriority = 0;
    }
    elsif ( index( $measurementName, "SR50" ) >= 0 || index( $measurementName, "Well_PT" ) == 0 ) {

        # All SR50 alerts and Well PT are medium priority
        $alertPriority = 1;
    }
    elsif ( $numMissingMeasurements >= ( 6 * $measurementsPerHour ) ) {

        # Critical priority if offline for 6 hours or more
        $alertPriority = 3;
    }

    $alertPriority = 0 if $sensorNode =~ /BoL$/;    # All BoL alerts are low priority

    # Skip TSN3 Soil temperature probes affected by wolves
    $alertPriority = 0 if $sensorNode eq "TSN3" && $measurementName =~ /^Soil_/;

    my $alertSignature   = "$alertType.$sensorNode.$measurementName";
    my $alertTitle       = "$sensorNode.$measurementName is missing recent measurements";
    my $alertDescription = "No <a href=\"$measurementURL\">$sensorNode.$measurementName</a> measurements recorded for ";

    my $minutesMissing = $numMissingMeasurements * 60.0;
    $minutesMissing = $numMissingMeasurements * 5.0 if $dataTable eq $fiveMinuteTable;

    if ( $minutesMissing < ( 60 * 72 ) ) {
        $alertDescription .= int( $minutesMissing / 6.0 ) / 10.0 . " hours";
    }
    else {
        $alertDescription .= int( $minutesMissing / ( 24.0 * 6.0 ) ) / 10.0 . " days";
    }

    my %alertMetadata;
    $alertMetadata{alertType}              = $alertType;
    $alertMetadata{sensorNode}             = $sensorNode;
    $alertMetadata{dataTable}              = $dataTable;
    $alertMetadata{measurementName}        = $measurementName;
    $alertMetadata{measurementURL}         = $measurementURL;
    $alertMetadata{lastRecordedTimestamp}  = $lastRecordedTimestamp;
    $alertMetadata{numMissingMeasurements} = $numMissingMeasurements;

    saveAlert( $alertSignature, $lastRecordedTimestamp, $alertPriority, $alertTitle, $alertDescription,
        \%alertMetadata );
}

# Raise alert for any meaurements that have been flagged in the last 4 days
foreach my $currentMeasurement (@allMeasurements) {
    next unless defined $currentMeasurement->{qcFlag} && $currentMeasurement->{dataTable} eq $oneHourTable;

    # Skip aggregated measurement values
    next
      if uc( $currentMeasurement->{measurementType} ) eq "AGGREGATED"
      || $currentMeasurement->{measurementName} =~ /^\d/;    # Starts with a digit, like 24hourRain

    my $sensorNode       = $currentMeasurement->{sensorNode};
    my $measurementName  = $currentMeasurement->{measurementName};
    my $measurementIndex = $currentMeasurement->{measurementIndex};

    # Skip nodes without any recent hourly data
    next
      unless exists $allTables{$sensorNode}
      && exists $allTables{$sensorNode}{$oneHourTable}
      && exists $allTables{$sensorNode}{$oneHourTable}{lastTimestamp};

    # Skip sensor nodes that are currently being ignored
    next if exists $ignoredSensorNodes{$sensorNode};

    # Skip sensor nodes that are not online
    next unless exists $onlineSensorNodes{$sensorNode};

    # Don't handle these particular alerts if the node has been offline for more then 10 days
    next if exists $offlineSensorNodes{$sensorNode} && $offlineSensorNodes{$sensorNode}{hoursBehind} > 240;

    # Skip the Standalone nodes
    next if index( $sensorNode, "SA_" ) == 0;

    $measurementName =~ s/_QC//;

    next if $measurementName =~ /^SR50A_TC_Distance/;    # Skip temperature corrected SR50 measurements
    next if $measurementName =~ /^SnowDepth/;            # Skip calculated snow depth measurements
    next if $measurementName =~ /_wtd$/;                 # Skip calculated water table depths
    next if $measurementName =~ /^cmWell/;               # Skip scaled well water levels
    next if $measurementName =~ /^cmPLS/;                # Skip scaled stream water levels
    next if $measurementName =~ /^WBJ/;                  # Skip charge controller related measurements
    next if $measurementName =~ /^Kid/;                  # Skip charge controller related measurements

    # Added based on email from Shawn in September, 2019.  These sensor have been removed
    next
      if ( $sensorNode =~ /^SSN708/ || $sensorNode =~ /^SSN819/ )
      && ( $measurementName =~ /^fDOM/ || $measurementName =~ /^pCO2/ || $measurementName =~ /^Turbidity/ );
    next if $sensorNode eq "SSN819US" && $measurementName =~ /^PLS_/;

    # Jan, 2020 Ignore Hecate air temp and rh measurements, due to broken sensor
    next if $sensorNode eq "Hecate" && $measurementName =~ /^AirTemp/;
    next if $sensorNode eq "Hecate" && $measurementName =~ /^RH/;

    my $lastTimestamp = $allTables{$sensorNode}{$oneHourTable}{lastTimestamp};
    my $lastTimeIndex = $hourlySamplingTimeIndex{$lastTimestamp};

    my $firstTimeIndex = $lastTimeIndex - 95;    # Flagged in last 4 days
    $firstTimeIndex = 0 if $firstTimeIndex < 0;

    my $firstFlaggedTimeIndex;
    my $lastFlaggedTimeIndex;
    my $numCheckedMeasurements = 0;
    my $numFlaggedMeasurements = 0;
    foreach my $timeIndex ( $firstTimeIndex .. ( $lastTimeIndex - 2 ) ) {
        $numCheckedMeasurements++;

        next unless exists $qcData[$measurementIndex]{$timeIndex};

        my $qcFlag = $qcData[$measurementIndex]{$timeIndex};

        # Ignore missing value flags, as missing values are counted independently
        # Also ignore cases where 1 or 2 of 12 five minute measurements have been flagged
        next if index( $qcFlag, "MV" ) == 0 || index( $qcFlag, " 1 of 12" ) > 0 || index( $qcFlag, " 2 of 12" ) > 0;

        $firstFlaggedTimeIndex = $timeIndex unless defined $firstFlaggedTimeIndex;
        $lastFlaggedTimeIndex = $timeIndex;
        $numFlaggedMeasurements++;
    }

    # Only alert if some, but not all, measurements have been flagged
    next unless $numFlaggedMeasurements > 0 && $numFlaggedMeasurements < $numCheckedMeasurements;

    my @associatedMeasurementNames;
    foreach my $otherMeasurement (@allMeasurements) {
        next
          unless exists $otherMeasurement->{isReferenced}
          && !exists $otherMeasurement->{qcField}
          && exists $otherMeasurement->{qcMeasurementIndex}
          && $otherMeasurement->{qcMeasurementIndex} == $measurementIndex;

        push @associatedMeasurementNames, $sensorNode . "." . $otherMeasurement->{measurementName};
    }
    push @associatedMeasurementNames, "$sensorNode.$measurementName" unless @associatedMeasurementNames;

    my $firstFlaggedTimestamp = $hourlySamplingTime[$firstFlaggedTimeIndex];
    my $lastFlaggedTimestamp  = $hourlySamplingTime[$lastFlaggedTimeIndex];

    my $measurementURL =
        "https://hecate.hakai.org/sn/p/viewsndata.pl?dataTable=$oneHourTable&measurements="
      . join( ",", @associatedMeasurementNames )
      . "&dateRange=last1week";

    my $alertType = "Suspect data";

    my $alertPriority = 1;    # Medium priority
    $alertPriority = 2 if $numFlaggedMeasurements >= 4;     # High priority if 4 or more flagged measurements
    $alertPriority = 3 if $numFlaggedMeasurements >= 10;    # Critical priority if 10 or more flagged measurements

    if ( $sensorNode eq "MarnaLab" ) {

        # All Marna lab alerts are low priority
        $alertPriority = 0;
    }
    elsif ( index( $measurementName, "SR50" ) >= 0 || index( $measurementName, "Well_PT" ) == 0 ) {

        # All SR50 alerts and Well PT are medium priority
        $alertPriority = 1;
    }

    $alertPriority = 0 if $sensorNode =~ /BoL$/;    # All BoL alerts are low priority

    # Skip TSN3 Soil temperature probes affected by wolves
    $alertPriority = 0 if $sensorNode eq "TSN3" && $measurementName =~ /^Soil_/;

    my $firstHoursAgo = $lastTimeIndex - $firstFlaggedTimeIndex;
    my $lastHoursAgo  = $lastTimeIndex - $lastFlaggedTimeIndex;

    my $alertSignature = "$alertType.$sensorNode.$measurementName";
    my $alertTitle     = "$sensorNode.$measurementName recently flagged";
    my $alertDescription =
      "$numFlaggedMeasurements <a href=\"$measurementURL\">$sensorNode.$measurementName</a> measurement";
    if ( $numFlaggedMeasurements > 1 ) {
        $alertDescription .= "s flagged between $lastHoursAgo and $firstHoursAgo hours ago";
    }
    else {
        $alertDescription .= " flagged $firstHoursAgo hours ago";
    }

    my %alertMetadata;
    $alertMetadata{alertType}              = $alertType;
    $alertMetadata{sensorNode}             = $sensorNode;
    $alertMetadata{dataTable}              = $oneHourTable;
    $alertMetadata{measurementName}        = $measurementName;
    $alertMetadata{measurementURL}         = $measurementURL;
    $alertMetadata{firstFlaggedTimestamp}  = $firstFlaggedTimestamp;
    $alertMetadata{lastFlaggedTimestamp}   = $lastFlaggedTimestamp;
    $alertMetadata{numFlaggedMeasurements} = $numFlaggedMeasurements;

    saveAlert( $alertSignature, $lastFlaggedTimestamp, $alertPriority, $alertTitle, $alertDescription,
        \%alertMetadata );
}

# Raise alert when the number of skipped scans or watchdog errors are greater then 0
foreach my $currentMeasurement (@allMeasurements) {
    next
      unless $currentMeasurement->{dataTable} eq $diagnosticsTable
      && ( $currentMeasurement->{measurementName} eq "SkippedScan"
        || $currentMeasurement->{measurementName} eq "WatchdogErrors" );

    my $sensorNode       = $currentMeasurement->{sensorNode};
    my $dataTable        = $currentMeasurement->{dataTable};
    my $measurementName  = $currentMeasurement->{measurementName};
    my $measurementIndex = $currentMeasurement->{measurementIndex};

    # Skip sensor nodes that are currently being ignored
    next if exists $ignoredSensorNodes{$sensorNode};

    # Skip sensor nodes that are not online
    next unless exists $onlineSensorNodes{$sensorNode};

    # Don't handle these particular alerts if the node has been offline for more then 10 days
    next if exists $offlineSensorNodes{$sensorNode} && $offlineSensorNodes{$sensorNode}{hoursBehind} > 240;

    my $lastTimestamp = $allTables{$sensorNode}{$dataTable}{lastTimestamp};
    my $lastTimeIndex = $hourlySamplingTimeIndex{$lastTimestamp};
    my $timeIndex     = $lastTimeIndex;

    my $mostRecentValue;
    my $moreRecentValue;
    my $numIncreases = 0;
    while ( $timeIndex >= 0 ) {
        my $currentValue = $allData[$measurementIndex][ $timeIndex-- ];
        next unless defined $currentValue;

        last if $currentValue == 0;

        $mostRecentValue = $currentValue unless defined $mostRecentValue;

        $numIncreases++ if defined $moreRecentValue && $moreRecentValue > $currentValue;

        $moreRecentValue = $currentValue;
    }
    next unless defined $mostRecentValue;

    my $lastRecordedTimestamp = $hourlySamplingTime[ $timeIndex + 1 ];

    my $measurementURL =
"https://hecate.hakai.org/sn/p/viewsndata.pl?dataTable=$dataTable&measurements=$sensorNode.$measurementName&dateRange=last4weeks";

    my $alertType = "Diagnostics warning";
    $alertType = "skipped scan"   if $measurementName eq "SkippedScan";
    $alertType = "watchdog error" if $measurementName eq "WatchdogErrors";
    $alertType .= "s" if $mostRecentValue > 1;

    my $alertPriority = 2;    # High priority
    $alertPriority = 3 if $numIncreases > 0;    # Critical priority if increased more then once

    # Don't notify about these alerts for FSN1
    $alertPriority = 0 if $sensorNode eq "FSN1";

    my $alertSignature   = "$alertType.$sensorNode.$measurementName";
    my $alertTitle       = "$mostRecentValue $alertType at $sensorNode";
    my $alertDescription = "$mostRecentValue $alertType at <a href=\"$measurementURL\">$sensorNode</a>";

    if ( $numIncreases > 0 ) {
        $alertTitle       .= " (increased $numIncreases times)";
        $alertDescription .= " (increased $numIncreases times)";
    }

    my %alertMetadata;
    $alertMetadata{alertType}       = $alertType;
    $alertMetadata{sensorNode}      = $sensorNode;
    $alertMetadata{dataTable}       = $oneHourTable;
    $alertMetadata{measurementName} = $measurementName;
    $alertMetadata{measurementURL}  = $measurementURL;
    $alertMetadata{numIncreases}    = $numIncreases;

    saveAlert( $alertSignature, $lastRecordedTimestamp, $alertPriority, $alertTitle, $alertDescription,
        \%alertMetadata );
}

# Raise alert when battery voltage levels have reached an alarm or critical state

my @batteryThresholds = (

    # 12V node defaults: alertThreshold => 12.2, criticalThreshold => 12
    # 24V node defaults: alertThreshold => 23,   criticalThreshold => 20

    #{ measurementName => "Buxton.BattVolt",           alertThreshold => 12.2, criticalThreshold => 12 },
    { measurementName => "BuxtonEast.BattVolt", alertThreshold => 12.3,  criticalThreshold => 12 },
    { measurementName => "RefStn.BattVolt",     alertThreshold => 12.15, criticalThreshold => 11.9 },

    #{ measurementName => "Ethel.BattVolt",            alertThreshold => 12.2, criticalThreshold => 12 },
    #{ measurementName => "Hecate.BattVolt",           alertThreshold => 12.2, criticalThreshold => 12 },
    { measurementName => "Koeye.BattVolt", alertThreshold => 24.2, criticalThreshold => 23.8 },

    #{ measurementName => "Lookout.BattVolt",          alertThreshold => 12.2, criticalThreshold => 12 },
    { measurementName => "Portable.BattVolt",       alertThreshold => 12.3, criticalThreshold => 12 },
    { measurementName => "PruthDock.BattVolt",      alertThreshold => 12.7, criticalThreshold => 12.5 },
    { measurementName => "PruthDock.WestBeachVolt", alertThreshold => 12.3, criticalThreshold => 12 },
    { measurementName => "Quadra.BattVolt",         alertThreshold => 11.9, criticalThreshold => 11.7 },

    #{ measurementName => "QuadraFTS.BattVolt",      alertThreshold => 12.7, criticalThreshold => 12.5 },
    #{ measurementName => "QuadraFTS2.BattVolt",     alertThreshold => 12.7, criticalThreshold => 12.5 },
    { measurementName => "QuadraLimpet.BattVolt", alertThreshold => 11.9, criticalThreshold => 11.7 },

    #{ measurementName => "SSN1015US.BattVolt",        alertThreshold => 23,   criticalThreshold => 20 },
    { measurementName => "SSN1015DS.BattVolt", alertThreshold => 13, criticalThreshold => 12.7 },

    { measurementName => "SSN626PWR.BattVolt", alertThreshold => 24.4, criticalThreshold => 24 },

    #{ measurementName => "SSN626US.BattVolt",         alertThreshold => 23,   criticalThreshold => 20 },
    #{ measurementName => "SSN626AS.BattVolt",         alertThreshold => 23,   criticalThreshold => 20 },
    #{ measurementName => "SSN626DS.BattVolt",         alertThreshold => 23,   criticalThreshold => 20 },

    { measurementName => "SSN693PWR.BattVolt", alertThreshold => 24.4, criticalThreshold => 24 },

    #{ measurementName => "SSN693US.BattVolt",         alertThreshold => 23,   criticalThreshold => 20 },
    #{ measurementName => "SSN693DS.BattVolt",         alertThreshold => 23,   criticalThreshold => 20 },
    #{ measurementName => "SSN703US.BattVolt",         alertThreshold => 12.2, criticalThreshold => 12 },
    { measurementName => "SSN703DS.BattVolt", alertThreshold => 11.8, criticalThreshold => 11.6 },

    #{ measurementName => "SSN708.BattVolt",           alertThreshold => 12.2, criticalThreshold => 12 },
    { measurementName => "SSN708DS.BattVolt", alertThreshold => 11.8, criticalThreshold => 11.6 },

    { measurementName => "SSN819.BattVolt", alertThreshold => 11.8, criticalThreshold => 11.6 },

    #{ measurementName => "SSN819PWR.BattVolt",        alertThreshold => 12.2, criticalThreshold => 12 },
    { measurementName => "SSN819US.BattVolt", alertThreshold => 11.7, criticalThreshold => 11.5 },

    #{ measurementName => "SSN844US.BattVolt",         alertThreshold => 23,   criticalThreshold => 20 },
    #{ measurementName => "SSN844DS.BattVolt",         alertThreshold => 12.2, criticalThreshold => 12 },
    { measurementName => "SSN844PWR.BattVolt",   alertThreshold => 13,   criticalThreshold => 12.7 },
    { measurementName => "TSN1.BattVolt",        alertThreshold => 12.3, criticalThreshold => 12 },
    { measurementName => "TSN2.BattVolt",        alertThreshold => 12.3, criticalThreshold => 12 },
    { measurementName => "TSN3.BattVolt",        alertThreshold => 12.3, criticalThreshold => 12.1 },
    { measurementName => "WSN626.BattVolt",      alertThreshold => 12.3, criticalThreshold => 12 },
    { measurementName => "WSN693_703.BattVolt",  alertThreshold => 12.3, criticalThreshold => 12 },
    { measurementName => "WSN703.BattVolt",      alertThreshold => 12.3, criticalThreshold => 12 },
    { measurementName => "WSN703_708.BattVolt",  alertThreshold => 12.3, criticalThreshold => 12 },
    { measurementName => "WSN819_1015.BattVolt", alertThreshold => 12.3, criticalThreshold => 12 },
    { measurementName => "WSN844.BattVolt",      alertThreshold => 12.3, criticalThreshold => 12 },
    { measurementName => "DumpVolt\$",           alertThreshold => 12.3, criticalThreshold => 12 }
);

foreach my $currentMeasurement (@allMeasurements) {
    next unless defined $currentMeasurement->{isRead} && $currentMeasurement->{dataTable} eq $oneHourTable;

    next
      unless $currentMeasurement->{measurementName} eq "BattVolt_Avg"
      || $currentMeasurement->{measurementName} eq "DumpVolt_Avg"
      || $currentMeasurement->{measurementName} eq "WestBeachVolt_Avg";

    my $sensorNode       = $currentMeasurement->{sensorNode};
    my $measurementName  = $currentMeasurement->{measurementName};
    my $measurementIndex = $currentMeasurement->{measurementIndex};

    # Skip sensor nodes that are currently being ignored
    next if exists $ignoredSensorNodes{$sensorNode};

    # Skip sensor nodes that are not online
    next unless exists $onlineSensorNodes{$sensorNode};

    # Don't handle these particular alerts if the node has been offline for more then 10 days
    next if exists $offlineSensorNodes{$sensorNode} && $offlineSensorNodes{$sensorNode}{hoursBehind} > 240;

    my $lastTimestamp  = $allTables{$sensorNode}{$oneHourTable}{lastTimestamp};
    my $lastTimeIndex  = $hourlySamplingTimeIndex{$lastTimestamp};
    my $firstTimeIndex = $lastTimeIndex - 95;
    $firstTimeIndex = 0 if $firstTimeIndex < 0;

    # For each 3 hour period over 4 days, find the lowest battery voltage that
    # was sustained for the three hour period
    my $minVoltageTimeIndex;
    my $minThreeHourVoltage;
    my $mostRecentVoltage;
    my @hourlyVoltages;
    foreach my $timeIndex ( $firstTimeIndex .. $lastTimeIndex ) {
        next unless defined $allData[$measurementIndex][$timeIndex];

        $mostRecentVoltage = $allData[$measurementIndex][$timeIndex];

        push @hourlyVoltages, $mostRecentVoltage;

        shift @hourlyVoltages if scalar(@hourlyVoltages) > 3;
        next if scalar(@hourlyVoltages) < 3;

        next if defined $minThreeHourVoltage && $minThreeHourVoltage <= max(@hourlyVoltages);

        $minVoltageTimeIndex = $timeIndex;
        $minThreeHourVoltage = max(@hourlyVoltages);
    }

    next unless defined $minThreeHourVoltage;

    my $alertVoltageThreshold = 12.2;
    my $criticalVoltageLevel  = 12;
    if ( $minThreeHourVoltage > 14.9 ) {
        $alertVoltageThreshold = 23;
        $criticalVoltageLevel  = 20;
    }

    my $voltageMeasurementName = "$sensorNode.$measurementName";
    $voltageMeasurementName =~ s/_Avg//;

    foreach $batteryThreshold (@batteryThresholds) {
        next unless $voltageMeasurementName =~ /$batteryThreshold->{measurementName}/;

        $log->debug("$voltageMeasurementName matches $batteryThreshold->{measurementName}, using custom thresholds");

        $alertVoltageThreshold = $batteryThreshold->{alertThreshold}
          if exists $batteryThreshold->{alertThreshold};

        $criticalVoltageLevel = $batteryThreshold->{criticalThreshold}
          if exists $batteryThreshold->{criticalThreshold};

        last;
    }

    $log->debug( "$voltageMeasurementName\tminThreeHourVoltage=$minThreeHourVoltage"
          . ",\tmostRecentVoltage=$mostRecentVoltage"
          . ",\talertThreshold=$alertVoltageThreshold"
          . ",\tcriticalThreshold=$criticalVoltageLevel" );

    next if $minThreeHourVoltage > $alertVoltageThreshold;
    my $alertType     = "Low voltage";
    my $alertPriority = 2;               # High priority
    if ( $minThreeHourVoltage <= $criticalVoltageLevel ) {
        $alertPriority = 3;              # Critical priority
    }

    my @associatedMeasurementNames;
    my $parentMeasurementIndex = $currentMeasurement->{parentMeasurementIndex};
    if ( defined $parentMeasurementIndex ) {
        $measurementName = $allMeasurements[$parentMeasurementIndex]{measurementName};

        foreach my $childMeasurementIndex ( @{ $allMeasurements[$parentMeasurementIndex]{childMeasurementIndexes} } ) {
            next unless defined $allMeasurements[$childMeasurementIndex]{isRead};

            push @associatedMeasurementNames,
              $sensorNode . "." . $allMeasurements[$childMeasurementIndex]{measurementName};
        }
    }
    push @associatedMeasurementNames, "$sensorNode.$measurementName" unless @associatedMeasurementNames;

    my $measurementURL =
        "https://hecate.hakai.org/sn/p/viewsndata.pl?dataTable=$oneHourTable&measurements="
      . join( ",", @associatedMeasurementNames )
      . "&dateRange=last1week";

    my $hoursAgo = $lastTimeIndex - $minVoltageTimeIndex;

    my $alertSignature = "$alertType.$sensorNode.$measurementName";
    my $alertTitle     = "$sensorNode.$measurementName low battery voltage";
    my $alertDescription =
"<a href=\"$measurementURL\">$sensorNode.$measurementName</a> had a battery voltage of $minThreeHourVoltage $hoursAgo hours ago";

    if ( $minThreeHourVoltage <= $criticalVoltageLevel ) {
        $alertTitle       = "[critical] $alertTitle (<=$criticalVoltageLevel)";
        $alertDescription = "[critical] $alertDescription (<=$criticalVoltageLevel)";
    }
    else {
        $alertTitle       = "$alertTitle (<=$alertVoltageThreshold)";
        $alertDescription = "$alertDescription (<=$alertVoltageThreshold)";
    }

    my %alertMetadata;
    $alertMetadata{alertType}             = $alertType;
    $alertMetadata{sensorNode}            = $sensorNode;
    $alertMetadata{dataTable}             = $oneHourTable;
    $alertMetadata{measurementName}       = $measurementName;
    $alertMetadata{measurementURL}        = $measurementURL;
    $alertMetadata{alertVoltageThreshold} = $alertVoltageThreshold;
    $alertMetadata{criticalVoltageLevel}  = $criticalVoltageLevel;
    $alertMetadata{hoursAgo}              = $hoursAgo;
    $alertMetadata{minThreeHourVoltage}   = $minThreeHourVoltage;

    saveAlert( $alertSignature, $hourlySamplingTime[$minVoltageTimeIndex],
        $alertPriority, $alertTitle, $alertDescription, \%alertMetadata );
}

# Raise alert when precipitation gauge level exceeds 1.4 m

foreach my $currentMeasurement (@allMeasurements) {
    next unless defined $currentMeasurement->{isRead} && $currentMeasurement->{dataTable} eq $oneHourTable;

    next unless $currentMeasurement->{measurementName} eq "PrecipGaugeLvl_Avg";

    my $sensorNode       = $currentMeasurement->{sensorNode};
    my $measurementName  = $currentMeasurement->{measurementName};
    my $measurementIndex = $currentMeasurement->{measurementIndex};

    # Skip sensor nodes that are currently being ignored
    next if exists $ignoredSensorNodes{$sensorNode};

    # Skip sensor nodes that are not online
    next unless exists $onlineSensorNodes{$sensorNode};

    # Don't handle these particular alerts if the node has been offline for more then 10 days
    next if exists $offlineSensorNodes{$sensorNode} && $offlineSensorNodes{$sensorNode}{hoursBehind} > 240;

    my $lastTimestamp = $allTables{$sensorNode}{$oneHourTable}{lastTimestamp};
    my $lastTimeIndex = $hourlySamplingTimeIndex{$lastTimestamp};

    # Search for the most recent value
    my $currentTimeIndex = $lastTimeIndex;
    while ( $currentTimeIndex > 0 && !defined $allData[$measurementIndex][$currentTimeIndex] ) {
        $currentTimeIndex--;
    }

    my $precipAlertThreshold = 1.4;

    next unless $currentTimeIndex > 0 && $allData[$measurementIndex][$currentTimeIndex] >= $precipAlertThreshold;

    my $measurementURL =
"https://hecate.hakai.org/sn/p/viewsndata.pl?dataTable=$oneHourTable&measurements=$sensorNode.$measurementName&dateRange=last4weeks";

    my $lastPrecipGaugeLvl = $allData[$measurementIndex][$currentTimeIndex];

    my $alertType      = "Precip gauge level";
    my $alertPriority  = 2;                                                         # High priority
    my $alertSignature = "$alertType.$sensorNode.$measurementName";
    my $alertTitle     = "$sensorNode precip gauge level is $lastPrecipGaugeLvl";
    my $alertDescription =
"<a href=\"$measurementURL\">$sensorNode.$measurementName</a> precip gauge level is $lastPrecipGaugeLvl (>$precipAlertThreshold)";

    my %alertMetadata;
    $alertMetadata{alertType}       = $alertType;
    $alertMetadata{sensorNode}      = $sensorNode;
    $alertMetadata{dataTable}       = $oneHourTable;
    $alertMetadata{measurementName} = $measurementName;
    $alertMetadata{measurementURL}  = $measurementURL;
    $alertMetadata{precipGaugeLvl}  = $lastPrecipGaugeLvl;

    saveAlert( $alertSignature, $hourlySamplingTime[$currentTimeIndex],
        $alertPriority, $alertTitle, $alertDescription, \%alertMetadata );
}

foreach my $alertSignature ( sort keys %allAlerts ) {

    next if exists $allAlerts{$alertSignature}{inserted} || exists $allAlerts{$alertSignature}{updated};

    closeAlert( $allAlerts{$alertSignature}{alertID}, $alertSignature );
}

my %newAlerts;
my %updatedAlerts;
my %closedAlerts;
my $sendEmailNotification;
foreach my $alertSignature ( sort keys %allAlerts ) {

    # Skip over alerts that have been dismissed
    next if exists $allAlerts{$alertSignature}{dismissedDT};

    my $alertPriority = $allAlerts{$alertSignature}{alertPriority};
    my $startedDT     = $allAlerts{$alertSignature}{startedDT};

    # Don't trigger an email notification unless at least one high priority alert has been opened or closed
    $sendEmailNotification = 1
      if $alertPriority > 1
      && ( exists $allAlerts{$alertSignature}{inserted} || exists $allAlerts{$alertSignature}{closed} );

    my $alertKey = "$alertPriority.$startedDT.$alertSignature";

    $newAlerts{$alertKey}{alertSignature}     = $alertSignature if exists $allAlerts{$alertSignature}{inserted};
    $updatedAlerts{$alertKey}{alertSignature} = $alertSignature if exists $allAlerts{$alertSignature}{updated};
    $closedAlerts{$alertKey}{alertSignature}  = $alertSignature if exists $allAlerts{$alertSignature}{closed};
}

################################################################################
# Send an email notification that the file has been processed
my $baseURL = "https://hecate.hakai.org";
if ( $sendEmailNotification && $notificationUser && $notificationPassword && !$quiet ) {
    my $emailSubject;
    my @alertDescriptions;
    foreach my $alertKey ( sort { $b cmp $a } keys %newAlerts ) {
        my $alertSignature = $newAlerts{$alertKey}{alertSignature};

        $emailSubject = $allAlerts{$alertSignature}{alertTitle} unless defined $emailSubject;

        push @alertDescriptions, "<b>New:</b> " . $allAlerts{$alertSignature}{alertDescription};
    }

    foreach my $alertKey ( sort { $b cmp $a } keys %closedAlerts ) {
        my $alertSignature = $closedAlerts{$alertKey}{alertSignature};

        $emailSubject = "CLOSED: " . $allAlerts{$alertSignature}{alertTitle} unless defined $emailSubject;

        push @alertDescriptions, "<b>Closed:</b> " . $allAlerts{$alertSignature}{alertDescription};
    }

    foreach my $alertKey ( sort { $b cmp $a } keys %updatedAlerts ) {
        my $alertSignature = $updatedAlerts{$alertKey}{alertSignature};

        # Skip over alerts that have been dismissed
        next if exists $allAlerts{$alertSignature}{dismissedDT};

        push @alertDescriptions, $allAlerts{$alertSignature}{alertDescription};
    }

    $emailSubject .= " (+)" if scalar( keys %newAlerts ) + scalar( keys %closedAlerts ) > 1;

    my $emailBody .= "<p>Hey dudes and dudettes, <b>pay attention!</b></p>\n";

    $emailBody .=
        "<p><b>New!</b> View the <a href=\"https://hecate.hakai.org/sn/p/viewsnmap.pl\">SN status map</a>"
      . " (<a href=\"https://hecate.hakai.org/sn/p/viewsnmap.pl?showAlerts=1\">only alerts</a>)</p>\n";

    my $numOpenAlerts   = 0;
    my $maxListedAlerts = 15;

    $emailBody .= "<ul>\n";

    foreach my $alertDescription (@alertDescriptions) {
        next if $numOpenAlerts++ > $maxListedAlerts;

        $emailBody .= "<li>$alertDescription</li>\n";
    }

    $emailBody .= "<li><b>+" . ( $numOpenAlerts - $maxListedAlerts ) . " additional open alerts</b></li>\n"
      if $numOpenAlerts > $maxListedAlerts;

    $emailBody .= "</ul>\n";

    $emailBody .=
"<p>See the <a href=\"https://hecate.hakai.org/sn/p/viewsnstatus.pl\">SN status page</a> for more information</p>\n";

    my $sender = Email::Send->new(
        {
            mailer      => 'Gmail',
            mailer_args => [
                username => "$notificationUser",
                password => "$notificationPassword",
            ]
        }
    );

    my @notifiedUsers;
    push @notifiedUsers, "sn.alerts\@hakai.org";

    foreach my $notifiedAddress (@notifiedUsers) {
        my $email = Email::MIME->create(
            header => [
                From    => "$notificationUser",
                To      => "$notifiedAddress",
                Subject => "$emailSubject",
            ],
            body => $emailBody
        );

        $email->content_type_set('text/html');

        eval { $sender->send($email) };
        $log->error("ERROR: failed to send email notification to $notifiedAddress, code was $@")
          if $@;

        $log->info("Sent email notification to $notifiedAddress");
    }
}

#
# End of process-sn-data.pl
#
