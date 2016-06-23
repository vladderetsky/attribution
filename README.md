Attribution
===========



This is a simple attribution application that produces a report that can then be fed into a database. It processes input datasets of events and impressions to compute attributed events and output some simple statistics on them.

The statistics that the application computes are:

- count of attributed events for each advertiser, grouped by event type.
- count of unique users that have generated attributed events for each advertiser, grouped by event type.


##Data Processing

Attribution application does the following:

- Loads event and impression data sets
- Applies de-duplication functionality to the event data set
- Computes attributed events
- Outputs some simple statistics

###De-duplication

Events are sometimes registered multiple times in the events dataset when they actually should be counted only once. For instance, a user might click on an ad twice by mistake. Application de-duplicates these events: for a given user / advertiser / event type combination, and removes events that happen more than once every minute. Period of time is a minute ( 60 seconds ), but it can be changed if desired.

###Attribution processing

Attributed event is an event that happened chronologically after an impression and is considered to be the result of that impression. The advertiser and the user of both the impression and the event have to be the same for the event to be attributable. Example: a user buying an object after seeing an ad from an advertiser.

Application computes attributed events and outputs some simple statistics on them.

###Outputs

Application provides two CSV report files:

- count_of_events.csv, contains the count of events for each advertiser, grouped by event type
- count_of_users.csv, contain the count of unique users for each advertiser, grouped by event type


##Schemas

###Events


    Num	Name            Type            Description
    === ==============  =============== ======================================
    1	timestamp	    integer	        Unix timestamp when the event happened.
    2	event_id	    string (UUIDv4)	Unique ID for the event.
    3	advertiser_id	integer	        The advertiser ID that the user interacted with.
    4	user_id	        string (UUIDv4)	An anonymous user ID that generated the event.
    5	event_type	    string	        The type of event. Potential values: click, visit, purchase


###Impressions


    Num Name            Type            Description
    === ==============  =============== ======================================
    1	timestamp	    integer	        Unix timestamp when the impression was served.
    2	advertiser_id	integer	        The advertiser ID that owns the ad that was displayed.
    3	creative_id	    integer	        The creative (or ad) ID that was displayed.
    4	user_id	        string (UUIDv4)	An anonymous user ID this ad was displayed to.


###count_of_events.csv


    Num Name            Type        Description
    === ==============  ========    ==========================================
    1	advertiser_id	integer	    The advertiser ID
    2	event_type	    string	    The type of event. Potential values: click, visit, purchase
    3	count	        integer	    The count of events for this advertiser ID and event type.


###count_of_users.csv


    Num Name            Type        Description
    === ==============  ========    ==========================================
    1	advertiser_id	integer	    The advertiser ID
    2	event_type	    string	    The type of event. Potential values: click, visit, purchase
    3	count	        integer	    The count of unique users for this advertiser ID and event type.


##Installation

Application is written in Python and requires Python 2.7. It uses the Spark computing framework (v1.6.1) that requires JVM(Java 7+).

###Git clone

To run Attribution application, you can clone this repository.


###Spark 1.6.1 distribution

Application uses the Spark computing framework in local mode. It can be modified for using it on a cluster. For the cluster mode, Apache Spark requires a cluster manager and a distributed storage system. For cluster management, Spark supports standalone (native Spark cluster), Hadoop YARN, or Apache Mesos
The following steps are for installing Spark and using it in the local mode.


    $ cd <directory where you want to install Spark>
    $ wget http://apache.mirrors.ionfish.org/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz
    $ tar zxvf spark-1.6.1-bin-hadoop2.6.tgz
    $ rm spark-1.6.1-bin-hadoop2.6.tgz
    $ ln -s spark-1.6.1-bin-hadoop2.6  spark
    $ export SPARK_HOME=$APPS/spark
    $ export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.9-src.zip
    $ export PATH=$PATH:$SPARK_HOME/python:$SPARK_HOME/bin:$SPARK_HOME/python/lib/py4j-0.9-src.zip


##Configure Application

To run application, you have to provide 2 input CSV files with events and impressions and specify their locations in config/attribution.ini configuration file, under :


    [input]
    event_file=/tmp/events.csv
    impression_file=/tmp/impressions.csv


Or use "events" and "imps" arguments: --events=/tmp/events.csv --imps=/tmp/impressions.csv

Also, provide there the output parameters were you desire to store two CSV report files:


    [output]
    count_of_events_path=/tmp/count_of_events.csv
    count_of_users_path=/tmp/count_of_users.csv


##Run Application


    $ cd <directory where application was cloned or copied>
    $ spark-submit attribution/attributed_events.py


## Output Results

Spark computing framework saves data to the targeted path, not to the targeted file. From a sample of the output configuration above, you can find the final data files as:


    $ ls -l /tmp/count_of_events.csv
    total 16
    -rw-r--r--  1 user  group   0 Jun 22 15:49 _SUCCESS
    -rw-r--r--  1 user  group  47 Jun 22 15:49 part-00000
    -rw-r--r--  1 user  group  61 Jun 22 15:49 part-00001


and


    $ ls -l /tmp/count_of_users.csv
    total 16
    -rw-r--r--  1 user  group   0 Jun 22 15:49 _SUCCESS
    -rw-r--r--  1 user  group  47 Jun 22 15:49 part-00000
    -rw-r--r--  1 user  group  61 Jun 22 15:49 part-00001


##Testing Application

Install nosetest package or any other python test engine before running the UnitTest suite for this application.


    $ pip install -r requirements-test.txt


To run the Unit Test set, just execute the following:


    $ nosetests tests


or if pytest package was installed then use the following:


    $ py.test tests


