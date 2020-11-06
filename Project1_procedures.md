##CLickstream Example

Step 1: Create an external table to hold data

    CREATE EXTERNAL TABLE clickstream (
    REFERRER STRING,
    REFERRED STRING,
    TYPE STRING,
    COUNT INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t';

Step 2: Load the data into the table

    LOAD DATA INPATH '/user/victor/clickstream' INTO TABLE clickstream;

Step 3: Create a table with the MapReduce result
    
    CREATE TABLE MRclickstream AS
    SELECT referred, sum(count) AS views
    from clickstream
    group by referred
    order by views desc ;

Step 4: Query the table

    SELECT * FROM mrclickstream limit #;
    0: jdbc:hive2://> SELECT * fROM mrclickstream limit 10;
    +-----------------------------+----------------------+
    |   mrclickstream.referred    | mrclickstream.views  |
    +-----------------------------+----------------------+
    | Main_Page                   | 160410827            |
    | Hyphen-minus                | 14763118             |
    | Ruth_Bader_Ginsburg         | 7570549              |
    | Amy_Coney_Barrett           | 5908040              |
    | Shooting_of_Breonna_Taylor  | 3965314              |
    | Tenet_(film)                | 3838165              |
    | Dennis_Nilsen               | 3548056              |
    | Deaths_in_2020              | 3383779              |
    | Mulan_(2020_film)           | 3205636              |
    | Bible                       | 3178927              |
    +-----------------------------+----------------------+

-----------------------------------------------------------------------------------

##Question 1 
Step 1: Create Table that will hold pageviews information.
      
    CREATE EXTERNAL TABLE Oct20VIEWS
    (DOMAIN_CODE STRING, PAGE_TITLE STRING, PAGE_VIEWS INT, RESPONSE_SIZE INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ' ';

Step 2: Load the data for the 24 hours of OCT-20-2020

    LOAD DATA INPATH '/user/victor/oct20/*' INTO TABLE Oct20VIEWS;

Step 3: Crate a mapreduced output file to query later.

    INSERT OVERWRITE DIRECTORY '/user/hive/q1output'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ' '
    SELECT PAGE_TITLE, SUM(PAGE_VIEWS) AS VIEWS
    FROM oct20views
    WHERE UPPER(DOMAIN_CODE)='EN'
    or UPPER(DOMAIN_CODE)='EN.M'
    GROUP BY PAGE_TITLE
    SORT BY VIEWS DESC
    LIMIT 100;

Step 4: Create table to hold those values

    CREATE EXTERNAL TABLE mroct20views
    (page_title string, views int)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ' ';

Step 5: Load the data to the table

    LOAD DATA INPATH '/user/victor/000000_0' OVERWRITE INTO TABLE mroct20views;

PART 5 => Query the table: 

    SELECT * FROM mroct20views LIMIT 10;
    +----------------------------+---------------------+
    |  mroct20views.page_title   | mroct20views.views  |
    +----------------------------+---------------------+
    | Main_Page                  | 5961008             |
    | Special:Search             | 1476831             |
    | -                          | 544714              |
    | Jeffrey_Toobin             | 321459              |
    | C._Rajagopalachari         | 210558              |
    | The_Haunting_of_Bly_Manor  | 185139              |
    | Robert_Redford             | 178779              |
    | Jeff_Bridges               | 159163              |
    | Bible                      | 151484              |
    | Chicago_Seven              | 149966              |
    +----------------------------+---------------------+


-------------------------------------------------------------------------

##Question 2 
_DISCLAIMER: DATA IS FROM SEPT 1-SEPT 15 AND CLICKSTREAM/2_

Step 1: Create table that will hold all the pageviews statistics for the month of 

    CREATE EXTERNAL TABLE septviews
    (DOMAIN_CODE STRING, PAGE_TITLE STRING, PAGE_VIEWS INT, RESPONSE_SIZE INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ' ';

Step2: Load the data for the month of September

    LOAD DATA INPATH '/user/victor/septviews/*' INTO TABLE septviews;

Step 3: Create Table with only information we need (referrer, sum(page_views))

    CREATE TABLE MRSEPTVIEWS AS
    SELECT PAGE_TITLE, SUM(PAGE_VIEWS)
    AS VIEWS
    FROM septviews
    WHERE UPPER(DOMAIN_CODE)='EN'
    or UPPER(DOMAIN_CODE)='EN.M'
    GROUP BY PAGE_TITLE
    ORDER BY VIEWS DESC
    LIMIT 100;

Step 4: Create table that will select pages that have the most clicks of internal links

    CREATE TABLE q2_clickstream AS
    SELECT referrer, type, (sum(count)*0.5) AS clicks
    from clickstream
    where type="link" AND NOT (referrer ="other-internal" OR
    referrer ="other-search" OR referrer ="other-external" 
    OR referrer ="other-empty" OR referrer ="other-other") 
    group by referrer, type
    order by clicks desc ;


    select * from q2_clickstream limit 10;
    +----------------------------+----------------------+------------------------+
    |  q2_clickstream.referrer   | q2_clickstream.type  | q2_clickstream.clicks  |
    +----------------------------+----------------------+------------------------+
    | Ruth_Bader_Ginsburg        | link                 | 1244613.5              |
    | Main_Page                  | link                 | 1189643.5              |
    | Cobra_Kai                  | link                 | 1120875.5              |
    | The_Boys_(2019_TV_series)  | link                 | 1003175.5              |
    | Mulan_(2020_film)          | link                 | 874759.5               |
    | Ratched_(TV_series)        | link                 | 834238.5               |
    | Deaths_in_2020             | link                 | 797857.5               |
    | Amy_Coney_Barrett          | link                 | 706672.5               |
    | Tenet_(film)               | link                 | 693043.0               |
    | Enola_Holmes_(film)        | link                 | 678155.5               |
    +----------------------------+----------------------+------------------------+

     select * from mrseptviews limit 10;
    +----------------------------+--------------------+
    |   mrseptviews.page_title   | mrseptviews.views  |
    +----------------------------+--------------------+
    | Main_Page                  | 79993215           |
    | Special:Search             | 20976735           |
    | -                          | 8571791            |
    | Tenet_(film)               | 2646264            |
    | Mulan_(2020_film)          | 2436954            |
    | Chadwick_Boseman           | 2012391            |
    | The_Boys_(2019_TV_series)  | 1834596            |
    | Cobra_Kai                  | 1750951            |
    | Bible                      | 1716404            |
    | September_11_attacks       | 1704461            |
    +----------------------------+--------------------+


Step 5: Create a table to join the two tables q2_clickstream + mrseptviews

    CREATE TABLE Q2_JOIN AS
    SELECT MRSEPTVIEWS.PAGE_TITLE, MRSEPTVIEWS.VIEWS AS PAGE_VIEWS, 
    q2_clickstream.CLICKS FROM q2_clickstream
    JOIN MRSEPTVIEWS ON (MRSEPTVIEWS.PAGE_TITLE = q2_clickstream.referrer)

    Select * from q2_join limit 10;
    +---------------------------------------------+---------------------+-----------------+
    |             q2_join.page_title              | q2_join.page_views  | q2_join.clicks  |
    +---------------------------------------------+---------------------+-----------------+
    | Main_Page                                   | 79993215            | 1189643.5       |
    | Cobra_Kai                                   | 1750951             | 1120875.5       |
    | The_Boys_(2019_TV_series)                   | 1834596             | 1003175.5       |
    | Mulan_(2020_film)                           | 2436954             | 874759.5        |
    | Deaths_in_2020                              | 1660052             | 797857.5        |
    | Tenet_(film)                                | 2646264             | 693043.0        |
    | Dune_(2020_film)                            | 1022867             | 600729.5        |
    | Joe_Biden                                   | 839251              | 575393.0        |
    | Donald_Trump                                | 724685              | 560069.0        |
    | COVID-19_pandemic_by_country_and_territory  | 619687              | 546660.5        |
    +---------------------------------------------+---------------------+-----------------+

Step 6: Create the final table with the stats and query

    CREATE TABLE Q2_STATS AS
    SELECT * , CAST(CLICKS/PAGE_VIEWS * 100 AS DECIMAL(5,3)) 
    AS PERCENT_OF_PEOPLE
    FROM Q2_JOIN
    ORDER BY PERCENT_OF_PEOPLE DESC
    LIMIT 10;

    SELECT * FROM Q2_STATS;
    +---------------------------------------------+----------------------+------------------+-----------------------------+
    |             q2_stats.page_title             | q2_stats.page_views  | q2_stats.clicks  | q2_stats.percent_of_people  |
    +---------------------------------------------+----------------------+------------------+-----------------------------+
    | COVID-19_pandemic_by_country_and_territory  | 619687               | 546660.5         | 88.216                      |
    | 2016_United_States_presidential_election    | 452920               | 384062.0         | 84.797                      |
    | Elizabeth_II                                | 552634               | 461072.5         | 83.432                      |
    | The_Karate_Kid                              | 543566               | 430283.5         | 79.159                      |
    | Donald_Trump                                | 724685               | 560069.0         | 77.284                      |
    | 2020_United_States_presidential_election    | 487204               | 374602.5         | 76.888                      |
    | Joe_Biden                                   | 839251               | 575393.0         | 68.560                      |
    | The_Babysitter:_Killer_Queen                | 489395               | 331931.5         | 67.825                      |
    | Christopher_Nolan                           | 452441               | 306367.0         | 67.714                      |
    | Cobra_Kai                                   | 1750951              | 1120875.5        | 64.015                      |
    +---------------------------------------------+----------------------+------------------+-----------------------------+

-------------------------------------------------------------------------
##Question 3

Step 1: Create a tables that adds total clicks and where those clicks are distributed

    CREATE TABLE q3_cs AS
    SELECT referrer, referred, sum(count) AS clicks
    from clickstream
    where type="link" AND NOT (referrer ="other-internal" OR
    referrer ="other-search" OR referrer ="other-external" 
    OR referrer ="other-empty" OR referrer ="other-other") 
    group by referrer, referred
    order by clicks desc ;

    SELECT * FROM q3_cs limit 10;
    +----------------------+---------------------------------------------+---------------+
    |    q3_cs.referrer    |               q3_cs.referred                | q3_cs.clicks  |
    +----------------------+---------------------------------------------+---------------+
    | Main_Page            | Deaths_in_2020                              | 1057958       |
    | Ruth_Bader_Ginsburg  | Martin_D._Ginsburg                          | 445464        |
    | COVID-19_pandemic    | COVID-19_pandemic_by_country_and_territory  | 435001        |
    | Sarah_Paulson        | Holland_Taylor                              | 372520        |
    | Ruth_Bader_Ginsburg  | Jane_C._Ginsburg                            | 354482        |
    | Cobra_Kai            | List_of_The_Karate_Kid_characters           | 319232        |
    | Sarah_Paulson        | Cherry_Jones                                | 299600        |
    | Mulan_(2020_film)    | Liu_Yifei                                   | 288437        |
    | Enola_Holmes_(film)  | Millie_Bobby_Brown                          | 260990        |
    | Lucifer_(TV_series)  | List_of_Lucifer_episodes                    | 258955        |
    +----------------------+---------------------------------------------+---------------+
    
Step 2: Create a table that adds up all of the clicks for a page    
    
    CREATE TABLE Q3_SUM AS
    SELECT REFERRER,  SUM(CLICKS) 
    AS total_clicks
    FROM Q3_cs
    group by referreR
    ORDER BY total_clicks DESC; 
    
    select * from q3_sum WHERE referrer = "Hotel_California";
    
    +-------------------+----------------------+
    |  q3_sum.referrer  | q3_sum.total_clicks  |
    +-------------------+----------------------+
    | Hotel_California  | 13779                |
    +-------------------+----------------------+


Step 3: Query tables from q3_cs and q3_sum that will only fetch 'Hotel_California' and click counts in descending order

    select * from q3_cs WHERE referrer="Hotel_California" limit 5;
    +-------------------+----------------------------------+---------------+
    |  q3_cs.referrer   |          q3_cs.referred          | q3_cs.clicks  |
    +-------------------+----------------------------------+---------------+
    | Hotel_California  | Hotel_California_(Eagles_album)  | 2222          |
    | Hotel_California  | Don_Henley                       | 1537          |
    | Hotel_California  | Don_Felder                       | 1519          |
    | Hotel_California  | Eagles_(band)                    | 1335          |
    | Hotel_California  | Glenn_Frey                       | 1021          |
    +-------------------+----------------------------------+---------------+
    
    select * from q3_sum WHERE referrer = "Hotel_California";
    +-------------------+----------------------+
    |  q3_sum.referrer  | q3_sum.total_clicks  |
    +-------------------+----------------------+
    | Hotel_California  | 13779                |
    +-------------------+----------------------+
    

Step 4: Repeat step 3 on the most clicked link.

    select * from q3_cs WHERE referrer="Hotel_California_(Eagles_album)" limit 5;
    +----------------------------------+----------------------------------+---------------+
    |          q3_cs.referrer          |          q3_cs.referred          | q3_cs.clicks  |
    +----------------------------------+----------------------------------+---------------+
    | Hotel_California_(Eagles_album)  | The_Long_Run_(album)             | 2127          |
    | Hotel_California_(Eagles_album)  | Hotel_California                 | 2010          |
    | Hotel_California_(Eagles_album)  | Their_Greatest_Hits_(1971–1975)  | 897           |
    | Hotel_California_(Eagles_album)  | Eagles_(band)                    | 801           |
    | Hotel_California_(Eagles_album)  | The_Beverly_Hills_Hotel          | 490           |
    +----------------------------------+----------------------------------+---------------+
    
    select * from q3_sum WHERE referrer = "Hotel_California_(Eagles_album)";
    +----------------------------------+----------------------+
    |         q3_sum.referrer          | q3_sum.total_clicks  |
    +----------------------------------+----------------------+
    | Hotel_California_(Eagles_album)  | 11487                |
    +----------------------------------+----------------------+
    
___________________________________________________________________________________________________

    select * from q3_cs WHERE referrer="The_Long_Run_(album)" limit 5;
    +-----------------------+----------------------------------+---------------+
    |    q3_cs.referrer     |          q3_cs.referred          | q3_cs.clicks  |
    +-----------------------+----------------------------------+---------------+
    | The_Long_Run_(album)  | Eagles_Live                      | 1322          |
    | The_Long_Run_(album)  | Hotel_California_(Eagles_album)  | 654           |
    | The_Long_Run_(album)  | I_Can't_Tell_You_Why             | 470           |
    | The_Long_Run_(album)  | Heartache_Tonight                | 327           |
    | The_Long_Run_(album)  | The_Long_Run_(song)              | 319           |
    +-----------------------+----------------------------------+---------------+
    
    select * from q3_sum WHERE referrer = "The_Long_Run_(album)";
    +-----------------------+----------------------+
    |    q3_sum.referrer    | q3_sum.total_clicks  |
    +-----------------------+----------------------+
    | The_Long_Run_(album)  | 5393                 |
    +-----------------------+----------------------+
    
________________________________________________________________________________________________________

    select * from q3_cs WHERE referrer="Eagles_Live" limit 5;
    +-----------------+-------------------------------+---------------+
    | q3_cs.referrer  |        q3_cs.referred         | q3_cs.clicks  |
    +-----------------+-------------------------------+---------------+
    | Eagles_Live     | Eagles_Greatest_Hits,_Vol._2  | 1136          |
    | Eagles_Live     | The_Long_Run_(album)          | 223           |
    | Eagles_Live     | Seven_Bridges_Road            | 127           |
    | Eagles_Live     | Eagles_(band)                 | 95            |
    | Eagles_Live     | Life's_Been_Good              | 47            |
    +-----------------+-------------------------------+---------------+
    
    select * from q3_sum WHERE referrer = "Eagles_Live";

    +------------------+----------------------+
    | q3_sum.referrer  | q3_sum.total_clicks  |
    +------------------+----------------------+
    | Eagles_Live      | 2094                 |
    +------------------+----------------------+
___________________________________________________________________________________________________________

    PATH: FROM -(CLIKS_FROM/TOTAL_CLICKS)-> 
        
    Hotel_California --(2222/13779(16.1%))--> Hotel_California_(Eagles_album) --(2127/11487(18.5%))--> 
    The_Long_Run_(album) --(1322/5393(24.5%))--> Eagles_Live --(1136/2094(54.2%))--> 
    Eagles_Greatest_Hits,_Vol._2 ...

------------------------------------------------------------------------------------------------------
##Question 4

Step 1: figure out from pageview for last week of October (25-31){00..23} which are daylight hours in 
       
       US: UTC (12:00-23:00) + next day UTC (00:00-03:00) Active hours 7:00 AM (EST)- 10:00 PM (PST)
       UK: UTC (07:00-22:00) Active Hours 7:00 AM (UTC) - 10:00 PM (PST)
      AUS: previous day UTC (21:00-23:00) + UTC (00:00-14:00) Active hours 7:00 AM (AEST) - 10:00 PM (AWST)
      
Step 2: => fetch those files and load them to hdfs

    US --> wget https://dumps.wikimedia.org/other/pageviews/2020/2020-10/pageviews-202010{25..31}-{12..23}0000.gz
           wget https://dumps.wikimedia.org/other/pageviews/2020/2020-10/pageviews-202010{26..31}-{00..03}0000.gz
           wget https://dumps.wikimedia.org/other/pageviews/2020/2020-11/pageviews-20201101-{00..03}0000.gz
    UK --> wget https://dumps.wikimedia.org/other/pageviews/2020/2020-10/pageviews-202010{25..31}-{07..22}0000.gz
    AUS -> wget https://dumps.wikimedia.org/other/pageviews/2020/2020-10/pageviews-202010{24..30}-{21..23}0000.gz
           wget https://dumps.wikimedia.org/other/pageviews/2020/2020-10/pageviews-202010{25..31}-{00..14}0000.gz

Step 3: create a table per country in hive

Australia Table:

    CREATE EXTERNAL TABLE aus_views
    (DOMAIN_CODE STRING, PAGE_TITLE STRING, PAGE_VIEWS INT, RESPONSE_SIZE INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ' ';

US Table:

    CREATE EXTERNAL TABLE us_views
    (DOMAIN_CODE STRING, PAGE_TITLE STRING, PAGE_VIEWS INT, RESPONSE_SIZE INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ' ';

UK Table:

    CREATE EXTERNAL TABLE uk_views
    (DOMAIN_CODE STRING, PAGE_TITLE STRING, PAGE_VIEWS INT, RESPONSE_SIZE INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ' ';

Step 4: Load data to each respective table

    LOAD DATA INPATH '/user/victor/aus_time/*' INTO TABLE aus_views;

    LOAD DATA INPATH '/user/victor/us_time/*' INTO TABLE us_views;

    LOAD DATA INPATH '/user/victor/uk_time/*' INTO TABLE uk_views;

Step 5: Create a MapReduced version of those tables to show the page_views in desc order

    CREATE TABLE q4_mr_aus AS
    SELECT page_title, sum(page_views) AS total_views
    from aus_views
    where UPPER(DOMAIN_CODE)='EN'
    or UPPER(DOMAIN_CODE)='EN.M'
    group by page_title
    order by total_views desc;

    CREATE TABLE q4_mr_us AS
    SELECT page_title, sum(page_views) AS total_views
    from us_views
    where UPPER(DOMAIN_CODE)='EN'
    or UPPER(DOMAIN_CODE)='EN.M'
    group by page_title
    order by total_views desc;

    CREATE TABLE q4_mr_uk AS
    SELECT page_title, sum(page_views) AS total_views
    from uk_views
    where UPPER(DOMAIN_CODE)='EN'
    or UPPER(DOMAIN_CODE)='EN.M'
    group by page_title
    order by total_views desc;

Step 6: Query every table 

    SELECT * FROM q4_mr_aus limit 10;
    +----------------------------------+------------------------+
    |       q4_mr_aus.page_title       | q4_mr_aus.total_views  |
    +----------------------------------+------------------------+
    | Main_Page                        | 27739753               |
    | Special:Search                   | 6453274                |
    | -                                | 2676027                |
    | The_Queen's_Gambit_(miniseries)  | 1440840                |
    | Khabib_Nurmagomedov              | 1374645                |
    | Sean_Connery                     | 1140062                |
    | Sacha_Baron_Cohen                | 1132006                |
    | Amy_Coney_Barrett                | 1050170                |
    | Bible                            | 973632                 |
    | Borat_Subsequent_Moviefilm       | 788252                 |
    +----------------------------------+------------------------+

    SELECT * FROM q4_mr_us limit 10;
    +-------------------------------------------+-----------------------+
    |            q4_mr_us.page_title            | q4_mr_us.total_views  |
    +-------------------------------------------+-----------------------+
    | Main_Page                                 | 29154069              |
    | Special:Search                            | 6823205               |
    | Sean_Connery                              | 3535342               |
    | -                                         | 2706336               |
    | The_Queen's_Gambit_(miniseries)           | 1432272               |
    | Amy_Coney_Barrett                         | 1006245               |
    | Sacha_Baron_Cohen                         | 973235                |
    | 2016_United_States_presidential_election  | 849507                |
    | Bible                                     | 842760                |
    | Khabib_Nurmagomedov                       | 783601                |
    +-------------------------------------------+-----------------------+

    SELECT * FROM q4_mr_uk limit 10;
    +----------------------------------+-----------------------+
    |       q4_mr_uk.page_title        | q4_mr_uk.total_views  |
    +----------------------------------+-----------------------+
    | Main_Page                        | 28354092              |
    | Special:Search                   | 6841447               |
    | Sean_Connery                     | 2995724               |
    | -                                | 2791645               |
    | The_Queen's_Gambit_(miniseries)  | 1083819               |
    | Khabib_Nurmagomedov              | 903770                |
    | Mirzapur_(TV_series)             | 822885                |
    | Bible                            | 800144                |
    | Sacha_Baron_Cohen                | 797960                |
    | Halloween                        | 716188                |
    +----------------------------------+-----------------------+

-----------------------------------------------------------------------------------------------------
##Question 5 

Step 1: Download the revisions data file for enwiki and upload it to HDFS
       
       wget aafefaeff

Step 2: Create a table to hold all of those values.

    CREATE TABLE revisions (WIKI_DB STRING, 
    EVENT_ENTITY STRING, ...)
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\t'; 

Step 3: Load the data in: 

    LOAD DATA INPATH '/user/victor/history/*' INTO TABLE revisions; 
    
Step 4: MapReduce the table to query later

    CREATE TABLE q5_mr_revision AS
    SELECT Round(AVG(Distinct revision_seconds_to_identity_revert),2) AS avg_revision_rev
    FROM revisions
    where revision_seconds_to_identity_revert > 0
    group by revision_seconds_to_identity_revert
    order by avg_revision_rev desc
    limit 10; 

    +----------------------------------+
    | q5_mr_revision.avg_revision_rev  |
    +----------------------------------+
    | 2578563.0                        |
    | 2552081.0                        |
    | 2550155.0                        |
    | 2550112.0                        |
    | 2548289.0                        |
    | 2532764.0                        |
    | 2532697.0                        |
    | 2519221.0                        |
    | 2516039.0                        |
    | 2516019.0                        |
    +----------------------------------+
     seconds -> day
     86,400  ->  1
    
    SELECT (avg_revision_rev/86400) AS avg_days_to_rev
    FROM q5_mr_revision;
    +---------------------+
    |   avg_days_to_rev   |
    +---------------------+
    | 29.844479166666666  |
    | 29.537974537037037  |
    | 29.51568287037037   |
    | 29.515185185185185  |
    | 29.494085648148147  |
    | 29.314398148148147  |
    | 29.313622685185184  |
    | 29.157650462962962  |
    | 29.120821759259258  |
    | 29.12059027777778   |
    +---------------------+
    
    Takes them about 29.844479 days to revert, so for one day = 0.994815
    
Step 5: Figure out the avg views of pages per day     
    
    select AVG(page_views) AS avg_pv,
    sum(page_views) AS total_page_views
    from septviews
    where UPPER(domain_code) = 'EN' OR UPPER(domain_code) = 'EN.M'
    
    
    +---------------------+-------------------+
    |       avg_pv        | total_page_views  |
    +---------------------+-------------------+
    | 4.7958202886055155  | 3559255190        |
    +---------------------+-------------------+

Step 6. Combine the answers

    4.7958202886055155*2  ~ (30 days) =  9.591640577 views per day 
    
    9.591640577 * 0.9948 ~ 9.5 Average page views before a revision is made.
    
   
-------------------------------------------------------------------------------------------------------
####Question 6 

What wikipedia articles are the most viewed from the presidential election. Basically a trace.

PART 1 => Use q3_cs dataset to query for anything involving the election.

    select * from q3_sum where referrer Like '%election' limit 10;
    +-------------------------------------------+----------------------+
    |              q3_sum.referrer              | q3_sum.total_clicks  |
    +-------------------------------------------+----------------------+
    | 2016_United_States_presidential_election  | 768124               |
    | 2020_United_States_presidential_election  | 749205               |
    | 2012_United_States_presidential_election  | 341194               |
    | 2008_United_States_presidential_election  | 317237               |
    | 2000_United_States_presidential_election  | 249939               |
    | 2004_United_States_presidential_election  | 242779               |
    | 1992_United_States_presidential_election  | 207533               |
    | 1996_United_States_presidential_election  | 193678               |
    | 1984_United_States_presidential_election  | 177773               |
    | 1988_United_States_presidential_election  | 165147               |
    +-------------------------------------------+----------------------+
    
PART 2 => Figure out the trace of '2016_United_States_presidential_election'
          and '2020_United_States_presidential_election'
          
        select * from q3_cs WHERE referrer="2016_United_States_presidential_election" limit 10;
        +-------------------------------------------+----------------------------------------------------+---------------+
        |              q3_cs.referrer               |                   q3_cs.referred                   | q3_cs.clicks  |
        +-------------------------------------------+----------------------------------------------------+---------------+
        | 2016_United_States_presidential_election  | 2020_United_States_presidential_election           | 102018        |
        | 2016_United_States_presidential_election  | 2012_United_States_presidential_election           | 99269         |
        | 2016_United_States_presidential_election  | 2016_United_States_presidential_election_in_Pennsylvania | 20585   |
        | 2016_United_States_presidential_election  | 2016_United_States_presidential_election_in_Texas  | 20079         |
        | 2016_United_States_presidential_election  | 2016_United_States_presidential_election_in_Florida | 18204        |
        | 2016_United_States_presidential_election  | 2016_United_States_presidential_election_in_Michigan | 17721       |
        | 2016_United_States_presidential_election  | 2016_United_States_presidential_election_in_Wisconsin | 17690      |
        | 2016_United_States_presidential_election  | 2016_United_States_presidential_election_in_California | 15983     |
        | 2016_United_States_presidential_election  | 2016_United_States_presidential_election_in_Minnesota | 14833      |
        | 2016_United_States_presidential_election  | Tim_Kaine                                          | 14676         |
        +-------------------------------------------+----------------------------------------------------+---------------+
        
        select * from q3_sum WHERE referrer = "2016_United_States_presidential_election";
        +-------------------------------------------+----------------------+
        |              q3_sum.referrer              | q3_sum.total_clicks  |
        +-------------------------------------------+----------------------+
        | 2016_United_States_presidential_election  | 768124               |
        +-------------------------------------------+----------------------+
        
        select * from q3_cs WHERE referrer="2020_United_States_presidential_election" limit 10;
        +-------------------------------------------+----------------------------------------------------+---------------+
        |              q3_cs.referrer               |                   q3_cs.referred                   | q3_cs.clicks  |
        +-------------------------------------------+----------------------------------------------------+---------------+
        | 2020_United_States_presidential_election  | Nationwide_opinion_polling_for_the_2020_United ... | 85024         |
        | 2020_United_States_presidential_election  | 2016_United_States_presidential_election           | 62027         |
        | 2020_United_States_presidential_election  | Joe_Biden                                          | 42502         |
        | 2020_United_States_presidential_election  | Kamala_Harris                                      | 42305         |
        | 2020_United_States_presidential_election  | Statewide_opinion_polling_for_the_2020_United_ ... | 24241         |
        | 2020_United_States_presidential_election  | Donald_Trump                                       | 24000         |
        | 2020_United_States_presidential_election  | Third_party_and_independent_candidates_for_the ... | 18313         |
        | 2020_United_States_presidential_election  | Mike_Pence                                         | 16903         |
        | 2020_United_States_presidential_election  | Jo_Jorgensen                                       | 16894         |
        | 2020_United_States_presidential_election  | 2020_United_States_presidential_debates            | 13305         |
        +-------------------------------------------+----------------------------------------------------+---------------+
        
        select * from q3_sum WHERE referrer = "2020_United_States_presidential_election";
        +-------------------------------------------+----------------------+
        |              q3_sum.referrer              | q3_sum.total_clicks  |
        +-------------------------------------------+----------------------+
        | 2020_United_States_presidential_election  | 749205               |
        +-------------------------------------------+----------------------+

        select * from q3_cs WHERE referrer="Nationwide_opinion_polling_for_the_2020_United_States_presidential_election" limit 10;
        +----------------------------------------------------+----------------------------------------------------+---------------+
        |                   q3_cs.referrer                   |                   q3_cs.referred                   | q3_cs.clicks  |
        +----------------------------------------------------+----------------------------------------------------+---------------+
        | Nationwide_opinion_polling_for_the_2020_United ... | Statewide_opinion_polling_for_the_2020_United_ ... | 14822         |
        | Nationwide_opinion_polling_for_the_2020_United ... | 2020_United_States_presidential_election           | 6814          |
        | Nationwide_opinion_polling_for_the_2020_United ... | 2016_United_States_presidential_election           | 2727          |
        | Nationwide_opinion_polling_for_the_2020_United ... | Joe_Biden                                          | 1366          |
        | Nationwide_opinion_polling_for_the_2020_United ... | Jo_Jorgensen                                       | 1169          |
        | Nationwide_opinion_polling_for_the_2020_United ... | Opinion_poll                                       | 1104          |
        | Nationwide_opinion_polling_for_the_2020_United ... | Howie_Hawkins                                      | 817           |
        | Nationwide_opinion_polling_for_the_2020_United ... | Donald_Trump                                       | 711           |
        | Nationwide_opinion_polling_for_the_2020_United ... | Opinion_polling_on_the_Donald_Trump_administration | 706           |
        | Nationwide_opinion_polling_for_the_2020_United ... | 2020_United_States_Senate_elections                | 531           |
        +----------------------------------------------------+----------------------------------------------------+---------------+

        select * from q3_sum WHERE referrer = "Nationwide_opinion_polling_for_the_2020_United_States_presidential_election";
        +----------------------------------------------------+----------------------+
        |                  q3_sum.referrer                   | q3_sum.total_clicks  |
        +----------------------------------------------------+----------------------+
        | Nationwide_opinion_polling_for_the_2020_United  ...| 37970                |
        +----------------------------------------------------+----------------------+
        
        select * from q3_cs WHERE referrer="Joe_Biden" limit 10;
        +-----------------+-------------------------+---------------+
        | q3_cs.referrer  |     q3_cs.referred      | q3_cs.clicks  |
        +-----------------+-------------------------+---------------+
        | Joe_Biden       | Beau_Biden              | 201049        |
        | Joe_Biden       | Hunter_Biden            | 168227        |
        | Joe_Biden       | Neilia_Hunter           | 156164        |
        | Joe_Biden       | Jill_Biden              | 130155        |
        | Joe_Biden       | Donald_Trump            | 36775         |
        | Joe_Biden       | Teetotalism             | 26755         |
        | Joe_Biden       | Dick_Cheney             | 25758         |
        | Joe_Biden       | Mike_Pence              | 25364         |
        | Joe_Biden       | University_of_Delaware  | 23934         |
        | Joe_Biden       | Kamala_Harris           | 23474         |
        +-----------------+-------------------------+---------------+
        
        select * from q3_sum WHERE referrer = "Joe_Biden";
        +------------------+----------------------+
        | q3_sum.referrer  | q3_sum.total_clicks  |
        +------------------+----------------------+
        | Joe_Biden        | 1150786              |
        +------------------+----------------------+
        
        select * from q3_cs WHERE referrer="Donald_Trump" limit 10;
        +-----------------+---------------------------------------------------+---------------+
        | q3_cs.referrer  |                  q3_cs.referred                   | q3_cs.clicks  |
        +-----------------+---------------------------------------------------+---------------+
        | Donald_Trump    | Melania_Trump                                     | 129234        |
        | Donald_Trump    | Ivana_Trump                                       | 103322        |
        | Donald_Trump    | Marla_Maples                                      | 90414         |
        | Donald_Trump    | Family_of_Donald_Trump                            | 85296         |
        | Donald_Trump    | Fred_Trump                                        | 59578         |
        | Donald_Trump    | Donald_Trump_Jr.                                  | 49632         |
        | Donald_Trump    | Tiffany_Trump                                     | 49519         |
        | Donald_Trump    | Ivanka_Trump                                      | 48984         |
        | Donald_Trump    | Eric_Trump                                        | 35762         |
        | Donald_Trump    | Wharton_School_of_the_University_of_Pennsylvania  | 33202         |
        +-----------------+---------------------------------------------------+---------------+
        
        select * from q3_sum WHERE referrer = "Donald_Trump";
        +------------------+----------------------+
        | q3_sum.referrer  | q3_sum.total_clicks  |
        +------------------+----------------------+
        | Donald_Trump     | 1120138              |
        +------------------+----------------------+
--------------------------------------------------------------------------------------------------        

                             |---------> 2020_US_Election...               (102,018) = 13.3%
                             |       
                             |---------> 2012_US_Election...                (99,269) = 12.9%
    2016_US_Election ------->|      
    (Total Clicks: 768,124)  |---------> 2016_US_Election..._Pennsylvania   (20,585) = 2.7%
                             |
                             |---------> 2016_US_Election..._Texas          (20,079) = 2.6%
                                                                   
  
                             |---------> Nationwide_opinion...              (85,024) = 11.3%
                             |       
                             |---------> 2016_US_Election                   (62,027) = 8.3%
    2020_US_Election ------->|      
    (Total Clicks: 749,205)  |---------> Joe_Biden                          (42,502) = 5.7%
                             |
                             |---------> Donald_Trump                       (24,000) = 3.2%
                                                 
                                                 
                                                 
                             |-----------> Statewide_opinion...             (14,822) =  39.0%                 
                             |-----------> 2020_US_election...              (6,814)  =  17.9%             
    Nationwide_opinion  ---->|-----------> Joe_Biden                        (1,366)  =   3.6%
    (Total Clicks: 37,970)   |-----------> Jo_Jorgensen                     (1,169)  =   3.1%
                             |-----------> Donald_Trump                      (711)   =   1.9%
                           
                             |-----------> Beau_Biden                      (201,049) =  17.5% 
                             |-----------> Hunter_Biden                    (168,227) =  14.6%
      Joe_Biden      ------> |-----------> Neilia_Hunter                   (156,164) =  13.6%
    (Total Clicks: 1,150,786)|-----------> Jill_Biden                      (130,155) =  11.3%
                             |-----------> Donald_Trump                     (36,775) =   3.2%  
                           
                             |-----------> Melania_Trump                   (129,234) =  11.5%
                             |-----------> Ivana_Trump                     (103,322) =  9.2%
     Donald_Trump    ------> |-----------> Marla_Maples                    (90,414)  =  8.1%
    (Total Clicks: 1,120,138)|-----------> Family_of_Donald_Trump          (85,296)  =  7.6%
                             |-----------> Fred_Trump                      (59,578)  =  5.3%