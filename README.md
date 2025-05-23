# Overview

The SIS poller works by comparing the time stamps in the SIS files of interest to the time stamps in a local database.  When the [last modified](https://files.anss-sis.scsn.org/production/FDSNStationXML1.1/UU/) time exceeds the time that is in the local database then an email is sent to interested parties. 

In practice, this workflow is a little more complicated at UUSS.

   1.  Launch a container that uses the software in this repository to
       *  Scrape SIS
       *  Update a Postgres database
       *  Provided an update is detected send a PUT request to an AWS gateway.
   2.  At AWS
       *  Pass the PUT request to a Lambda function
       *  The Lambda validates the request and propagates to SNS
       *  Interested parties that are subscribed to the appropriate SNS topics then receive emails

## Getting Started

To get this up and running requires two steps.  The first is creating a database.  Then, once the database is created, populating the database with an initial batch of last update times.

### Create the Database

To enable the SIS polling script to detect relevant updates it must compare the last modified time with a locally stored time.  To allow this to happen, we must create a database.  Originally, the updating was done with respect to a sqlite3 database but as our network is shifting job deployment strategies we now use a Postgres database.

#### Postgres

As user postgres you can run the following script to create a sis\_poller database

    psql -f createDatabase.sql

where createDatabase.sql assumes you have defined read\_write and read\_only roles defined and would look like

    --- createDatabase.sql
    --- Run as postgres user to create the sis_poller database.
    DROP DATABASE IF EXISTS sis_poller;
    CREATE DATABASE sis_poller WITH TABLESPACE pg_default;
    \connect sis_poller;
    DROP SCHEMA IF EXISTS production;
    CREATE SCHEMA production;

    DROP TABLE IF EXISTS production.xml_update;

    --- Table with XML file names (e.g., UU_FORK and last update)
    CREATE TABLE production.xml_update
    (
        xml_file TEXT PRIMARY KEY NOT NULL,
        last_modified TIMESTAMP DEFAULT timezone('UTC'::text, CURRENT_TIMESTAMP)
    );

    --- Allow read-write user to connect/do updates
    ---CREATE ROLE read_write;
    GRANT CONNECT ON DATABASE sis_poller TO read_write;
    GRANT USAGE, CREATE ON SCHEMA production TO read_write;
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA production TO read_write;

    --- Allow read-only user to connect/read
    ---CREATE ROLE read_only;
    GRANT CONNECT ON DATABASE sis_poller TO read_only;
    GRANT USAGE ON SCHEMA production TO read_only;
    GRANT SELECT ON ALL TABLES IN SCHEMA production TO read_only;

    --- Create users
    DROP USER IF EXISTS sis_poller_updater;
    CREATE USER sis_poller_updater WITH PASSWORD 'A6evP0uiEU9mmp';
    GRANT read_write TO sis_poller_updater;
    DROP USER IF EXISTS sis_poller_viewer;
    CREATE USER sis_poller_viewer WITH PASSWORD 'sis_poller_browser';
    GRANT read_only TO sis_poller_viewer;

Should you not have these read\_write and read\_only roles, then you will have to uncomment the appropriate lines above.

#### SQLite3

Note, the following is minimally tested.  As any the SIS poller user you can run the following script to create a local sqlite3 database

    #!/usr/bin/bash
    # Purpose: Creates the database containing the SIS file and the last  update.

    TARGET_DIRECTORY=./
    TABLE_NAME=sisXMLUpdates.sqlite3

    if [ ! -d ${TARGET_DIRECTORY} ]; then
       mkdir -p ${TARGET_DIRECTORY}
    fi

    sqlite3 ${TARGET_DIRECTORY}/${TABLE_NAME} << EOL
    CREATE TABLE IF NOT EXISTS xml_update (
        xml_file TEXT PRIMARY KEY NOT NULL,
        last_modified DATE DEFAULT (datetime('now','UTC'))
    );
    EOL

### Using Postgres

If using a Postgres database, you must set the following environment variables for the user running poller.py

1. SIS_POLLER_READ_WRITE_USER (the read-write user's name)
2. SIS_POLLER_READ_WRITE_PASSWORD (the read-write user's password)
3. SIS_POLLER_DATABASE (the database name, e.g., sis_poller)
4. SIS_POLLER_HOST (by default this is localhost)
5. SIS_POLLER_SCHEMA (the schema, e.g., production)
6. SIS_POLLER_PORT (by default this is 5432)

### Installation

After the databases have been decided upon, you can install the software

    /path/to/pip install -e .
    
### Initialize Data in Database

The initial set of timestamps is input into the database with something like

    python3 poller.py -i

This action will not result in an email being sent.

## General Usage

Now that the database is created and populated, typical usage involves something akin to 

    python3 poller.py

on regular intervals (e.g., daily).

