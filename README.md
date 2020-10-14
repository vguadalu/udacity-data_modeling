# Sparkify Database

## Purpose
The purpose of this database is to allow easy analysis of the songs being played by Sparkify users. 

### Schema
The Sparikfy analysts are mainly interested in the information pertaining to the songs being played which is not an overly complicated request from the data. Since the star schema allows for simpler queries when extracting data and quicker reads it was the right choice for the Sparkify database. The data could easily be connected with a fact table since it did not need to have any parent-child relations between any of the dimension tables. Various information pertaining to the songs being played can be selected easily due to the star schema being used. 

The fact table is called songplays and contains the songplay id, timestamp, user id, artist id, level, song_id, location, session id and user agent. The songplay id is an integer and the primary key for this table. The start time is a NOT NULL timestamp and the foreign key to the times table. The user id, artist id, song id and location are all varchar(100). The artist id, user id and song id are the foreign keys to the artists, users and songs table, respectively, and can not be null since they are foreign keys.  The level (char(4)) can not be null since the membership level must be part of the user's account info. The session (integer) and is not null because the a session id should be created when a session is started. The location can be null since its not required to be provided. The user agent (text) and can not be null.

There are four dimension tables, users, artists, times, and songs. The users table contains the user id, first name, last name, gender and level. 
The user id is an integer and the primary key. The first name and last name are varchar(50) and can not be null since they are required when creating a Sparkify account. The gender is a char(1) and can be null. The level is a char(4) and can not be null since its part of the Sparkify account wether they are paying or using it for free.
The artists table contains the artist id, name, location, latitude and longitude. The artist id is a varchar(100) and the primary key. The name and location are both varchar(50) since its not expected to have arist names and location with more than 50 characters. The name can not be null because for there to be an artist id there must be a name for that artist. The location, latitude (decimal) and longitude(decimal) can be null since its not required information to be provided. 
The songs table contains the song id, title, artist id, year and duration. The song id(varchar(100)) is the primary key and thus can not be null and must be unique. The title(varchar(100)) is the name of the song and can not be null since the title should be provided when creating a new song id. The artist_id (varchar(100)), year(integer) and duration(decimal) can be null and is not a required field when inserting data into the songs table. 
The times table contains the start time, hour, day, week, month, year and weekday. The start time is a timestamp and is the primary key of the times table. The hour, day, week, month, year, weekday are all integers and can not be null since they can be extracted from the start time and allow for the data to be more versatile and easily accessible. 
 
### ETL
The extraction of the data from both the log and song files was done by simply reading in the JSON log and song files into a pandas DataFrame. 

To transform and insert the data for the song files, the values of the desired columns in the DataFrames were extracted and simply inserted into a dimension table using SQL INSERT INTO queries. The songs DataFrame was created by selecting the song id, song title, artist id, song length and year from the original DataFrame. The the songs table was created containing those column's data and making the song_id column the primary key. Similarly the artist table was created. A DataFrame containing the artist id, artist name, artist location, latitude and longitude was generated and then inserted into the artists table, making the artist_id the primary key. 

The log files required a bit more work to transform the data so that it could be inserted into the tables. From the log files, the times, users and songplays tables were generated. Before the tables could begin to be generated only the rows that had "NextSong" as the value for the column 'page' were desired. To accomplish this a DataFrame containing all rows that matched that condition was generated and used to create the smaller more specific DataFrames.

The times table's values were created by grabbing the 'ts' column, transforming the value into a timestamp, extracting the hour, day, week, month, year, and weekday and storing them in a dictionary with the key as the column name and the value the previously mention extracted values. 

The users table contains the user id, first name, last name, gender and membership level. The primary key is the user id. A DataFrame containing those columns was created and then the duplicates found in the user id column were dropped to avoid errors when inserting into the table since the user id should be unique. 

The songplays table required the most work to transform and insert the data since its the fact table and contains data that is distributed among the songs and artist tables. To create the songplays DataFrame, each log files's was iterated through and JOIN query was performed on each row to get the song id and artist id from the respective songs and artists tables. If no result was found the song id and artist id were set to "None". This is important since "None" values are not considered NULL and the song id and artist id can not be NULL due to them being primary keys. The row's data is then appended to a list before moving on to the next row. Once all the rows in the file are iterated through, a DataFrame is created using the appended list and the desired columns. 

The log files contain multiple lines of data so rather than inserting one line a time the SQL COPY function was used to insert the data in bulk for the users, times and songplays tables. The following are the steps taken to insert the data into the tables once the transformed data was ready:
1. Placed the DataFrame into a CSV file
2. Created a temporary table with the same columns as the final table without primary key constraints
3. Used COPY to insert CSV file into the temporary table
4. Used INSERT INTO with SELECT to insert the desired data into the final table
5. DROP the temporary table

To determine the data types of the data values a print statement with type() was used after extracting the data from the JSON files. The constraints for each column in the tables were determined by the information provied and by using the data_quality notebook.

### Contents of Package
The following are the files contained in this package:
- data/: Contains the JSON song and log files from which the data is extracted
- create_tables.py: Python script which connects to the default database, generates the Sparkify database and creates empty tables in the Sparkify database using the SQL queries located at sql_queries.py.
- data_quality.ipynb: Python notebook used to test the quality of the data located in the Sparkify database. It contains examples of queries for song play analysis.
- etl.ipynb: Python notebook used to brainstorm and test ETL logic on one file before using it on all the JSON files. 
- etl.py: Python script which extracts, transforms and inserts the data from all the JSON files located in data/ into the tables of the Sparkify databse.
- README.md: ReadMe currently reading, which contains the purpose and implementation of the Sparkify database, content of the package, and how to generate the database.
- sql_queries.py: Python script which contains the DROP, CREATE, INSER and COPY SQL queries to be used by create_tables.py and etl.py.
- test.ipynb: Python notebook which contains SELECT queries for each table to be used for verification.

### How to create the Sparkify Database
To generate the Sparkify database the following step should be followed:
1. Generate the empty database and tables:
    python create_tables.py
2. Insert data into the tables:
    python etl.py

### Example Queries and Result
Example queries and results can be found in the data_quality notebook.