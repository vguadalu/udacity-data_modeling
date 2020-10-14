import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath:str):
    """ Creates songs and artists tables
    
    Reads in a JSON song file and creates song and artist
    DataFrames. Once the DataFrame is created it inserts the
    DataFrame into a relational table. 
    
    Parameters
    ----------
    cur: cursor
         PostgrSQL cursor
    filepath: str 
              Path to the JSON file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id','year', 
                         'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 
                           'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def insert_using_copy(cur, df, log_file:str, create:str,
                      copy:str, insert:str, drop:str):
    """ Insert data from one table to another.
    
    Create csv file from DataFrame. Create empty temporary table.
    Copy data from the csv file into the temporary table.
    Insert desired columns from temporary table into the official
    table being created. Drop the temporary table once data is inserted.
    
    Parameters
    ----------
    cur: cursor
         PostgrSQL cursor
    df: DataFrame
        Pandas DataFrame containg desired data
    log_file: str 
              Path to the csv file where the DataFrame will be stored
    create: str
            SQL command for creating temporary table
    copy: str
          SQL command for inserting data in csv file to temporary 
          table using COPY
    insert: str
            SQL command for inserting columns from temporary table 
            to official table
    drop: str
          SQL command for dropping temporary table
    """

    # generate csv file using dataframe 
    df.to_csv(log_file, index=False)
    
    # create temporary table
    cur.execute(create)
        
    # copy data from csv file to temporary table
    cur.execute(copy)
    
    # insert temporary table into official table
    cur.execute(insert)
    
    # drop temporary table
    cur.execute(drop)
    
def process_log_file(cur, filepath:str):
    """ Creates times, users and songplays tables.
    
    Reads in a JSON log file and generates relational tables 
    for users, times and songplays. 
    A table is created by selecting the desired columns and 
    adding them to a data frame. The data frame is then placed 
    in a csv file and copied to a temporary table. The temporary 
    table's desired columns are then inserted into the
    official table.
    
    Parameters
    ----------
    cur: cursor
         PostgreSQL cursor
    filepath: str
              Path to the JSON file
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df.page.isin(['NextSong'])]
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month,
                t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    time_dict = {column_labels[i]:time_data[i] for i in range(0,len(time_data))}
    time_df = pd.DataFrame.from_dict(time_dict)
    insert_using_copy(cur, time_df, 'data/time_log_file.csv', temp_time_table_create, 
                      time_table_copy, time_table_insert, temp_time_table_drop)
    
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates(subset='userId')
    insert_using_copy(cur, user_df, 'data/user_log_file.csv', temp_user_table_create, 
                      user_table_copy, user_table_insert, temp_user_table_dorp)
    
    # insert songplay records
    songplays = []
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data =(index, pd.to_datetime(row.ts, unit='ms'), row.userId, 
                             row.level, songid, artistid, row.sessionId, row.location, 
                             row.userAgent)
        songplays.append(songplay_data)
    songplay_df = pd.DataFrame(songplays, columns=['songplay_id', 'start_time', 'user_id', 
                                                       'level', 'song_id', 'artist_id', 'session_id',
                                                       'location', 'user_agent'])
    insert_using_copy(cur, songplay_df, 'data/songplay_log_file.csv', temp_songplay_table_create, 
                      songplay_table_copy, songplay_table_insert, temp_songplay_table_drop)

def process_data(cur, conn, filepath:str, func):
    """ Process data into tables
    
    Grab data files and process the data 
    into tables based on predefined functions.

    Parameters
    ----------
    cur: cursor
         PostgreSQL cursor
    conn: connection
          PostgreSQL connection
    filepath: str
              Path to data files
    func: function
          Function to be used for processing the data in 
          the data files
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()