import sqlalchemy
import os
import sqlite3
from dotenv import load_dotenv

load_dotenv()

db_location = os.environ["DATABASE_LOCATION"]

class Database():
    engine = sqlalchemy.create_engine(db_location)

    def connect(self):
        
        try:
            conn = sqlite3.connect('my_played_tracks.sqlite')
            cursor = conn.cursor()
            print("Connection to db sucesfully")
        except Exception as e:
            print("Connection to db failed, reason:", e)
        
        return cursor, conn
 
    
    def close_connection(self, conn):
        conn.close()
        print("Close database successfully") 


    def create_tracks_table(self, cursor):

        sql_query = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200) PRIMARY KEY,
            timestamp VARCHAR(200),
            UNIQUE(played_at)
        )
        """
        cursor.execute(sql_query)
        print("Opened/Created table successfully")