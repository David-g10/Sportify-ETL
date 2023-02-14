import sqlalchemy
from sqlalchemy.orm import sessionmaker
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

class Database():
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)

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