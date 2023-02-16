import datetime
import requests
import pandas as pd
from database import Database
import os
from dotenv import load_dotenv
import base64

load_dotenv()

user_id = os.environ["USER_ID"]
client_id = os.environ["CLIENT_ID"]
client_secret = os.environ["CLIENT_SECRET"]

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if a Dataframe is empty.
    if df.empty:
        print("No songs recently played, Finishing the execution.")
        return False

    # Primary key check
    if not pd.Series(df["played_at"]).is_unique:
        raise Exception("Primary key check is violated")

    # Check for nulls.
    if df.isnull().values.any():
        raise Exception("Null value found.")

    # Check that all timestamps are of yestarday's date.
    elapsed_hours = datetime.datetime.now() - datetime.timedelta(days=2)
    elapsed_hours = elapsed_hours.replace(hour=0 , minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") < elapsed_hours:
            print(datetime.datetime.strptime(timestamp, "%Y-%m-%d"))
            print(elapsed_hours)
            raise Exception("At least one of the returned songs doesn't come within the last 48 hours.")

    return True


if __name__ == "__main__":
    
    # Authorize

    auth_url = "https://accounts.spotify.com/authorize?"
    headers = {
        "response_type": "code",
        "client_id" : client_id,
        "scope": "user-read-recently-played",
        "redirect_uri" : 'http://localhost:8888/callback'
    }
    import webbrowser
    import urllib.parse

    webbrowser.open(auth_url + urllib.parse.urlencode(headers))

    # Login to the api.
    # url = 'https://accounts.spotify.com/api/token'
    # headers = {}
    # data = {}
    # message = f"{client_id}:{client_secret}"
    # message_bytes = message.encode('ascii')
    # base64_bytes = base64.b64encode(message_bytes)
    # base64_message = base64_bytes.decode('ascii')

    # headers['Authorization'] = f"Basic {base64_message}"
    # headers["Scope"]= "user-read-recently-played"
    # data['grant_type'] = "client_credentials"

    # response = requests.post(url, headers=headers, data=data)
    # token_id = response.json()['access_token']
    # print(token_id)

    # EXTRACT part of the ETL process.
    
    # headers = {
    #     "Accept": "application/json",
    #     "Content-Type": "application/json",
    #     "Authorization": f"Bearer {token_id}"
    # }

    # today = datetime.datetime.now()
    # yesterday = today - datetime.timedelta(days=2)
    # yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    
    # r = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}",
    #                 headers=headers)
    # data = r.json()
    # print(data)
    
    # song_names = []
    # artist_names = []
    # played_at_list = []
    # timestamps = []

    # if "items" not in data.keys():
    #     raise Exception("No data available, check your credentials")

    # for song in data["items"]:
    #     song_names.append(song["track"]["name"])
    #     artist_names.append(song["track"]["artists"][0]["name"])
    #     played_at_list.append(song["played_at"])
    #     timestamps.append(song["played_at"][0:10])

    # song_dict = {
    #     "song_name" : song_names,
    #     "artist_name" : artist_names,
    #     "played_at": played_at_list,
    #     "timestamp": timestamps
    # }

    # song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
    # print(song_df)
    
    # # VALIDATION(TRANSFORM) part of the ETL process
    # if check_if_valid_data(song_df):
    #     print("Data valid, proceed to LOAD stage")
    

    # # LOAD part of the ETL process.
    # cursor, conn = Database().connect()
    # Database().create_tracks_table(cursor)

    # try:
    #     song_df.to_sql("my_played_tracks", Database().engine, index=False, if_exists='append', method=None)
    # except Exception as e:
    #     print(f"There was an issue loading the data:  {e}")

    # Database().close_connection(conn)