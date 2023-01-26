import datetime
import requests
import pandas as pd

DATABASE_LOCATION = "sqlite//:my_played_tracks.sqlite"
USER_ID = "dav"
TOKEN_ID = "BQAu6vr8b6uGeBjBF5JJyg5MqKXvDkJGhSh6v8b8Rb7h_1LztNfrDuKh0F9utH4td6WReUh8lH6jAz6uof7h485Li5CclkHZ29UKq7sBbuoh-SViIqdTINI-CVjJGFAUfGMZF0RoTnzBTgoaCHTuVnAvMFogFeAR21V38FvHnzkEWmjEJBLZrZifcOBNPZlX0b_NZA"

# Link to generate the TOKEN_ID: https://developer.spotify.com/console/get-recently-played

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
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0 , minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("At least one of the returned songs doesn't come within the last 24 hours.")

    return True


if __name__ == "__main__":

    #EXTRACT part of the ETL process.

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN_ID}"
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}",
                    headers=headers)
    
    data = r.json()

    #print(data)
    
    song_names = []
    artists_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artists_names.append(song["track"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artists_name" : artists_names,
        "played_at": played_at_list,
        "timestamps": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artists_name", "played_at", "timestamps"])
    
    # VALIDATION(TRANSFORM) part of the ETL process
    if check_if_valid_data(song_df):
        print("Data valid, proceed to LOAD stage")
    
    # LOAD part of the ETL process.