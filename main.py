import datetime
import requests
import pandas as pd

DATABASE_LOCATION = "sqlite//:my_played_tracks.sqlite"
USER_ID = "dav"
TOKEN_ID = "BQDtCh7dehHemY9MUH4_CUq4XEzD124y116wtwQxKa31qfPT1lDsvHM2AicxMY0GnXhnlT1qWg20OKJw-2P8vdLfovHLhkUGzhuzbA06LzBuSMRfZ4f66u6-IH_dNF5_-ufRZ5RA2OwgbU6RtISyrVRqDBWTW_9u_fMLpzhJmdYl4yquYnZFgOfm5eTucBbKtO8Q4w"


if __name__ == "__main__":

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
    print(song_df)