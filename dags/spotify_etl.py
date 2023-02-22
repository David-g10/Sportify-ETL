import datetime
import requests
import pandas as pd
from db.database import Database
import os
from dotenv import load_dotenv
import base64
import tekore as tk
from flask import Flask, request, redirect, session
import subprocess

load_dotenv()

user_id = os.environ["USER_ID"]
client_id = os.environ["SPOTIFY_CLIENT_ID"]
client_secret = os.environ["SPOTIFY_CLIENT_SECRET"]

def check_if_valid_data(df: pd.DataFrame) -> bool:
     # VALIDATION(TRANSFORM) part of the ETL process     

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
    print("Data valid, proceed to LOAD stage")
    return True

def get_token(client_id,client_secret,code):
    #Login to the api.
    url = 'https://accounts.spotify.com/api/token'
    headers = {}
    data = {}
    message = f"{client_id}:{client_secret}"
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    headers['Authorization'] = f"Basic {base64_message}"

    data['grant_type'] = "client_credentials"

    response = requests.post(url, headers=headers, data=data)
    token_id = response.json()['access_token']
    print(token_id)
    return token_id

def extract_recently_played_tracks(token_id):
    #EXTRACT part of the ETL process.
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token_id}"
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=2)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    
    r = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}",
                    headers=headers)
    data = r.json()
    
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    if "items" not in data.keys():
        raise Exception("No data available, check your credentials")

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
    print(song_df)
    return song_df    

def load_to_db(song_df: pd.DataFrame):
    # LOAD part of the ETL process.
    cursor, conn = Database().connect()
    Database().create_tracks_table(cursor)

    try:
        song_df.to_sql("my_played_tracks", Database().engine, index=False, if_exists='append', method=None)
        print("Data sucesfully loaded")
    except Exception as e:
        print(f"There was an issue loading the data:  {e}")

    Database().close_connection(conn)

def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

conf = tk.config_from_environment()
cred = tk.Credentials(*conf)
spotify = tk.Spotify()

auths = {}  # Ongoing authorisations: state -> UserAuth
users = {}  # User tokens: state -> token (use state as a user ID)

in_link = '<a href="/login">login</a>'
out_link = '<a href="/logout">logout</a>'
login_msg = f'You can {in_link} or {out_link}'

def app_factory() -> Flask:
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'aliens'
    app.config["SESSION_PERMANENT"] = False
    app.config["SESSION_TYPE"] = "filesystem"

    @app.route('/', methods=['GET'])
    def main():
        user = session.get('user', None)
        token = users.get(user, None)

        # Return early if no login or old session
        if user is None or token is None:
            session.pop('user', None)
            return f'User ID: None<br>{login_msg}'

        page = f'User ID: {user}<br>{login_msg}'
        if token.is_expiring:
            token = cred.refresh(token)
            users[user] = token

        try:    
            songs_df = extract_recently_played_tracks(token)
            if check_if_valid_data(songs_df):
                load_to_db(songs_df)

        except tk.HTTPError:
            page += '<br>Error in retrieving now playing!'

        return page

    @app.route('/login', methods=['GET'])
    def login():
        if 'user' in session:
            return redirect('/', 307)

        scope = tk.scope.user_read_recently_played
        auth = tk.UserAuth(cred, scope)
        auths[auth.state] = auth
        return redirect(auth.url, 307)

    @app.route('/callback', methods=['GET'])
    def login_callback():
        code = request.args.get('code', None)
        state = request.args.get('state', None)
        auth = auths.pop(state, None)

        if auth is None:
            return 'Invalid state!', 400

        token = auth.request_token(code, state)
        session['user'] = state
        users[state] = token
        return redirect('/', 307)

    @app.route('/logout', methods=['GET'])
    def logout():
        uid = session.pop('user', None)
        if uid is not None:
            users.pop(uid, None)
        return redirect('/shutdown', 307)

    @app.route('/shutdown', methods=['POST'])
    def shutdown():
        shutdown_server()
        return 'Server shutting down...'
    return app

def run_spotify_etl():
    application = app_factory()
    application.run('127.0.0.1', 5000)

# if __name__ == '__main__':



    # Authorize

    # auth_url = "https://accounts.spotify.com/authorize?"
    # headers = {
    #     "response_type": "code",
    #     "client_id" : client_id,
    #     "scope": "user-read-recently-played",
    #     "redirect_uri" : 'http://localhost:8888/callback'
    # }
    # 



