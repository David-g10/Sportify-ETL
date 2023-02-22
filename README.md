# Spotify-ETL

This proyect is about an ETL process using the request module in Python to extract data(recently played songs) from the Spotify API, then the data is validated and transformed before the load stage in a Sqlite database.

The pipeline is also scheduled using apache-airflow, in two separeted terminals the commands <airflow webserver -p 8080> and <airflow scheduler> has to be executed.
