## Data modeling with Postgres
### Summary
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3. In this project, you'build an ETL pipeline for a database hosted on Redshift.

The Redshift database contains these tables:

* Staging Tables
 - staging_events
 - staging_songs
* Fact Table:
 - songplays: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
* Dimension Tables: 
 - users: user_id, first_name, last_name, gender, level
 - songs: song_id, title, artist_id, year, duration
 - artists: artist_id, name, location, latitude, longitude
 - time: start_time, hour, day, week, month, year, weekday


The project includes six files:
* dwh.cfg contains infra params und access keys
* create_cluster.ipynb creates the Redshift cluster
* create_tables.py drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
* etl.py reads and processes files from song_data and log_data and loads them into your tables. 
* sql_queries.py contains all your sql queries, and is imported into the last three files above.
README.md provides discussion on your project.

### Build ETL Pipeline
Run create_tables.py to create/reset your tables. Run etl.py to process the copy data from s3 to Redshift in staging tables and then populate fact and dim tables.