# Sparkify Song Play Analysis Database:

## Introduction

Sparkify, a burgeoning startup in the music streaming industry, has launched a new project to analyze the data they've been collecting on songs and user activity on their music streaming app. The primary goal is to understand what songs users are listening to. To achieve this, a robust database and an ETL pipeline have been designed to facilitate the easy querying and analysis of this data.

## Database Design

### Schema Design

The database uses a star schema optimized for queries on song play analysis. This schema has one main fact table (`songplays`) and four dimension tables (`users`, `songs`, `artists`, and `time`).

- **songplays** - records log data associated with song plays. This includes songplay ID, start time, user ID, level, song ID, artist ID, session ID, location, and user agent.
- **users** - users in the app. User attributes include user ID, first name, last name, gender, and level.
- **songs** - songs in the music database. Attributes include song ID, title, artist ID, year, and duration.
- **artists** - artists in the music database. Attributes include artist ID, name, location, latitude, and longitude.
- **time** - timestamps of records in songplays broken down into specific units such as hour, day, week, month, year, and weekday.

### ETL Pipeline

The ETL pipeline extracts data from two main sources in JSON format stored in S3 buckets: song data and log data. 

- The song data (`song_data`) contains information about songs and artists. 
- The log data (`log_data`) contains user activity logs.

The pipeline processes the data using Python and SQL. It involves the following steps:

1. Extract data from the song and log datasets and load it into staging tables.
2. Transform data into a set of dimensional tables and a fact table.
3. Load processed data into the analytics tables in the Redshift cluster.

## Example Queries and Results

### Most Played Song

```sql
SELECT s.title, COUNT(*) as play_count
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
GROUP BY s.title
ORDER BY play_count DESC
LIMIT 1;
```

This query would return the most played song in the database.

### Highest Usage Time of Day

```sql
SELECT t.hour, COUNT(*) as total_plays
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY t.hour
ORDER BY total_plays DESC
LIMIT 1;
```

This query would return the hour of the day with the most song plays, indicating the highest usage time.

### User Level Analysis

```sql
SELECT level, COUNT(DISTINCT user_id) as user_count
FROM songplays
GROUP BY level;
```

This query would provide insights into how many unique users are in each subscription level (free or paid).