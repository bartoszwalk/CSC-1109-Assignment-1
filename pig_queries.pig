-- This file is where all the pig queries are located.
-- This includes the two simple queries.

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- First just clearing the results/ folders in order 
fs -rm -f -r results/album_with_most_songs;

-- Query 1: What are the 5 albums with the most number of songs

track_data = LOAD 'clean_data/deduped_tracks/part-r-00000' USING CSVLoader() 
    AS (index:int, track_id:chararray, artists:chararray, album_name:chararray, track_name:chararray,
    popularity:int, duration_ms:int, explicit:chararray, danceability:float, energy:float, key:int,
    loudness:float, mode:int, speechiness:float, acousticness:float, instrumentalness:float,
    liveness:float, valence:float, tempo:float, time_signature:float, track_genre:chararray);

grouped_by_album = GROUP track_data by album_name;
album_track_count = FOREACH grouped_by_album GENERATE group AS album_name, COUNT(track_data) as track_count; 
sorted_album_track_count = ORDER album_track_count BY track_count DESC;
top_5_albums_by_song_count = LIMIT sorted_album_track_count 5;
-- DUMP top_5_albums_by_song_count;

-- Results are as follows:
-- (The Complete Hank Williams,110)
-- (Greatest Hits,77)
-- (Mozart: A Night of Classics,74)
-- (Hans Zimmer: Epic Scores,68)
-- (Mozart - All Day Classics,54)

-- Query 2: Which are the top 5 most popular explicit tracks and list the track_name, artist(s) and popularity score
only_explicit_tracks = FILTER track_data BY explicit == 'True';
sorted_explicit_tracks = ORDER only_explicit_tracks BY popularity DESC;
top_5_explicit_tracks_by_popularity = LIMIT sorted_explicit_tracks 5;
top_5_explicit_tracks = FOREACH top_5_explicit_tracks_by_popularity GENERATE track_name, album_name, popularity;
DUMP top_5_explicit_tracks;

-- Results are as follows:
-- (I'm Good (Blue),I'm Good (Blue),98)
-- (Me Porto Bonito,Un Verano Sin Ti,97)
-- (Under The Influence,Indigo (Extended),96)
-- (Moscow Mule,Un Verano Sin Ti,94)
-- (CUFF IT,RENAISSANCE,93)









