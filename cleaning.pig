-- This is the intial cleaning for the spotify data

fs -rm -f -r -R clean_data/;
fs -rm -f -r R hive_data/;

---------------------------------------------------------------------
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();


tracks = LOAD 'raw_data/dataset.csv' USING PigStorage(',')
 AS (index:int, track_id:chararray, artists:chararray, album_name:chararray, track_name:chararray,
  popularity:int, duration_ms:int, explicit:chararray, danceability:float, energy:float, key:int,
  loudness:float, mode:int, speechiness:float, acousticness:float, instrumentalness:float,
  liveness:float, valence:float, tempo:float, time_signature:float, track_genre:chararray);

-- Taking the example of track with index 21 we can see that the ',' in the album_name is causing
-- Pig to injest the data incorrectly leading to many other values in the wrong columns and a general mismatch

-- problematic_track_filter = FILTER tracks BY (index == 21);
-- DUMP problematic_track_filter;

-- Load data using CSVLoader as opposed the orignal PigStorage
tracks = LOAD 'raw_data/dataset.csv' USING CSVLoader(',')
    AS (index:int, track_id:chararray, artists:chararray, album_name:chararray, track_name:chararray,
  popularity:int, duration_ms:int, explicit:chararray, danceability:float, energy:float, key:int,
  loudness:float, mode:int, speechiness:float, acousticness:float, instrumentalness:float,
  liveness:float, valence:float, tempo:float, time_signature:float, track_genre:chararray);

-- problematic_track_filter = FILTER tracks BY (index == 21);
-- DUMP problematic_track_filter;
-- Much Better, no empty or miss-aligned features

----------------------------------------------------------------------

-- Filter out first line so the header isn't treated as a piece of data
-- Using the values from the first row to filter out by. Don't expect a track_id to be called literally 'track_id'
-- Also filter out any nulls for empty strings relating to track_name, artists or album_name
filtered_tracks = FILTER tracks BY (track_id != 'track_id');
filtered_tracks = FILTER filtered_tracks BY (track_name IS NOT NULL) AND (artists IS NOT NULL) AND (album_name IS NOT NULL);
filtered_tracks = FILTER filtered_tracks BY (track_name != '') AND (artists != '') AND (album_name != '');
--LIMITED_TRACKS = LIMIT filtered_tracks 5;
--DUMP LIMITED_TRACKS;
-- First row is not visible, we can move onto the next step

------------------------------------------------------------------------

-- In this data it is visible that the same song can be present multiple times 
-- but in different albums. This can be re-relaseses and since In my analysis I don't want duplicatea
-- to impact the results, I will de-dupe based on the song name, artists and duration and keep the one with the highest popularity

tracks = GROUP filtered_tracks ALL;
track_count = FOREACH tracks GENERATE COUNT(filtered_tracks.track_id);
-- DUMP track_count;

-- There are 114,000 rows
grouped_tracks = GROUP filtered_tracks BY (track_name, artists, duration_ms);
deduped_tracks = FOREACH grouped_tracks {
    sorted_tracks = ORDER filtered_tracks BY popularity DESC;
    top_track = LIMIT sorted_tracks 1;
    GENERATE FLATTEN(top_track);
};

grouped_deduped_tracks = GROUP deduped_tracks ALL;
deduped_track_count = FOREACH (GROUP deduped_tracks ALL) GENERATE COUNT(deduped_tracks);
-- DUMP deduped_track_count;

-- Much smaller at just 83,075

-- Sanity check on example that has many tracks in the data under different albums
filtered_track_example = FILTER filtered_tracks BY (track_name == 'Winter Wonderland') AND (artists == 'Jason Mraz');
filtered_track_example_count = FOREACH (GROUP filtered_track_example ALL) GENERATE COUNT(filtered_track_example);
-- DUMP filtered_track_example_count;
-- 10 Entries for the same song here

filtered_deduped_track_example = FILTER deduped_tracks BY (track_name == 'Winter Wonderland') AND (artists == 'Jason Mraz');
filtered_deduped_track_example_count = FOREACH (GROUP filtered_deduped_track_example ALL) GENERATE COUNT(filtered_deduped_track_example);
-- DUMP filtered_deduped_track_example_count;
-- Just 1 entry for the same song in the deduped_tracks
-- Success


-- Since the Artists column contains multiple artists names, I want to separate the artists and have a separate
-- table containing the artists and each song.
split_artists_single_row = FOREACH deduped_tracks GENERATE track_id, FLATTEN(STRSPLIT(artists, ';'));
split_artists_multiple_rows = FOREACH split_artists_single_row GENERATE $0, FLATTEN(TOBAG(*));
-- Have to remove instances where track_id is duplicate under artists due to FLATTEN(TOBAG(*)) opearation
split_artists_multiple_rows = FILTER split_artists_multiple_rows BY ($0 != $1);

-- Store all the two cleanded datasets in a clean_data folder and under either deduped_tracks or split_artists directories 
STORE split_artists_multiple_rows INTO 'clean_data/split_artists' USING org.apache.pig.piggybank.storage.CSVExcelStorage();
STORE deduped_tracks INTO 'clean_data/deduped_tracks' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-- Store the two same datasets for reading into hive using a tab as a delimiter.
STORE split_artists_multiple_rows INTO 'hive_data/split_artists' USING PigStorage();
STORE deduped_tracks into 'hive_data/deduped_tracks' USING PigStorage();