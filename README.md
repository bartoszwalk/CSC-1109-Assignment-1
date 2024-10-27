# CSC-1109-Assignment-1

This is the repository for CSC-1109 Data at Speed and Scale Assignment 1.

This repository contains the various pig and hive scripts used in order to complete the assignments, the data (both raw and cleaned) and jupyter notebooks used to create the visualisations.

### Contents of this repository can be explored as follows:

- [Raw Data](raw_data) - This contains the raw uncleaned Spotify dataset.
- [Clean Data](clean_data) - This contains the cleaned CSV version of the data.
- [Hive Data](hive_data) - This contains the cleaned TSV version of the data for loading into hive. If the cleaning script is re-run, the resulting files need to be renamed to .tsv files.
- [Results](results) - This contains the results from the three complex queries so that they can be loaded into a notebook and visualisations can be produced.
- [Visualisations Notebook](Visualisations.ipynb) - This notebook reads in the results from the complex queries and produces visualisations.
- [Cleaning](cleaning.pig) - This script cleans the data and saves it the clean_data and hive_data folders
- [Pig Queries](pig_queries.pig) - This file performs 2 simple queries on the cleaned data.
- [Hive Queries](hive_queries.hive) - This file performs 2 simple queries just like the pig queries but also performs 3 complex queries involving joins, sampling and aggregate functions. It saves the result to Results/ folder.
