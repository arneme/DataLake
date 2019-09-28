# Data Lake project
This README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository.

## Table of Contents

1. [Summary of the Data lake project](#summary)
2. [Setting up a Spark Cluster](#spark)
3. [How to run the scripts](#run)
4. [Notes](#notes)

## <a name="summary"></a>Summary of the Data lake project
The purpose of this project is to provide Sparkify the necessary tool to retrieve vital, business oriented, information about their service. Their current main focus is to be able to, easily and fast, to retrieve information about which songs users are listening to. They do have information about this stored in logfiles already but it is not easy to search or aggregate data from these logfiles. In the Data Lake assignment we have been asked to transform data from the logfiles together with meta data about songs (also stored in files) into a database design that will allow Sparkify to meet their business need related to finding out which songs their users are listening to.

In the Data Lake project we will extract the logfiles and song data files into Spark and also use Spark to transform the files and store Spark parquet files that follows the business oriented star schema. I.e. we will create artist, song, user, time dimension tables and a songplay fact table stored as Spark parquet files.

## <a name="run"></a>How to run the scripts

### Setup a Spark Cluster using your AWS Console
The python script is a script designed to run on a Spark cluster and it is therefore necessary to setup the cluster before the _etl.py_ script can be runned.

### Run etl.py standalone

