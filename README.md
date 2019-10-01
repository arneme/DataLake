# Data Lake project
This README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository.

## Table of Contents

1. [Summary of the Data lake project](#summary)
2. [Setting up a Spark Cluster](#spark)
3. [How to run the scripts](#run)
4. [Notes](#notes)

## <a name="summary"></a>Summary of the Data lake project
The purpose of this project is to provide Sparkify the necessary tool to retrieve vital, business oriented, information about their service. Their current main focus is to be able to, easily and fast, to retrieve information about which songs users are listening to. They do have information about this stored in logfiles already but it is not easy to search or aggregate data from these logfiles. In the Data Lake assignment we have been asked to transform data from the logfiles together with meta data about songs (also stored in files) into a business oriented (star) database design that will allow Sparkify to meet their business need related to for example finding out which songs their users are listening to and when.

In the Data Lake project we will read the logfiles and song data files into Spark and also use Spark to transform the files and store Spark parquet files that follows the business oriented star schema. I.e. we will create artist, song, user, time dimension tables and a songplay fact table stored as Spark parquet files.

## <a name="run"></a>How to run the scripts

### Setup a Spark Cluster using AWS Console
The python script _etl.py_ is a script designed to run on a Spark cluster and it is therefore necessary to setup the cluster before the _etl.py_ script can be run. The next paragraphs describes the steps used in order to setup a cluster for this project and how to run the script on the cluster.

#### Create keys
To login to the master node of your Spark cluster (this is where you will run the script) it is necessary to generate a keypair for ssh. The private part of the key will have to be stored on the machine you use to login from and the public key will be on the master Spark node. The next paragtraph describes how to do this.

Login to your AWS account and select the EC2 service. Select *Key Pairs* from *Network and Security* in the right hand side menu. Click on the  *Create Key Pair* button and give it an appropriate name (the private part of the key will be automatically downloaded to your local computer. Store it in a safe place).

#### Setup Spark Cluster
The easiest way to setup a Spark cluster is to use the (mostly) automated Elastic Map Reduce (EMR) service. The steps we have used is as follows:

1. Login to your AWS account and select the EMR service
2. Click on the *Create Cluster* button
3. Give the cluster a name and maker sure that *Launch Mode*: *Cluster* is selected
4. Select hardware configuration *m5.xlarge* and software configuration release *emr-5.27.0* and application *Spark*.
5. Select the number of nodes in your cluster
5. In the *Security and Access* configuration make sure to select the keypair you crerated in the previous section
6. Scroll down and click the final *Create Cluster* button

You can of course use other types of EC2 instances for your cluster if you wish. For this project it is sufficient to have one node (since the amount of data is not really large) but you can use more if you wish :-)

After a while your Spark cluster should be up and running and you are ready to run your pySpark PYTHON script (or notebook) using this cluster.

### Run etl.py standalone
To transfer the python script to your Spark master node, first transfer it to a pre created bucket (data-lake-sparkify) and then use _aws s3 cp s3://data-lake-sparkify/etl.py ._ to copy it to the EMR master node (after logging in to the master node using ssh).

Login to your cluster using ssh naming the private key using the -i option (change the path to where you have stored the pem file). Note: You will have to add a rule to the firewall for the master node that allows inbound traffic from your IP address first (click on the *Security groups for* master in the cluster overview to do this).


_ssh -i ~/.ssh/spark-cluster.pem hadoop@ec2-35-160-245-xxx.us-west-2.compute.amazonaws.com_

When logged in, you can run the python script like this (after you have copied it to the master node):

_spark\_submit etl.py_

## <a name="notes"></a>Notes
The _etl.py_ script assumes that a bucket called s3://data-lake-sparkify exists (this is where it will store the parquet files). Before you run the script you should create a bucket for the parquet files and change the name in the _etl.py_ file accordingly.

Since the etl.py script is uploaded to the master node there is no use for the dl.cfg file and it has not been included in the github repo as part of the submission.


