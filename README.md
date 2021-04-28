# ExternalSortProject
External sort with compression and decompression of chunks
External sort algorithm is used to save main memory usage while sorting a large database by generating smaller chunks. In this project, a compression and decompression mechanism is added to the external sort algorithm as an enhancement to save the disk space utilized by the intermittent chunks. 

## Setup:
### 1. Fetching Database:
Use the below command to fetch the 500MB of records and save it to events.json output file
mongoexport --uri mongodb+srv://sowjanya_3:*$pwd*@clusterexercises.104co.mongodb.net/testDB --collection=irs_data --db=testDB --out=events.json
Request for password and replace with $pwd

### 2. Adding Dependencies 
pom.xml
Use this file to add all the maven dependencies required for the implementation.

## External sort algorithm
ExternalMergeSort.java - it is a two-way merge sort algorithm used to compare two files at a time and sort the records respectively
It takes two input parameters.
i. Argument1 - input file to be sorted
ii. Argument2 - output file where the sorted records need to be stored

### 3. Steps to run:
In your IDE(ex. Eclipse), go to the run configurations and add the paths to these two files in the Arguments section. Then hit run button.
If dealing with a high amount of data such as 500MB, you might need to increase the java heap space to complete this operation.

### 4. Steps to increase java heap space:
In the run configurations, go to Arguments section and add "-Xms5046M -Xmx10092M" to the VM arguments textbox.

It will generate chunks with the chunksize mentioned in the code and sort the records based on the key(attribute) specified in the code.

## External sort algorithm with compression and decompression
ES.java - it is similar to ExternalMergeSort but with enhanced compression and decompression models to save disk space.

### 5. Steps to run:
Repeat the steps mentioned in Step 3 and 4 for the respective ES.java file.

It will generate compressed chunks(non-readable) with the chunksize mentioned in the code and sort the records based on the key(attribute) specified in the code with reduced storage space for the chunks.
