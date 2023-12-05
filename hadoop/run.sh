#!/bin/bash

# Check if an argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 [Parameter]"
    exit 1
fi

PARAMETER=$1

# Remove previous output directory if it exists
hdfs dfs -rm -r t-output

# Run Hadoop job with provided parameter
hadoop jar TrafficMapReduce.jar com.example.traffic.TrafficAnalysis traffic/traffic.csv t-output $PARAMETER

# List and display output files
hdfs dfs -ls t-output
hdfs dfs -cat t-output/part-r-00000
