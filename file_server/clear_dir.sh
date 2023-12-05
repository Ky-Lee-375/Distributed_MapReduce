#!/bin/bash

# Define the directories to clear
directories=(
    "/home/jsku2/shardedLeader"
    "/home/jsku2/shardedWorker"
    "/home/jsku2/maple"
    "/home/jsku2/pre-juice"
    "/home/jsku2/juice"
    "/home/jsku2/post-juice"
)

# Loop through each directory and clear its contents
for dir in "${directories[@]}"; do
    echo "Clearing contents of: $dir"
    find "$dir" -mindepth 1 -delete
    echo "Contents of $dir have been cleared."
done

echo "All specified directories have been cleared."
