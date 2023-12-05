package com.example.traffic;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration; 

import java.util.Arrays;
public class TrafficMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");

        Configuration conf = context.getConfiguration();
        String passedInterconnectionType = conf.get("interconnectionType");

        // Check if the array has enough elements (at least 12)
        if (parts.length < 12) {
            // Log a warning or skip this line
            // System.out.println("Skipping line: " + line);

        // Extract the necessary columns
        String interconnectType = parts[10].trim();
        String detectionType = parts[9].trim();

        // Skip processing for header line
        if (interconnectType.equals("Interconne") || detectionType.equals("Detection_")) {
            return;
        }

        if (interconnectType.equals("")) {
            interconnectType = "empty";
        }

        if (detectionType.equals("")) {
            detectionType = "empty";
        }

        // Your logic here, for example:
        if (interconnectType.equals(passedInterconnectionType)) {
            // System.out.println("{ INTERCONNE: " + interconnectType + " || DETECTION: " + detectionType);
            word.set(detectionType);
            context.write(word, one);
        }

        // Additional conditions and logic can be added as needed
    }

}
