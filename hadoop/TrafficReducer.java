package com.example.traffic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TrafficReducer extends Reducer<Text, IntWritable, Text, Text> {

    private static final Log LOG = LogFactory.getLog(TrafficReducer.class);
    private int globalTotalCount = 0;
    private Map<Text, Integer> keyCounts = new HashMap<>();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sumForKey = 0;

        // Calculate the sum for this key
        for (IntWritable val : values) {
            sumForKey += val.get();
        }

        globalTotalCount += sumForKey;
        keyCounts.put(new Text(key), sumForKey);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Calculate and write the percentage for each key
        for (Map.Entry<Text, Integer> entry : keyCounts.entrySet()) {
            double percentage = ((double) entry.getValue() / globalTotalCount) * 100.0;
            context.write(entry.getKey(), new Text(String.format("%.2f%%", percentage)));
        }
    }
}
