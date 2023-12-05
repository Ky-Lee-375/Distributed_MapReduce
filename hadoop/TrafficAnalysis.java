package com.example.traffic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrafficAnalysis {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: TrafficAnalysis <input path> <output path> <interconnection type>");
            System.exit(-1);
        }

        conf.set("interconnectionType", args[2]);
        Job job = Job.getInstance(conf, "Traffic Analysis");
        job.setJarByClass(TrafficAnalysis.class);
        job.setMapperClass(TrafficMapper.class);
        job.setReducerClass(TrafficReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
