package com.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    private static final int ITERATION = 3;
    private static final int REDUCER = 122;
    private static final String prefix = "[topic] ";

    private static Configuration conf = new Configuration();

    public static String getRootDirectory() throws Exception {
        FileSystem dfs = FileSystem.get(conf);
        Path now = dfs.getHomeDirectory();
        return (now.getParent()).toString();
    }

    public static String getMyDirectory() throws Exception {
        FileSystem dfs = FileSystem.get(conf);
        Path now = dfs.getHomeDirectory();
        now = new Path(now.toString()).getParent();
        return (new Path(now.toString() + "/vandiar")).toString();
    }

    public static void cleanUp() throws Exception {
        String basepath = getRootDirectory();
        FileSystem dfs = FileSystem.get(conf);

        for(int i = 0; i <= 3; ++i) {
            dfs.mkdirs(new Path(basepath + "/iteration" + i));
            dfs.delete(new Path(basepath + "iteration" + i + "/output"), true);
        }
        dfs.delete(new Path(basepath + "/result"), true);
    }

    public static void init() throws Exception {
        Job job = Job.getInstance(conf, prefix + "init");
        job.setJarByClass(Main.class);
        job.setMapperClass(InitMapper.class);
        job.setReducerClass(InitReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Attribute.class);

        String inputPath = getRootDirectory() + "user/twitter";
        String outputPath = getMyDirectory() + "/iteration0/output";
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setNumReduceTasks(REDUCER);
        job.waitForCompletion(true);
    }

    public static void iterate(int iteration) throws Exception {
        Job job = Job.getInstance(conf, prefix + "iterasi " + iteration);
        job.setJarByClass(Main.class);
        job.setMapperClass(IterateMapper.class);
        job.setReducerClass(IterateReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Attribute.class);

        String inputPath = getRootDirectory() + "/iteration" + (iteration - 1) + "/output";
        String outputPath = getMyDirectory() + "/iteration" + (iteration) + "/output";
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setNumReduceTasks(REDUCER);
        job.waitForCompletion(true);
    }

    public static void finish() throws Exception {
        Job job = Job.getInstance(conf, prefix + "finishing");
        job.setJarByClass(Main.class);
        job.setMapperClass(FinishMapper.class);
        job.setReducerClass(FinishReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Attribute.class);

        String inputPath = getRootDirectory() + "/iteration" + ITERATION + "/output";
        String outputPath = getMyDirectory() + "/result";
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setNumReduceTasks(REDUCER);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        cleanUp();
        init();
        for(int i = 1; i <= ITERATION; ++i) {
            iterate(i);
        }
        finish();
    }
}
