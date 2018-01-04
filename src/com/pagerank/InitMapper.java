package com.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class InitMapper extends Mapper<Object, Text, LongWritable, Attribute> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        long user = Long.parseLong(stringTokenizer.nextToken());
        long follower = Long.parseLong(stringTokenizer.nextToken());

        Attribute attribute = new Attribute(user + "", 0);
        context.write(new LongWritable(follower), attribute);

        Attribute attribute1 = new Attribute("", 0);
        context.write(new LongWritable(user), attribute1);
    }
}
