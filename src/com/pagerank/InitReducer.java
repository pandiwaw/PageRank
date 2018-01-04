package com.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class InitReducer extends Reducer<LongWritable, Attribute, LongWritable, Attribute> {

    @Override
    protected void reduce(LongWritable key, Iterable<Attribute> values, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        for(Attribute attribute : values) {
            List<LongWritable> followees = attribute.getFollowing();
            for(int i = 0; i < followees.size(); ++i) {
                String add = followees.get(i).get() + ",";
                text.append(add.getBytes(), 0, add.length());
            }
        }
        Attribute attribute1 = new Attribute(text.toString(), 1);
        context.write(new LongWritable(key.get()), attribute1);
    }
}
