package com.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class IterateReducer extends Reducer<LongWritable, Attribute, LongWritable, Attribute> {

    private static final double D = 0.85F;

    @Override
    protected void reduce(LongWritable key, Iterable<Attribute> values, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        double sum = 0;

        for(Attribute attr : values) {
            List<LongWritable> followees = attr.getFollowing();
            for(int i = 0; i < followees.size(); ++i) {
                String add = followees.get(i).get() + ",";
                text.append(add.getBytes(), 0, add.length());
            }
            sum += attr.getPageRank();
        }

        double pageRank = D * sum + (1 - D);
        Attribute attribute1 = new Attribute(text.toString(), pageRank);
        context.write(new LongWritable(key.get()), attribute1);
    }
}
