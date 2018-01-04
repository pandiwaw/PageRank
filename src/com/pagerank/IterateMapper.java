package com.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class IterateMapper extends Mapper<Object, Text, LongWritable, Attribute> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer str = new StringTokenizer(value.toString());
        long id = Long.parseLong(str.nextToken());
        double pageRank = Double.parseDouble(str.nextToken());
        List<LongWritable> followees = new ArrayList<>();
        Attribute attribute = new Attribute();
        if(str.hasMoreTokens()) {
            attribute = new Attribute(str.nextToken(), pageRank);
            followees = attribute.getFollowing();
        }

        long n = followees.size();
        double addedPageRank = (n > 0 ? pageRank / (double) n : 0);
        for(LongWritable idFollowee : followees) {
            Attribute attribute2 = new Attribute("", addedPageRank);
            context.write(new LongWritable(idFollowee.get()), attribute2);
        }

        Attribute attribute1 = new Attribute(attribute.getFollowee().toString(), 0);
        context.write(new LongWritable(id), attribute1);
    }
}
