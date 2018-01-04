package com.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class FinishReducer extends Reducer<LongWritable, Attribute, LongWritable, Attribute> {

    @Override
    protected void reduce(LongWritable key, Iterable<Attribute> values, Context context) throws IOException, InterruptedException {
        List<Attribute> results = new ArrayList<>();
        for (Attribute attr : values) {
            Attribute append = new Attribute(attr.getFollowee().toString(), attr.getPageRank());
            results.add(append);
            Collections.sort(results, new Comparator<Attribute>() {
                @Override
                public int compare(Attribute o1, Attribute o2) {
                    if(o1.getPageRank() > o2.getPageRank()) {
                        return -1;
                    } else if(o1.getPageRank() < o2.getPageRank()) {
                        return 1;
                    }
                    return 0;
                }
            });
            if(results.size() > 5) {
                results.remove(results.size() - 1);
            }
        }
        for (Attribute result : results) {
            Attribute attribute = new Attribute(result.getFollowee().toString(), result.getPageRank());
            context.write(new LongWritable(key.get()), attribute);
        }
    }
}
