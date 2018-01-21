package com.RankOrder;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ThirdJobMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    /**
     * Rank Ordering (Mapper only)
     * Input file format (separator is TAB):
     *
     *      <title> <page-rank> <link1>,<link2>,<link3>,...,<linkN>
     *
     * This is simple job which does the ordering of our documents according to the computed pagerank.
     * We will map the pagerank (key) to its value (page) and Hadoop will do the sorting keys for us.
     * There is no need to implement a reducer: the mapping and sorting is enough for our purpose.
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int tIdx1 = value.find("\t");
        int tIdx2 = value.find("\t", tIdx1 + 1);

        // extract tokens from the current line
        String page = Text.decode(value.getBytes(), 0, tIdx1);
        float pageRank = Float.parseFloat(Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1)));

        context.write(new DoubleWritable(pageRank), new Text(page));
    }
}
