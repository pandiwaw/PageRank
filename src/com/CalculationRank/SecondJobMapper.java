package com.CalculationRank;

import com.PageRank.Main;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondJobMapper extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * PageRank Calculation algorithm (Mapper)
     * Input file format (separator is TAB):
     *
     *      <title> <page-rank> <link1>,<link2>,<link3>,...,<linkN>
     *
     * Output has two kind of records:
     * One record composed by the collection of links of each page:
     *
     *      <title> | <link1>,<link2>,<link3>,...,<linkN>
     *
     * Another record composed by linked page, the page rank of the source page
     * and total amount of out links of the source page:
     *
     *      <link>  <page-rank>  <total-links>
     *
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
        String pageRank = Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1));
        String links = Text.decode(value.getBytes(), tIdx2 + 1, value.getLength() - (tIdx2 + 1));

        String[] allOtherPages = links.split(",");
        for(String otherPage : allOtherPages) {
            Text pageRankWithTotalLinks = new Text(pageRank + "\t" + allOtherPages.length);
            context.write(new Text(otherPage), pageRankWithTotalLinks);
        }

        // put the original links so the reducer is able to produce the correct output.
        context.write(new Text(page), new Text(Main.LINKS_SEPARATOR + links));
    }
}
