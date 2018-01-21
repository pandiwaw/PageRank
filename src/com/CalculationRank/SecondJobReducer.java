package com.CalculationRank;

import com.PageRank.Main;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Izzati Alvandiar     <al.vandiar@gmail.com>
 */

public class SecondJobReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * PageRank Calculation algorithm (Reducer)
     * Input file format has two kind of records (separator is TAB):
     *
     * One record composed by the collection of links of each page:
     *
     *      <title> | <link1>,<link2>,<link3>,...,<linkN>
     *
     * Another record composed by linked page, the page rank of the source page
     * and total amount of out links of the source page:
     *
     *      <link>  <page-rank>  <total-links>
     *
     */

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder links = new StringBuilder();
        double sumShareOtherPageRanks = 0.0;

        for(Text value : values) {
            String content = value.toString();

            if(content.startsWith(Main.LINKS_SEPARATOR)) {
                // if this value contain node links append them to the 'links' string
                // for future use: this is needed to reconstruct the input for Job#2 Mapper
                // in case of multiple iterations of it.
                links.append(content.substring(Main.LINKS_SEPARATOR.length()));
            } else {
                String[] split = content.split("\\t");

                // extract tokens
                double pageRank = Double.parseDouble(split[0]);
                int totalLinks = Integer.parseInt(split[1]);

                // add the contribution of all the pages having an outlink pointing
                // to the current node: we will add the DAMPING factor later when recomputing
                // the final pagerank value before submitting the result to the next job.
                sumShareOtherPageRanks += (pageRank / totalLinks);
            }
        }

        double newRank = Main.DAMPING * sumShareOtherPageRanks + (1 - Main.DAMPING);
        context.write(key, new Text(newRank + "\t" + links));
    }
}
