package com.GraphParser;

import com.PageRank.Main;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Izzati Alvandiar     <al.vandiar@gmail.com>
 */

public class FirstJobReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * First Job Reducer will scroll all the nodes pointed by the given "key" node, constructing a comma
     * separated list of values and initializing the page rank for the "key" node.
     * Output format is the following (separator is TAB):
     *
     *      <title>     <page-rank> <link1>, <link2>, <link3>, ..., <linkN>
     * As for the pagerank initial value, early version PageRank algorithm is used 1.0 as default,
     * however later versions of PageRank assume a probability distribution between 0 and 1, hence the
     * initial value is set to DAMPING FACTOR / TOTAL_NODES for each node in the graph.
     *
     */

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean first = true;
        StringBuilder links = new StringBuilder((Main.DAMPING / Main.NODES.size()) + "\t");

        for(Text value : values) {
            if(! first) {
                links.append(",");
            }
            links.append(value.toString());
            first = false;
        }

        context.write(key, new Text(links.toString()));
    }
}
