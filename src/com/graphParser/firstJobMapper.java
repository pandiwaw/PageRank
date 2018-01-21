package com.graphParser;

import com.pagerank.Main;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class firstJobMapper extends Mapper<Text, Text, Text, Text> {

    /**
     * firstJobMapper will simply parse a line of the input graph creating a map with key-value(s) pairs.
     * Input format is the following (separator is TAB):
     *
     *      <nodeA>     <nodeB>
     *
     * which denotes an edge going from <nodeA> to <nodeB>
     * We would need to skip comment lines (denoted by the # characters at the beginning of the line).
     * We will also collect all the distinct nodes in our graph: this is needed to compute the initial
     * pagerank value in Job #1 reducer and also in later jobs.
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        if(value.charAt(0) != '#') {
            int tabIndex = value.find("\t");
            String nodeA = Text.decode(value.getBytes(), 0, tabIndex);
            String nodeB = Text.decode(value.getBytes(), tabIndex + 1, value.getLength() - (tabIndex + 1));
            context.write(new Text(nodeA), new Text(nodeB));

            // add the current source node to the list so we can
            // compute the total amount of nodes of our graph in Job #2
            Main.NODES.add(nodeA);
            // also add the target node to the same list: we may have a target node
            // with no outlinks (so it will never be parsed as source)
            Main.NODES.add(nodeB);
        }
    }
}
