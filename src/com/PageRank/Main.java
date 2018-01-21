package com.PageRank;

import com.CalculationRank.SecondJobMapper;
import com.CalculationRank.SecondJobReducer;
import com.GraphParser.FirstJobMapper;
import com.GraphParser.FirstJobReducer;
import com.RankOrder.ThirdJobMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Izzati Alvandiar     <al.vandiar@gmail.com>
 */

public class Main {

    // arguments key
    private static final String KEY_DAMPING = "--damping";
    private static final String KEY_DAMPING_ALIAS = "-d";

    private static final String KEY_COUNT = "--count";
    private static final String KEY_COUNT_ALIAS = "-c";

    private static final String KEY_INPUT = "--input";
    private static final String KEY_INPUT_ALIAS = "-i";

    private static final String KEY_OUTPUT = "--output";
    private static final String KEY_OUTPUT_ALIAS = "-o";

    private static final String KEY_HELP = "--help";
    private static final String KEY_HELP_ALIAS = "-h";

    // utility attributes
    private static NumberFormat NF = new DecimalFormat("00");
    public static Set<String> NODES = new HashSet<>();
    public static String LINKS_SEPARATOR = "|";

    // configuration values
    public static Double DAMPING = 0.85;
    private static int ITERATIONS = 2;
    private static String INPUT = "";
    private static String OUTPUT = "";


    /**
     * Main class that run against the Hadoop Cluster
     * It will run all the jobs needed for the Page Rank Algorithm.
     *
     * @param args          input parameters configuration for running application
     * @throws Exception    throws if there is exist error on the program, such as number formatting or exaggerate arguments.
     */

    public static void main(String[] args) throws Exception {
        // parse input parameters
        try {
            for(int i = 0; i < args.length; i += 2) {
                String key = args[i];
                String value = args[i + 1];

                switch (key) {
                    case KEY_DAMPING:
                    case KEY_DAMPING_ALIAS:
                        // damping factor is in the interval [0..1]
                        Main.DAMPING = Math.max(Math.min(Double.parseDouble(value), 1.0), 0.0);
                        break;
                    case KEY_COUNT:
                    case KEY_COUNT_ALIAS:
                        // at least 1 iteration
                        Main.ITERATIONS = Math.max(Integer.parseInt(value), 1);
                        break;
                    case KEY_INPUT:
                    case KEY_INPUT_ALIAS:
                        Main.INPUT = value.trim();
                        if (Main.INPUT.charAt(Main.INPUT.length() - 1) == '/')
                            Main.INPUT = Main.INPUT.substring(0, Main.INPUT.length() - 1);
                        break;
                    case KEY_OUTPUT:
                    case KEY_OUTPUT_ALIAS:
                        Main.OUTPUT = value.trim();
                        if (Main.OUTPUT.charAt(Main.OUTPUT.length() - 1) == '/')
                            Main.OUTPUT = Main.OUTPUT.substring(0, Main.INPUT.length() - 1);
                        break;
                    case KEY_HELP:
                    case KEY_HELP_ALIAS:
                        debugMessage(null);
                        System.exit(0);
                }
            }
        } catch(ArrayIndexOutOfBoundsException | NumberFormatException e) {
            debugMessage(e.getMessage());
            System.exit(1);
        }

        // check for parameter to be set
        if(Main.INPUT.isEmpty() || Main.OUTPUT.isEmpty()) {
            debugMessage("Missing required parameters");
            System.exit(1);
        }

        // delete output path if it's already exist
        FileSystem fs = FileSystem.get(new Configuration());
        if(fs.exists(new Path(Main.OUTPUT)))
            fs.delete(new Path(Main.OUTPUT), true);

        // print current configuration in the console
        System.out.println("Damping factor: " + Main.DAMPING);
        System.out.println("Number of iterations: " + Main.ITERATIONS);
        System.out.println("Input Directory: " + Main.INPUT);
        System.out.println("Output Directory: " + Main.OUTPUT);
        System.out.println("--------------------------------------");

        Thread.sleep(1000);

        String inputPath = null;
        String lastOutputPath = null;
        Main pagerank = new Main();

        System.out.println("Running Job #1 (Graph Parsing) ....");
        boolean isCompleted = pagerank.parseGraph(INPUT, OUTPUT + "/iter000");

        if(! isCompleted) {
            System.exit(1);
        }

        for(int run = 0; run < ITERATIONS; ++run) {
            inputPath = OUTPUT + "/iter" + NF.format(run);
            lastOutputPath = OUTPUT + "/iter" + NF.format(run + 1);
            System.out.println("Running Job #2 [" + (run + 1) + "/" + Main.ITERATIONS + "] (Pagerank Calculation) ....");
            isCompleted = pagerank.calculateRank(inputPath, lastOutputPath);
            if(! isCompleted) {
                System.exit(1);
            }
        }

        System.out.println("Running job #3 ...");
        isCompleted = pagerank.orderRank(inputPath, lastOutputPath);
        if(! isCompleted) {
            System.exit(1);
        }

        System.out.println("DONE !");
        System.exit(0);
    }

    /**
     * This will run Job #1 (Graph Parsing)
     * Will parse the graph given as input and initialize the page rank.
     *
     * @param input         the directory of the input data
     * @param output        the main directory of the output
     *
     */

    private boolean parseGraph(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "Job #1");
        job.setJarByClass(Main.class);

        // input / mapper
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(FirstJobMapper.class);

        // otuput / reducer
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(FirstJobReducer.class);

        return job.waitForCompletion(true);
    }

    /**
     * This will run Job #2 (Rank Calculation).
     * It calculates the new ranking and generates the same output format as the input,
     * so this job can run multiple times (more iterations will increase accuracy).
     *
     * @param input         the directory of the input data
     * @param output        the main directory of the output
     *
     */

    private boolean calculateRank(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "Job #2");
        job.setJarByClass(Main.class);

        // input / mapper
        FileInputFormat.setInputPaths(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(SecondJobMapper.class);

        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(SecondJobReducer.class);

        return job.waitForCompletion(true);
    }

    /**
     * This will run the Job #3 (Rank Ordering)
     * It will sort documents according to their page rank value.
     *
     * @param input         the directory of the input data
     * @param output        the main directory of the output
     *
     */

    private boolean orderRank(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "Job #3");
        job.setJarByClass(Main.class);

        // input / mapper
        FileInputFormat.setInputPaths(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(ThirdJobMapper.class);

        // output
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true);
    }

    /**
     * Print the main help text in the System.out
     * @param error     optional error message to display
     */

    private static void debugMessage(String error) {
        if(error != null) {
            System.err.println("[ERROR]: " + error + ".\n");
        }

        System.out.println("Usage: pagerank.jar " + KEY_INPUT + " <input> " + KEY_OUTPUT + " <output>\n");
        System.out.println("Options:\n");
        System.out.println("    " + KEY_INPUT + "( " + KEY_INPUT_ALIAS + " ) <input>    The directory of the input graph [REQUIRED]");
        System.out.println("    " + KEY_OUTPUT + "( " + KEY_OUTPUT_ALIAS + " ) <output>    The directory of the output result [REQUIRED]");
        System.out.println("    " + KEY_DAMPING + "( " + KEY_DAMPING_ALIAS + " ) <damping>    The damping factor [OPTIONAL]");
        System.out.println("    " + KEY_COUNT + "( " + KEY_COUNT_ALIAS + " ) <iterations>    The number of iterations [OPTIONAL]");
        System.out.println("    " + KEY_HELP + "( " + KEY_INPUT_ALIAS + " )            Display the help text\n");
    }
}
