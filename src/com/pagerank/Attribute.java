package com.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Attribute implements Writable {

    private Text followee;
    private Text pageRank;

    public Attribute() {
        followee = new Text();
        pageRank = new Text();
    }

    public Attribute(String param, double pageRank) {
        this.followee = new Text(param);
        this.pageRank = new Text(Double.toString(pageRank));
    }

    public Attribute(Text param, DoubleWritable pageRank) {
        this.followee = param;
        this.pageRank = new Text(Double.toString(pageRank.get()));
    }

    public Text getFollowee() {
        return new Text(followee);
    }

    public List<LongWritable> getFollowing() {
        List<LongWritable> followees = new ArrayList<>();
        String temp = this.followee.toString();
        String[] parsed = temp.split(",");
        for(String str : parsed) {
            try {
                followees.add(new LongWritable(Long.parseLong(str.trim())));
            } catch(Exception e) { System.err.print(e);}
        }
        return followees;
    }

    public long getSize() {
        return getFollowing().size();
    }

    public double getPageRank() {
        return Double.parseDouble(pageRank.toString());
    }

    public void setPageRank(double pageRank) {
        this.pageRank = new Text(pageRank + "");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pageRank.readFields(dataInput);
        followee.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        pageRank.write(dataOutput);
        followee.write(dataOutput);
    }

    @Override
    public String toString() {
        return pageRank.toString() + " " + followee.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 37;
        int result = 1;
        result = result * prime + followee.hashCode();
        result = result * prime + pageRank.hashCode();
        return result;
    }

}
