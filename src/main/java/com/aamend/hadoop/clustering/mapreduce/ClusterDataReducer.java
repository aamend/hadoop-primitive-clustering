package com.aamend.hadoop.clustering.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by antoine on 12/05/14.
 */
public class ClusterDataReducer extends Reducer<IntWritable, ObjectWritable, IntWritable, ObjectWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<ObjectWritable> values, Context context)
            throws IOException, InterruptedException {

        // Reducer used only to group all data belonging to a same cluster
        for (ObjectWritable value : values) {
            context.write(key, value);
        }
    }
}
