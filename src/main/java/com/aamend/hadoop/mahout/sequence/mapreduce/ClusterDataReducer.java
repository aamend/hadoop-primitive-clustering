package com.aamend.hadoop.mahout.sequence.mapreduce;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by antoine on 12/05/14.
 */
public class ClusterDataReducer extends
        Reducer<Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable> {

    @Override
    protected void reduce(Text key,
                          Iterable<ArrayPrimitiveWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        // Nothing to do, just group same data by cluster ID
        for (ArrayPrimitiveWritable value : values) {
            context.write(key, value);
        }
    }
}
