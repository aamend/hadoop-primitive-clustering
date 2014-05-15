package com.aamend.hadoop.mahout.sequence.mapreduce;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by antoine on 12/05/14.
 */
public class ClusterDataReducer extends
        Reducer<Text, ArrayPrimitiveWritable, Text, Text> {

    private static final Text VALUE = new Text();

    @Override
    protected void reduce(Text key,
                          Iterable<ArrayPrimitiveWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        // Used to group all data belonging to a same cluster
        for (ArrayPrimitiveWritable value : values) {
            VALUE.set(Arrays.toString((int[]) value.get()));
            context.write(key, VALUE);
        }
    }
}
