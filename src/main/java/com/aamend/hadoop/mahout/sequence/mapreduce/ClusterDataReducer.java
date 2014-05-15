package com.aamend.hadoop.mahout.sequence.mapreduce;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by antoine on 12/05/14.
 */
public class ClusterDataReducer extends
        Reducer<Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ClusterDataReducer.class);

    @Override
    protected void reduce(Text key,
                          Iterable<ArrayPrimitiveWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        // Used to group all data belonging to a same cluster
        for (ArrayPrimitiveWritable value : values) {
            context.write(key, value);
        }
    }
}
