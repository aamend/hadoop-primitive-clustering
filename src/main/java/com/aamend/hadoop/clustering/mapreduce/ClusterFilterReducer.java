package com.aamend.hadoop.clustering.mapreduce;

import com.aamend.hadoop.clustering.cluster.Canopy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by antoine on 12/05/14.
 */
public class ClusterFilterReducer extends
        Reducer<Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable> {

    private long minObservations;
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ClusterFilterReducer.class);

    public static final String COUNTER = "data";
    public static final String COUNTER_CANOPY = "canopies";

    @Override
    protected void setup(
            Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        minObservations = conf.getLong(Canopy.MIN_OBSERVATIONS, 1);
    }

    @Override
    protected void reduce(Text key,
                          Iterable<ArrayPrimitiveWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        long count = 0;
        ArrayPrimitiveWritable center = null;
        for (ArrayPrimitiveWritable value : values) {
            if (center == null) {
                center = value;
            }
            count++;
        }

        // Make sure we have enough data points
        if (count < minObservations) {
            LOGGER.warn(
                    "Cluster {} rejected - " +
                            "Not enough data points ({} < MIN_OBSERVATIONS)",
                    Arrays.toString((int[]) center.get()), count);
            return;
        }

        context.getCounter(COUNTER, COUNTER_CANOPY).increment(1L);
        context.write(key, center);

    }
}
