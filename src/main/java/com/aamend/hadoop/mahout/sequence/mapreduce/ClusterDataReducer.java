package com.aamend.hadoop.mahout.sequence.mapreduce;

import com.aamend.hadoop.mahout.sequence.cluster.CanopyConfigKeys;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by antoine on 12/05/14.
 */
public class ClusterDataReducer extends
        Reducer<Text, ArrayPrimitiveWritable, Text, Text> {

    private int minObservations;
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ClusterDataReducer.class);

    @Override
    protected void setup(
            Context context)
            throws IOException, InterruptedException {
        minObservations =
                context.getConfiguration().getInt(CanopyConfigKeys.MIN_OBS, 10);
    }

    @Override
    protected void reduce(Text key,
                          Iterable<ArrayPrimitiveWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        List<ArrayPrimitiveWritable> list =
                new ArrayList<ArrayPrimitiveWritable>();
        for (ArrayPrimitiveWritable value : values) {
            list.add(value);

        }

        // Make sure we have enough data points
        if (list.size() < minObservations) {
            LOGGER.warn(
                    "Cluster {} rejected - Not enough data points ({} < CF)",
                    key.toString(), list.size());
            context.getCounter(ClusterDataMapper.COUNTER,
                    ClusterDataMapper.COUNTER_REJECTED_CLUSTER).increment(1L);
            context.getCounter(ClusterDataMapper.COUNTER,
                    ClusterDataMapper.COUNTER_NON_CLUSTERED)
                    .increment(list.size());
            return;
        }

        // Output Cluster ID and Sequence
        for (ArrayPrimitiveWritable value : list) {
            context.getCounter(ClusterDataMapper.COUNTER,
                    ClusterDataMapper.COUNTER_CLUSTERED).increment(1L);
            context.write(key, new Text(Arrays.toString((int[]) value.get())));
        }
    }
}
