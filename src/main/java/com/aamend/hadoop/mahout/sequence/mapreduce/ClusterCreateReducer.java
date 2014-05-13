package com.aamend.hadoop.mahout.sequence.mapreduce;

import com.aamend.hadoop.mahout.sequence.cluster.CanopyConfigKeys;
import com.aamend.hadoop.mahout.sequence.distance.DistanceMeasure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClusterCreateReducer
        extends
        Reducer<Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable> {

    private static final Text KEY = new Text("canopies");
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ClusterCreateReducer.class);
    public static final String COUNTER = "data";
    public static final String COUNTER_CANOPY = "canopies";

    private boolean lastIteration;
    private int clusterFilter;
    private DistanceMeasure measure;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        measure = CanopyConfigKeys.configureMeasure(conf);
        clusterFilter = conf.getInt(CanopyConfigKeys.CF_KEY, 10);
        lastIteration = conf.getBoolean(CanopyConfigKeys.LAST_IT_KEY, false);
    }

    @Override
    protected void reduce(Text key, Iterable<ArrayPrimitiveWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        // Try to find a center that could minimize all data points
        List<int[]> points = new ArrayList<int[]>();
        for (ArrayPrimitiveWritable value : values) {
            points.add((int[]) value.get());
        }

        if (lastIteration) {
            if (points.size() < clusterFilter) {
                LOGGER.warn(
                        "Cluster {} rejected - not enough data points (<{})",
                        key.toString(), clusterFilter);
            }
        }

        LOGGER.info("Minimizing distance across {} data points in cluster {}",
                points.size(), key.toString());

        double[] averages = new double[points.size()];
        for (int i = 0; i < points.size(); i++) {
            double average = 0.0d;
            // Consider center i
            // Compute distance to other points
            for (int j = 0; j < points.size(); j++) {
                if (j != i) {
                    average += measure.distance(points.get(i), points.get(j));
                }
            }
            averages[i] = average / (points.size() - 1);
        }

        double min = Double.MAX_VALUE;
        int minIdx = 0;
        for (int i = 0; i < averages.length; i++) {
            double average = averages[i];
            if (average < min) {
                min = average;
                minIdx = i;
            }
        }

        context.getCounter(COUNTER, COUNTER_CANOPY).increment(1L);
        context.write(KEY, new ArrayPrimitiveWritable(points.get(minIdx)));

    }

}