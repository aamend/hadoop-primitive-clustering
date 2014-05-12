package com.aamend.hadoop.mahout.sequence.mapreduce;

import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopy;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopyConfigKeys;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCluster;
import com.aamend.hadoop.mahout.sequence.distance.SequenceDistanceMeasure;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SequenceCanopyMergeCombiner
        extends
        Reducer<Text, ArrayPrimitiveWritable, Text, SequenceCluster> {

    private List<int[]> clustersCenters;
    private static final Text KEY = new Text("dummy");
    public static final String COUNTER = "data";
    public static final String COUNTER_CANOPY = "canopies";
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SequenceCanopyMergeCombiner.class);

    private SequenceDistanceMeasure measure;

    @Override
    protected void setup(Context context) throws IOException {
        clustersCenters = new ArrayList<int[]>();
        measure = SequenceCanopyConfigKeys
                .configureSequenceDistanceMeasure(context.getConfiguration());
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

        LOGGER.info("Minimizing center for {} data points in cluster {}",
                points.size(), key.toString());

        double[] averages = new double[points.size()];
        for (int i = 0; i < points.size(); i++) {
            double average = 0.0d;
            // Consider center i
            // Compute distance to other points
            for (int j = 0; j < points.size(); j++) {
                if (j != i) {
                    average += measure.distance(points.get(i),
                            points.get(j));
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

        clustersCenters.add(points.get(minIdx));
        context.getCounter(COUNTER, COUNTER_CANOPY).increment(1L);
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        for (int i = 0; i < clustersCenters.size(); i++) {
            SequenceCluster cluster =
                    new SequenceCanopy(clustersCenters.get(i), i, measure);
            context.write(KEY, cluster);
        }
    }


}