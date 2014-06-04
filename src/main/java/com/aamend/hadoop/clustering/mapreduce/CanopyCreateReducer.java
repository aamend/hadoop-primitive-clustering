package com.aamend.hadoop.clustering.mapreduce;

import com.aamend.hadoop.clustering.cluster.Canopy;
import com.aamend.hadoop.clustering.cluster.CanopyWritable;
import com.aamend.hadoop.clustering.cluster.Cluster;
import com.aamend.hadoop.clustering.distance.DistanceMeasure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CanopyCreateReducer extends Reducer<Text, CanopyWritable, Text, CanopyWritable> {

    private static final Text KEY = new Text("canopies");
    private static final Logger LOGGER = LoggerFactory.getLogger(CanopyCreateReducer.class);
    public static final String COUNTER = "data";
    public static final String COUNTER_CANOPY = "canopies";
    public static final String COUNTER_REJECTED_CANOPY = "canopies.rejected";

    private boolean lastIteration;
    private long minObservations;
    private DistanceMeasure measure;
    private int nextCanopyId;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        minObservations = conf.getLong(Canopy.MIN_OBSERVATIONS, 1);
        lastIteration = conf.getBoolean(Canopy.LAST_ITERATION, false);
        measure = Canopy.configureMeasure(conf);
    }

    @Override
    protected void reduce(Text key, Iterable<CanopyWritable> values, Context context)
            throws IOException, InterruptedException {

        // Try to find a center that could minimize all data points
        List<int[]> points = new ArrayList<int[]>();

        long obs = 0L;
        Cluster clusterTemplate = null;
        for (CanopyWritable value : values) {
            if (clusterTemplate == null) {
                clusterTemplate = value.get();
            } else {
                obs += value.get().getNum();
                points.add(value.get().getCenter());
            }
        }

        // Increment number of observations for this cluster
        clusterTemplate.observe(obs);

        if (lastIteration) {
            if (clusterTemplate.getNum() < minObservations) {
                context.getCounter(COUNTER, COUNTER_REJECTED_CANOPY).increment(1L);
                return;
            }
        }

        LOGGER.info("Minimizing distance across {} data points in cluster center {}",
                points.size(), Arrays.toString(clusterTemplate.getCenter()));

        clusterTemplate.computeCenter(points, measure);
        nextCanopyId++;
        Cluster newCluster = new Canopy(nextCanopyId, clusterTemplate.getCenter(), clusterTemplate.getNum());
        context.getCounter(COUNTER, COUNTER_CANOPY).increment(1L);
        context.write(KEY, new CanopyWritable(newCluster));

    }

}