package com.aamend.hadoop.clustering.mapreduce;

import com.aamend.hadoop.clustering.cluster.Canopy;
import com.aamend.hadoop.clustering.cluster.Cluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClusterCreateReducer extends Reducer<Text, Cluster, Text, Cluster> {

    private static final Text KEY = new Text("canopies");
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterCreateReducer.class);
    public static final String COUNTER = "data";
    public static final String COUNTER_CANOPY = "canopies";

    private boolean lastIteration;
    private long minObservations;

    @Override
    protected void setup(Context context){
        Configuration conf = context.getConfiguration();
        minObservations = conf.getLong(Canopy.MIN_OBSERVATIONS, 1);
        lastIteration = conf.getBoolean(Canopy.LAST_ITERATION, false);
    }

    @Override
    protected void reduce(Text key, Iterable<Cluster> values, Context context)
            throws IOException, InterruptedException {

        // Try to find a center that could minimize all data points
        List<int[]> points = new ArrayList<int[]>();
        Cluster cluster = null;
        for (Cluster value : values) {
            if (cluster == null) {
                cluster = value;
            } else {
                cluster.observe(value.getObservations());
            }
            points.add(value.getCenter());
        }

        if(lastIteration){
            if(points.size() < minObservations){
                LOGGER.warn("Cluster {} rejected, not enough data points", cluster.asFormattedString());
            }
        }

        LOGGER.info("Minimizing distance across {} data points in cluster {}", points.size(), key.toString());
        cluster.computeCenter(points);
        context.getCounter(COUNTER, COUNTER_CANOPY).increment(1L);
        context.write(KEY, cluster);

    }

}