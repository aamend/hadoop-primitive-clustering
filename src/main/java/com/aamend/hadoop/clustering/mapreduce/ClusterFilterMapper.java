package com.aamend.hadoop.clustering.mapreduce;

import com.aamend.hadoop.clustering.cluster.Canopy;
import com.aamend.hadoop.clustering.cluster.Cluster;
import com.aamend.hadoop.clustering.distance.DistanceMeasure;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Created by antoine on 12/05/14.
 */
public class ClusterFilterMapper extends Mapper<Text, Cluster, IntWritable, Cluster> {

    private static final IntWritable KEY = new IntWritable();
    private List<Cluster> clusters;
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterFilterMapper.class);

    @Override
    protected void map(Text key, Cluster value, Context context) throws IOException, InterruptedException {

        if (value.getObservations() < )
        // Get distance from that point to any cluster center
        double[] pdf = new double[clusters.size()];
        for (int i = 0; i < clusters.size(); i++) {
            Cluster cluster = clusters.get(i);
            pdf[i] = cluster.pdf((int[]) value.get());
        }

        // Get the cluster with smallest distance to that point
        double maxSimilarity = pdf[0];
        int maxSimilarityId = 0;
        for (int i = 1; i < pdf.length; i++) {
            if (pdf[i] > maxSimilarity) {
                maxSimilarity = pdf[i];
                maxSimilarityId = i;
            }
        }

        if (maxSimilarity == 0.0d) {
            // Point could not be added to any cluster
            return;
        }

        // Point has been added to that cluster
        Cluster cluster = clusters.get(maxSimilarityId);
        KEY.set(String.valueOf(cluster.getId()));
        context.write(KEY, new ArrayPrimitiveWritable(cluster.getCenter()));

    }
}
