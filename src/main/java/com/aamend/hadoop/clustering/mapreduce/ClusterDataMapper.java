package com.aamend.hadoop.clustering.mapreduce;

import com.aamend.hadoop.clustering.cluster.Canopy;
import com.aamend.hadoop.clustering.cluster.CanopyWritable;
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
public class ClusterDataMapper extends
        Mapper<WritableComparable, ArrayPrimitiveWritable, IntWritable, ObjectWritable> {

    private static final IntWritable KEY = new IntWritable();
    public static final String COUNTER = "data";
    public static final String COUNTER_CLUSTERED = "clustered.points";
    public static final String COUNTER_NON_CLUSTERED = "non.clustered.points";
    public static final String CLUSTERS_FINAL_DIR_CONF = "cluster.final.dir_conf";

    private List<Cluster> clusters;
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ClusterDataMapper.class);

    private DistanceMeasure measure;
    private float minSimilarity;

    @Override
    protected void setup(Context context) throws IOException {

        // Configure distance measure
        Configuration conf = context.getConfiguration();
        measure = Canopy.configureMeasure(conf);
        minSimilarity = conf.getFloat(Canopy.MIN_SIMILARITY, 0.0f);
        clusters = Lists.newArrayList();

        for (URI uri : DistributedCache.getCacheFiles(conf)) {

            if (uri.getPath().contains(conf.get(ClusterDataMapper.CLUSTERS_FINAL_DIR_CONF))) {
                LOGGER.info("Loading file [{}] from distributed cache", uri);
                // Read canopies
                SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                        SequenceFile.Reader.file(new Path(uri)));
                WritableComparable key = (WritableComparable) ReflectionUtils
                        .newInstance(reader.getKeyClass(), conf);
                CanopyWritable value =
                        (CanopyWritable) ReflectionUtils
                                .newInstance(reader.getValueClass(), conf);

                int i = 0;
                while (reader.next(key, value)) {
                    i++;
                    Cluster cluster = value.get();
                    clusters.add(cluster);
                }

                IOUtils.closeStream(reader);
            }
        }

        if (clusters.size() == 0) {
            throw new IOException(
                    "Could not find / load any canopy. Check distributed cache");
        }

        LOGGER.info("Loaded {} clusters", clusters.size());
    }

    @Override
    protected void map(WritableComparable key, ArrayPrimitiveWritable value, Context context)
            throws IOException, InterruptedException {

        // Get distance from that point to any cluster center
        double[] pdf = new double[clusters.size()];
        for (int i = 0; i < clusters.size(); i++) {
            Cluster cluster = clusters.get(i);
            pdf[i] = cluster.pdf((int[]) value.get(), measure);
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

        if (maxSimilarity < minSimilarity) {
            // Point could not be added to any cluster
            context.getCounter(COUNTER, COUNTER_NON_CLUSTERED).increment(1L);
            return;
        }

        // Point has been added to that cluster
        context.getCounter(COUNTER, COUNTER_CLUSTERED).increment(1L);
        Cluster cluster = clusters.get(maxSimilarityId);

        KEY.set(cluster.getId());
        context.write(KEY, new ObjectWritable(key));

    }
}
