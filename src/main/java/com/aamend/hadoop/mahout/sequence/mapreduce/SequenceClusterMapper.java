package com.aamend.hadoop.mahout.sequence.mapreduce;

import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopy;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopyConfigKeys;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCluster;
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
public class SequenceClusterMapper extends
        Mapper<Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable> {

    private static final Text KEY = new Text();
    private List<SequenceCanopy> clusters;
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SequenceClusterMapper.class);

    private float minPdf;

    public static final String COUNTER = "DATA";
    public static final String COUNTER_NON_CLUSTER = "non.clustered";
    public static final String COUNTER_CLUSTER = "clustered";

    @Override
    protected void setup(Context context) throws IOException {

        Configuration conf = context.getConfiguration();

        minPdf = conf.getFloat(SequenceCanopyConfigKeys.MIN_PDF, 1.0f);
        clusters = Lists.newArrayList();

        for (URI uri : DistributedCache.getCacheFiles(conf)) {

            if (uri.getPath().contains("part")) {
                LOGGER.info("Loading file [{}] from distributed cache", uri);
                // Read canopies
                SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                        SequenceFile.Reader.file(new Path(uri)));
                WritableComparable key = (WritableComparable) ReflectionUtils
                        .newInstance(reader.getKeyClass(), conf);
                SequenceCluster value =
                        (SequenceCluster) ReflectionUtils
                                .newInstance(reader.getValueClass(), conf);

                while (reader.next(key, value)) {
                    clusters.add((SequenceCanopy) value);
                }

                IOUtils.closeStream(reader);
            }
        }

        if (clusters.size() == 0) {
            throw new IOException(
                    "Could not find / load any canopy. Check distributed cache");
        }

        LOGGER.info("Loaded {} canopies", clusters.size());
    }

    @Override
    protected void map(Text key, ArrayPrimitiveWritable value, Context context)
            throws IOException, InterruptedException {

        // Get distance from that point to any cluster center
        double[] distances = new double[clusters.size()];
        for (int i = 0; i < clusters.size(); i++) {
            SequenceCluster cluster = clusters.get(i);
            distances[i] = cluster.pdf(value);
        }

        // Get the cluster with smallest distance to that point
        double min = Double.MAX_VALUE;
        int minIdx = 0;
        for (int i = 0; i < clusters.size(); i++) {
            if (distances[i] < min) {
                min = distances[i];
                minIdx = i;
            }
        }

        if (min >= minPdf) {
            // Point could not be added to any cluster
            context.getCounter(COUNTER, COUNTER_NON_CLUSTER).increment(1L);
            return;
        }

        // Point has been added to that cluster
        SequenceCanopy cluster = clusters.get(minIdx);
        context.getCounter(COUNTER, COUNTER_CLUSTER).increment(1L);

        KEY.set(cluster.getIdentifier());
        context.write(KEY, value);

    }
}
