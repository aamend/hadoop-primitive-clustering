package com.aamend.hadoop.mahout.sequence.mapreduce;

import com.aamend.hadoop.mahout.sequence.cluster.Canopy;
import com.aamend.hadoop.mahout.sequence.cluster.CanopyConfigKeys;
import com.aamend.hadoop.mahout.sequence.cluster.Cluster;
import com.aamend.hadoop.mahout.sequence.distance.DistanceMeasure;
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
public class ClusterFilterMapper extends
        Mapper<Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable> {

    private static final Text KEY = new Text();
    private List<Cluster> clusters;
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ClusterFilterMapper.class);

    private DistanceMeasure measure;

    @Override
    protected void setup(Context context) throws IOException {

        // Configure distance measure
        Configuration conf = context.getConfiguration();
        measure = CanopyConfigKeys.configureMeasure(conf);
        clusters = Lists.newArrayList();

        for (URI uri : DistributedCache.getCacheFiles(conf)) {

            if (uri.getPath().contains(Cluster.CLUSTERS_TMP_DIR)) {
                LOGGER.info("Loading file [{}] from distributed cache", uri);
                // Read canopies
                SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                        SequenceFile.Reader.file(new Path(uri)));
                WritableComparable key = (WritableComparable) ReflectionUtils
                        .newInstance(reader.getKeyClass(), conf);
                ArrayPrimitiveWritable value =
                        (ArrayPrimitiveWritable) ReflectionUtils
                                .newInstance(reader.getValueClass(), conf);

                int i = 0;
                while (reader.next(key, value)) {
                    i++;
                    Cluster cluster = new Canopy((int[]) value.get(), i,
                            measure);
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
    protected void map(Text key, ArrayPrimitiveWritable value, Context context)
            throws IOException, InterruptedException {

        // Get distance from that point to any cluster center
        double[] pdf = new double[clusters.size()];
        for (int i = 0; i < clusters.size(); i++) {
            Cluster cluster = clusters.get(i);
            pdf[i] = cluster.pdf(value);
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
        Canopy cluster = (Canopy) clusters.get(maxSimilarityId);
        KEY.set(cluster.getIdentifier());
        context.write(KEY, new ArrayPrimitiveWritable(cluster.getCenter()));

    }
}
