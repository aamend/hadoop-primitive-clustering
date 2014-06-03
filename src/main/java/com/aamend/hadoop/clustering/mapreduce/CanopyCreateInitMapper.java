package com.aamend.hadoop.clustering.mapreduce;

import com.aamend.hadoop.clustering.cluster.Canopy;
import com.aamend.hadoop.clustering.cluster.CanopyWritable;
import com.aamend.hadoop.clustering.cluster.Cluster;
import com.aamend.hadoop.clustering.distance.DistanceMeasure;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class CanopyCreateInitMapper extends
        Mapper<WritableComparable, ArrayPrimitiveWritable, Text, CanopyWritable> {

    private float t1;
    private float t2;
    private int nextCanopyId;
    private DistanceMeasure measure;
    private Collection<Cluster> canopies = Lists.newArrayList();

    private static final Text KEY = new Text();
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CanopyCreateInitMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // Retrieve params fom configuration
        Configuration conf = context.getConfiguration();
        measure = Canopy.configureMeasure(conf);
        t1 = conf.getFloat(Canopy.CLUSTER_T1, 1.0f);
        t2 = conf.getFloat(Canopy.CLUSTER_T2, 0.8f);
    }

    @Override
    protected void map(WritableComparable key, ArrayPrimitiveWritable value, Context context) throws IOException, InterruptedException {

        int[] point = (int[]) value.get();
        boolean stronglyBound = false;
        for (Cluster canopy : canopies) {
            double dist = measure.distance(canopy.getCenter(), point);
            if (dist < t1) {
                KEY.set(Arrays.toString(canopy.getCenter()));
                Cluster newCluster;
                if(dist < t2){
                    newCluster = new Canopy(canopy.getId(), point, 1L);
                    LOGGER.debug("Adding (T2) {} to Cluster center {}", Arrays.toString((int[]) value.get()),
                            Arrays.toString(canopy.getCenter()));
                } else {
                    newCluster = new Canopy(canopy.getId(), point, 0L);
                    LOGGER.debug("Adding (T1) {} to Cluster center {}", Arrays.toString((int[]) value.get()),
                            Arrays.toString(canopy.getCenter()));
                }
                context.write(KEY, new CanopyWritable(newCluster));
            }
            stronglyBound = stronglyBound || dist < t2;
        }
        if (!stronglyBound) {
            nextCanopyId++;
            Cluster canopy = new Canopy(nextCanopyId, point, 1L);
            LOGGER.debug("Creating a new Cluster {}", canopy.asFormattedString());
            canopies.add(canopy);
            KEY.set(Arrays.toString(canopy.getCenter()));
            context.write(KEY, new CanopyWritable(canopy));
        }

    }
}
