package com.aamend.hadoop.clustering.array.mapreduce;

import com.aamend.hadoop.clustering.array.cluster.Canopy;
import com.aamend.hadoop.clustering.array.cluster.CanopyConfigKeys;
import com.aamend.hadoop.clustering.array.distance.DistanceMeasure;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class ClusterCreateMapper extends
        Mapper<Text, ArrayPrimitiveWritable, Text,
                ArrayPrimitiveWritable> {

    private float t1;
    private float t2;
    private int nextCanopyId;
    private DistanceMeasure measure;
    private Collection<Canopy> canopies = Lists.newArrayList();

    private static final Text KEY = new Text();
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ClusterCreateMapper.class);

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        // Retrieve params fom configuration
        Configuration conf = context.getConfiguration();
        measure = CanopyConfigKeys.configureMeasure(conf);
        t1 = conf.getFloat(CanopyConfigKeys.CLUSTER_T1, 1.0f);
        t2 = conf.getFloat(CanopyConfigKeys.CLUSTER_T2, 0.8f);

        LOGGER.info("Configuring distance with T1, T2 = {}, {}", t1, t2);

    }

    @Override
    protected void map(Text key,
                       ArrayPrimitiveWritable value, Context context)
            throws IOException, InterruptedException {

        // Add this point to canopies
        int[] point = (int[]) value.get();
        addPointToCanopies(point, context);
    }

    public void addPointToCanopies(int[] point,
                                   Context context)
            throws IOException, InterruptedException {

        boolean stronglyBound = false;
        for (Canopy canopy : canopies) {
            double dist = measure.distance(canopy.getCenter(), point);
            if (dist < t1) {
                canopy.observe(point);
                KEY.set(Arrays.toString(canopy.getCenter()));
                context.write(KEY, new ArrayPrimitiveWritable(point));
            }
            stronglyBound = stronglyBound || dist < t2;
        }
        if (!stronglyBound) {
            nextCanopyId++;
            Canopy canopy = new Canopy(point, nextCanopyId, measure);
            canopies.add(canopy);
            KEY.set(Arrays.toString(canopy.getCenter()));
            context.write(KEY, new ArrayPrimitiveWritable(point));
        }
    }
}
