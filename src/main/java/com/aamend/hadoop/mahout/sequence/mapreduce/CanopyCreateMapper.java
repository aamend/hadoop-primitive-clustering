package com.aamend.hadoop.mahout.sequence.mapreduce;

import com.aamend.hadoop.mahout.sequence.cluster.SequenceAbstractCluster;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopy;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopyConfigKeys;
import com.aamend.hadoop.mahout.sequence.distance.SequenceDistanceMeasure;
import com.aamend.hadoop.mahout.sequence.io.SequenceWritable;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class CanopyCreateMapper extends
        Mapper<Text, SequenceWritable, Text,
                SequenceWritable> {

    private float t1;
    private float t2;
    private int nextCanopyId;
    private SequenceDistanceMeasure measure;
    private Collection<SequenceCanopy> canopies = Lists.newArrayList();

    private static final Text KEY = new Text();
    private static final String COUNTER = "data";
    private static final String COUNTER_CANOPY = "canopies";
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CanopyCreateMapper.class);

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        // Retrieve params fom configuration
        Configuration conf = context.getConfiguration();
        t1 = conf.getFloat(SequenceCanopyConfigKeys.T1_KEY, 1.0f);
        t2 = conf.getFloat(SequenceCanopyConfigKeys.T2_KEY, 0.8f);

        // Configure distance measure
        try {
            Class<?> clazz = Class.forName(
                    conf.get(SequenceCanopyConfigKeys.DISTANCE_MEASURE_KEY));
            Object obj = clazz.newInstance();
            measure = (SequenceDistanceMeasure) obj;
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        } catch (InstantiationException e) {
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }

        measure.configure(conf);

    }

    @Override
    protected void map(Text key,
                       SequenceWritable value, Context context)
            throws IOException, InterruptedException {

        // Add this point to canopies
        int[] point = value.get();
        boolean newCanopy = addPointToCanopies(point, context);
        if (newCanopy) {
            context.getCounter(COUNTER, COUNTER_CANOPY).increment(1L);
        }
    }

    public boolean addPointToCanopies(int[] point,
                                      Context context)
            throws IOException, InterruptedException {

        boolean stronglyBound = false;
        for (SequenceCanopy sequenceCanopy : canopies) {
            double dist = measure.distance(sequenceCanopy.getCenter(), point);
            if (dist < t1) {
                sequenceCanopy.observe(point);
                String key = MD5Hash.digest(SequenceAbstractCluster
                        .formatSequence(sequenceCanopy.getCenter())).toString();
                KEY.set(key);
                context.write(KEY,
                        new SequenceWritable(sequenceCanopy.getCenter()));
            }
            stronglyBound = stronglyBound || dist < t2;
        }
        if (!stronglyBound) {
            nextCanopyId++;
            canopies.add(new SequenceCanopy(point, nextCanopyId, measure));
            String key = MD5Hash.digest(SequenceAbstractCluster
                    .formatSequence(point)).toString();
            KEY.set(key);
            context.write(KEY, new SequenceWritable(point));
        }

        return !stronglyBound;
    }
}
