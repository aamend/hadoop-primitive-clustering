package com.aamend.hadoop.mahout.sequence.mapreduce;

import com.aamend.hadoop.mahout.sequence.cluster.SequenceAbstractCluster;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopy;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopyClusterBuilder;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopyConfigKeys;
import com.aamend.hadoop.mahout.sequence.io.SequenceWritable;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class CanopyCreateMapper extends
        Mapper<Text, SequenceWritable, Text,
                SequenceWritable> {

    private SequenceCanopyClusterBuilder builder;
    private final Collection<SequenceCanopy> canopies = Lists.newArrayList();
    private static final Text KEY = new Text();
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CanopyCreateMapper.class);

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        builder = SequenceCanopyConfigKeys
                .configureClusterBuilder(context.getConfiguration());
    }

    @Override
    protected void map(Text key,
                       SequenceWritable value, Context context)
            throws IOException, InterruptedException {

        // Add this point to canopies
        int[] point = value.get();
        List<String> centers = builder.addPointToCanopies(point, canopies);

        // Output data with clusterID
        for (String center : centers) {
            KEY.set(center);
            context.write(KEY, value);
            System.out.println(KEY.toString() + "\t" +
                    SequenceAbstractCluster
                            .formatSequence(value.get()));
        }
    }
}
