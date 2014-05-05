package com.aamend.hadoop.mahout.sequence.mapreduce;

import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopy;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopyClusterBuilder;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopyConfigKeys;
import com.aamend.hadoop.mahout.sequence.io.SequenceWritable;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class CanopyCreateReducer
        extends
        Reducer<Text, SequenceWritable, Text, SequenceWritable> {

    private final Collection<SequenceCanopy> canopies = Lists.newArrayList();
    private SequenceCanopyClusterBuilder builder;
    public static final Text KEY = new Text("canopies");
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CanopyCreateReducer.class);

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        builder = SequenceCanopyConfigKeys
                .configureClusterBuilder(context.getConfiguration());
    }

    @Override
    protected void reduce(Text key, Iterable<SequenceWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        for (SequenceWritable value : values) {
            int[] point = value.get();
            List<String> centers = builder.addPointToCanopies(point, canopies);
            // Output data with clusterID
            for (String center : centers) {
                KEY.set(center);
                context.write(KEY, value);
            }
        }
    }
}
