package com.aamend.hadoop.mahout.sequence.cluster;

import com.aamend.hadoop.mahout.sequence.distance.SequenceDistanceMeasure;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public final class SequenceCanopyConfigKeys {

    private SequenceCanopyConfigKeys() {
    }

    public static final String T1_KEY = "cluster.t1";
    public static final String T2_KEY = "cluster.t2";
    public static final String DISTANCE_MEASURE_KEY = "cluster.measure";
    public static final String MAX_DISTANCE_MEASURE = "cluster.max.measure";

    public static SequenceDistanceMeasure configureSequenceDistanceMeasure(
            Configuration conf) throws IOException {

        // Retrieve params fom configuration
        float t1 = conf.getFloat(T1_KEY, 1.0f);
        float t2 = conf.getFloat(T2_KEY, 0.8f);

        SequenceDistanceMeasure measure;
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

        // Configure distance measure
        measure.configure(conf);
        return measure;
    }
}
