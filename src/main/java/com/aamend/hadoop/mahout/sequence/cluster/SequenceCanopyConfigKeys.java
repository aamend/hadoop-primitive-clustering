package com.aamend.hadoop.mahout.sequence.cluster;

import com.aamend.hadoop.mahout.sequence.distance.SequenceDistanceMeasure;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.ClassUtils;

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

    public static SequenceCanopyClusterBuilder configureClusterBuilder(
            Configuration conf) {

        // Retrieve params fom configuration
        float t1 = conf.getFloat(T1_KEY, 1.0f);
        float t2 = conf.getFloat(T2_KEY, 0.8f);
        SequenceDistanceMeasure measure = ClassUtils
                .instantiateAs(conf.get(DISTANCE_MEASURE_KEY),
                        SequenceDistanceMeasure.class);

        // Configure distance measure
        measure.configure(conf);

        // Return new builder instance
        return new SequenceCanopyClusterBuilder(measure, t1, t2);
    }

    public static SequenceDistanceMeasure configureSequenceDistanceMeasure(
            Configuration conf) {

        // Retrieve params fom configuration
        float t1 = conf.getFloat(T1_KEY, 1.0f);
        float t2 = conf.getFloat(T2_KEY, 0.8f);
        SequenceDistanceMeasure measure = ClassUtils
                .instantiateAs(conf.get(DISTANCE_MEASURE_KEY),
                        SequenceDistanceMeasure.class);

        // Configure distance measure
        measure.configure(conf);
        return measure;
    }
}
