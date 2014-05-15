package com.aamend.hadoop.clustering.cluster;

import com.aamend.hadoop.clustering.distance.DistanceMeasure;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public final class CanopyConfigKeys {

    public static final String MIN_OBSERVATIONS = "min.cluster.observations";
    public static final String MIN_SIMILARITY = "min.cluster.similarity";
    public static final String CLUSTER_T1 = "cluster.t1";
    public static final String CLUSTER_T2 = "cluster.t2";
    public static final String CLUSTER_MEASURE = "cluster.measure";
    public static final String MAX_DISTANCE = "cluster.max.measure";

    public static DistanceMeasure configureMeasure(
            Configuration conf) throws IOException {

        DistanceMeasure measure;
        String className = conf.get(CanopyConfigKeys.CLUSTER_MEASURE);
        if (StringUtils.isEmpty(className)) {
            throw new IllegalArgumentException(
                    "Distance measure is empty. " +
                            "It should be specified from Hadoop configuration");
        }
        try {
            Class<?> clazz = Class.forName(className);
            Object obj = clazz.newInstance();
            measure = (DistanceMeasure) obj;
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
