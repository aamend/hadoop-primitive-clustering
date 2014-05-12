package com.aamend.hadoop.mahout.sequence.cluster;

import com.aamend.hadoop.mahout.sequence.distance.SequenceDistanceMeasure;
import com.aamend.hadoop.mahout.sequence.distance.SequenceLevenshteinDistanceMeasure;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public final class SequenceCanopyConfigKeys {

    private SequenceCanopyConfigKeys() {
    }

    public static final String MIN_PDF = "min.cluster.pdf";
    public static final String T1_KEY = "cluster.t1";
    public static final String T2_KEY = "cluster.t2";
    public static final String DISTANCE_MEASURE_KEY = "cluster.measure";
    public static final String MAX_DISTANCE_MEASURE = "cluster.max.measure";
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SequenceDistanceMeasure.class);

    public static SequenceDistanceMeasure configureSequenceDistanceMeasure(
            Configuration conf) throws IOException {

        SequenceDistanceMeasure measure;
        String className =
                conf.get(SequenceCanopyConfigKeys.DISTANCE_MEASURE_KEY);
        if (StringUtils.isEmpty(className)) {
            LOGGER.warn(
                    "Distance measure is empty, using Levenshtein distance");
            className = SequenceLevenshteinDistanceMeasure.class.getName();
        }
        try {
            Class<?> clazz = Class.forName(className);
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
