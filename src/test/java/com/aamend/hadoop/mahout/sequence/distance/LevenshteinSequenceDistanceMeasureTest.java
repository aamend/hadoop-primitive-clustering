package com.aamend.hadoop.mahout.sequence.distance;

import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopyConfigKeys;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by antoine on 05/05/14.
 */
@RunWith(JUnit4.class)
public class LevenshteinSequenceDistanceMeasureTest {

    private static Logger LOGGER = LoggerFactory.getLogger
            (LevenshteinSequenceDistanceMeasureTest.class);

    @Test
    public void testDistance() {
        LevenshteinSequenceDistanceMeasure measure = new
                LevenshteinSequenceDistanceMeasure();
        measure.configure(new Configuration());

        Object[] seq1 = new Object[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        Object[] seq2 = new Object[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        double dist = measure.distance(seq1, seq2);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 0.0d);

        // 1 substitution
        Object[] seq3 = new Object[]{15, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq3);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 0.1d);

        // 1 insertion
        Object[] seq4 = new Object[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq4);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 0.09090909090909091d);

        // 1 deletion
        Object[] seq5 = new Object[]{2, 3, 4, 5, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq5);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 0.1d);

    }

    @Test
    public void testDistanceThreshold() {
        LevenshteinSequenceDistanceMeasure measure = new
                LevenshteinSequenceDistanceMeasure();

        Configuration conf = new Configuration();
        conf.setFloat(SequenceCanopyConfigKeys.MAX_DISTANCE_MEASURE, 0.1f);
        measure.configure(conf);

        Object[] seq1 = new Object[]{1, 2, 3, 4, 5, 5, 7, 8, 9, 10};
        Object[] seq2 = new Object[]{1, 2, 3, 4, 5, 6, 6, 8, 9, 10};

        double dist = measure.distance(seq1, seq2);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 1.0d);

        // 1 substitution
        Object[] seq3 = new Object[]{15, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq3);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 1.0d);

        // 1 insertion
        Object[] seq4 = new Object[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq4);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 0.18181818181818182d);

        // 1 deletion
        Object[] seq5 = new Object[]{2, 3, 4, 5, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq5);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 1.0d);

    }

}
