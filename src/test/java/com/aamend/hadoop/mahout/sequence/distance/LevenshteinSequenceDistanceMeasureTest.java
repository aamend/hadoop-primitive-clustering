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
        SequenceLevenshteinDistanceMeasure measure = new
                SequenceLevenshteinDistanceMeasure();
        measure.configure(new Configuration());

        int[] seq1 = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int[] seq2 = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        double dist = measure.distance(seq1, seq2);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 0.0d);

        // 1 substitution
        int[] seq3 = new int[]{15, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq3);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 0.1d);

        // 1 insertion
        int[] seq4 = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq4);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 0.09090909090909091d);

        // 1 deletion
        int[] seq5 = new int[]{2, 3, 4, 5, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq5);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 0.1d);

    }

    @Test
    public void testDistanceThreshold() {
        SequenceLevenshteinDistanceMeasure measure = new
                SequenceLevenshteinDistanceMeasure();

        Configuration conf = new Configuration();
        conf.setFloat(SequenceCanopyConfigKeys.MAX_DISTANCE_MEASURE, 0.1f);
        measure.configure(conf);

        int[] seq1 = new int[]{1, 2, 3, 4, 5, 5, 7, 8, 9, 10};
        int[] seq2 = new int[]{1, 2, 3, 4, 5, 6, 6, 8, 9, 10};

        double dist = measure.distance(seq1, seq2);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 1.0d);

        // 1 substitution
        int[] seq3 = new int[]{15, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq3);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 1.0d);

        // 1 insertion
        int[] seq4 = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq4);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 0.18181818181818182d);

        // 1 deletion
        int[] seq5 = new int[]{2, 3, 4, 5, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq5);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(dist, 1.0d);

    }

}
