package com.aamend.hadoop.clustering.distance;

import com.aamend.hadoop.clustering.cluster.Canopy;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
@RunWith(JUnit4.class)
public class LevenshteinDistanceMeasureTest {

    private static Logger LOGGER = LoggerFactory.getLogger(LevenshteinDistanceMeasureTest.class);
    private DecimalFormat df = new DecimalFormat("###.##");

    @Test
    public void testDistance() {

        LevenshteinDistanceMeasure measure = new LevenshteinDistanceMeasure();
        measure.configure(new Configuration());

        int[] seq1 = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int[] seq2 = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        double dist = measure.distance(seq1, seq2);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(0.0d, round(dist));

        // 2 substitution
        int[] seq3 = new int[]{15, 2, 3, 4, 15, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq3);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(0.20d, round(dist));

        // 2 insertions
        int[] seq4 = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
        dist = measure.distance(seq1, seq4);
        LOGGER.info("Distance is {}", round(dist));
        Assert.assertEquals(0.17d, round(dist));

        // 2 deletions
        int[] seq5 = new int[]{2, 3, 4, 5, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq5);
        LOGGER.info("Distance is {}", round(dist));
        Assert.assertEquals(0.20d, round(dist));

    }

    @Test
    public void testDistanceThreshold() {

        LevenshteinDistanceMeasure measure = new LevenshteinDistanceMeasure();
        Configuration conf = new Configuration();
        conf.setFloat(Canopy.MAX_DISTANCE, 0.18f);
        measure.configure(conf);

        int[] seq1 = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int[] seq2 = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        double dist = measure.distance(seq1, seq2);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(0.0d, round(dist));

        // 2 substitution
        int[] seq3 = new int[]{15, 2, 3, 4, 15, 6, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq3);
        LOGGER.info("Distance is {}", dist);
        Assert.assertEquals(1.0d, round(dist));

        // 2 insertions
        int[] seq4 = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
        dist = measure.distance(seq1, seq4);
        LOGGER.info("Distance is {}", round(dist));
        Assert.assertEquals(0.17d, round(dist));

        // 2 deletions
        int[] seq5 = new int[]{2, 3, 4, 5, 7, 8, 9, 10};
        dist = measure.distance(seq1, seq5);
        LOGGER.info("Distance is {}", round(dist));
        Assert.assertEquals(1.0d, round(dist));
    }

    private double round(double val) {
        return Double.valueOf(df.format(val));
    }

}
