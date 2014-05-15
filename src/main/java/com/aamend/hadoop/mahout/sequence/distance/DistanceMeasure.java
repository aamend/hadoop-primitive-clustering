package com.aamend.hadoop.mahout.sequence.distance;

import org.apache.hadoop.conf.Configuration;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public interface DistanceMeasure {

    double distance(int[] seq1, int[] seq2);

    void configure(Configuration conf);

}
