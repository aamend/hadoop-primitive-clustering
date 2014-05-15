package com.aamend.hadoop.mahout.sequence.distance;

import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.Set;

/**
 * Author: antoine.amend@gmail.com
 * Date: 10/03/14
 */
public class TanimotoDistanceMeasure
        implements DistanceMeasure {

    @Override
    public double distance(int[] seq1, int[] seq2) {

        Set<Integer> union = new HashSet<Integer>();
        for (int i : seq1) {
            union.add(i);
        }

        double intersection = 0.0d;
        for (int j : seq2) {
            if (union.contains(j)) {
                intersection++;
            } else {
                union.add(j);
            }
        }
        return 1 - intersection / union.size();
    }

    @Override
    public void configure(Configuration config) {
        // Nothing to do
    }
}
