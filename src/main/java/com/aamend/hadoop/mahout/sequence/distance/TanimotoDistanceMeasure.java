package com.aamend.hadoop.mahout.sequence.distance;

import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.Set;

/**
 * Author: antoine.amend@gmail.com
 * Date: 10/03/14
 */
public class TanimotoDistanceMeasure implements DistanceMeasure {

    @Override
    public double distance(int[] seq1, int[] seq2) {

        Set<Integer> common = new HashSet<Integer>();
        for (int i : seq1) {
            common.add(i);
        }

        double intersection = 0.0d;
        for (int j : seq2) {
            if (common.contains(j)) {
                intersection++;
            } else {
                common.add(j);
            }
        }
        double union = common.size();
        common.clear();
        return 1 - intersection / union;
    }

    @Override
    public void configure(Configuration conf) {
        // Nothing to do
    }
}
