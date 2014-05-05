package com.aamend.hadoop.mahout.sequence.distance;

import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopyConfigKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.parameters.Parameter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Author: antoine.amend@gmail.com
 * Date: 10/03/14
 */
public class LevenshteinSequenceDistanceMeasure implements SequenceDistanceMeasure {

    private float maxLevDistance;

    private double getNormalizedDistance(Object[] s, Object[] t,
                                         int threshold) {

        if (s == null || t == null) {
            throw new IllegalArgumentException("Array must not be null");
        }

        if (threshold < 0) {
            throw new IllegalArgumentException(
                    "Threshold must not be negative");
        }

        /*
        This implementation only computes the distance if it's less than or equal to the
        threshold value, returning -1 if it's greater.
        */

        int n = s.length; // length of s
        int m = t.length; // length of t

        // if one string is empty, the edit distance is necessarily the length of the other
        if (n == 0) {
            return m <= threshold ? m : 1;
        } else if (m == 0) {
            return n <= threshold ? n : 1;
        }

        if (n > m) {
            // swap the two strings to consume less memory
            final Object[] tmp = s.clone();
            s = t;
            t = tmp;
            n = m;
            m = t.length;
        }

        int p[] = new int[n + 1]; // 'previous' cost array, horizontally
        int d[] = new int[n + 1]; // cost array, horizontally
        int _d[]; // placeholder to assist in swapping p and d

        // fill in starting table values
        final int boundary = Math.min(n, threshold) + 1;
        for (int i = 0; i < boundary; i++) {
            p[i] = i;
        }
        // these fills ensure that the value above the rightmost entry of our
        // stripe will be ignored in following loop iterations
        Arrays.fill(p, boundary, p.length, Integer.MAX_VALUE);
        Arrays.fill(d, Integer.MAX_VALUE);

        // iterates through t
        for (int j = 1; j <= m; j++) {
            final Object t_j = t[j - 1]; // jth character of t
            d[0] = j;

            // compute stripe indices, constrain to array size
            final int min = Math.max(1, j - threshold);
            final int max = (j > Integer.MAX_VALUE - threshold) ? n :
                    Math.min(n, j + threshold);

            // the stripe may lead off of the table if s and t are of different sizes
            if (min > max) {
                return 1.0;
            }

            // ignore entry left of leftmost
            if (min > 1) {
                d[min - 1] = Integer.MAX_VALUE;
            }

            // iterates through [min, max] in s
            for (int i = min; i <= max; i++) {
                if (s[i - 1].equals(t_j)) {
                    // diagonally left and up
                    d[i] = p[i - 1];
                } else {
                    // 1 + minimum of cell to the left, to the top, diagonally left and up
                    d[i] = 1 + Math.min(Math.min(d[i - 1], p[i]), p[i - 1]);
                }
            }

            // copy current distance counts to 'previous row' distance counts
            _d = p;
            p = d;
            d = _d;
        }

        // if p[n] is greater than the threshold, there's no guarantee on it being the correct
        // distance
        if (p[n] <= threshold) {
            double lev = (double) p[n] / (Math.max(s.length, t.length));
            return lev;
        } else {
            return 1.0;
        }
    }

    @Override
    public double distance(Object[] seq1, Object[] seq2) {
        // Compute Levenshtein threshold
        int maxDistance = Math.max(seq1.length, seq2.length);
        int threshold = (int) Math.ceil(maxDistance * maxLevDistance);
        // Compute normalized distance
        return getNormalizedDistance(seq1, seq2, threshold);
    }

    @Override
    public Collection<Parameter<?>> getParameters() {
        return Collections.emptyList();
    }

    @Override
    public void createParameters(String prefix, Configuration conf) {
        // nothing to do
    }

    @Override
    public void configure(Configuration config) {
        maxLevDistance =
                config.getFloat(SequenceCanopyConfigKeys.MAX_DISTANCE_MEASURE,
                        1.0f);
    }
}
