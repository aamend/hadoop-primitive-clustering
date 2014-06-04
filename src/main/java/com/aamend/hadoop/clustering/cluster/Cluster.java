package com.aamend.hadoop.clustering.cluster;

import com.aamend.hadoop.clustering.distance.DistanceMeasure;

import java.util.List;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public interface Cluster {

    // default directory for output of clusters per iteration
    String CLUSTERS_TMP_DIR = "clusters-tmp-";

    double pdf(int[] x, DistanceMeasure measure);

    void observe(long number);

    void computeCenter(List<int[]> centers, DistanceMeasure measure);

    int getId();

    int[] getCenter();

    long getNum();

    String asFormattedString();

}
