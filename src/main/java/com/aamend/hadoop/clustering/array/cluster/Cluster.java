package com.aamend.hadoop.clustering.array.cluster;

import com.aamend.hadoop.clustering.array.io.SequenceModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public interface Cluster extends SequenceModel<ArrayPrimitiveWritable> {

    // default directory for all clustered points
    String CLUSTERED_POINTS_DIR = "clustered-points";

    // default directory for output of clusters per iteration
    String CLUSTERS_TMP_DIR = "clusters-tmp-";

    // final directory for output of clusters per iteration
    String CLUSTERS_FINAL_DIR = "clusters-final";


    int getId();

    int[] getCenter();

    long getObservations();

    String asFormatString();

    void configure(Configuration job);

}
