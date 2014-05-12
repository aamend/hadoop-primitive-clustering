package com.aamend.hadoop.mahout.sequence.cluster;

import com.aamend.hadoop.mahout.sequence.io.SequenceModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public interface Cluster extends SequenceModel<ArrayPrimitiveWritable> {

    // default directory for all clustered points
    String CLUSTERED_POINTS_DIR = "clusteredPoints";

    // default directory for initial clusters to prime iterative clustering
    // algorithms
    String INITIAL_CLUSTERS_DIR = "clusters-0";

    // default directory for output of clusters per iteration
    String CLUSTERS_DIR = "clusters-";

    // default suffix for output of clusters for final iteration
    String FINAL_ITERATION_SUFFIX = "-final";

    /**
     * Get the id of the Cluster
     *
     * @return a unique integer
     */
    int getId();

    /**
     * Get the "center" of the Cluster as an array
     *
     * @return an Integer Array
     */
    int[] getCenter();

    /**
     * Produce a custom, human-friendly, printable representation of the Cluster.
     *
     * @return a String
     */
    String asFormatString();

    /**
     * Configure sequence based on Hadoop configuration
     *
     * @param job
     */
    void configure(Configuration job);

}
