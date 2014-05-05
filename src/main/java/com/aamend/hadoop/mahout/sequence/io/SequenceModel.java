package com.aamend.hadoop.mahout.sequence.io;

import org.apache.hadoop.io.Writable;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public interface SequenceModel<O> extends Writable {

    /**
     * Return the probability that the observation is described by this model
     *
     * @param x an Observation from the posterior
     * @return the probability that x is in the receiver
     */
    double pdf(O x);

    /**
     * Observe the given observation, retaining information about it
     *
     * @param x an Observation from the posterior
     */
    void observe(O x);

    /**
     * Return the number of observations that this model has seen since its
     * parameters were last computed
     *
     * @return a long
     */
    long getNumObservations();

}
