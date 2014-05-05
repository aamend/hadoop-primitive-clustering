package com.aamend.hadoop.mahout.sequence.cluster;

import com.aamend.hadoop.mahout.sequence.distance.SequenceDistanceMeasure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public class SequenceCanopyClusterBuilder {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SequenceCanopyClusterBuilder.class);

    private int nextCanopyId;
    private double t1;
    private double t2;
    private SequenceDistanceMeasure measure;

    public SequenceCanopyClusterBuilder(SequenceDistanceMeasure measure,
                                        float t1,
                                        float t2) {
        this.t1 = t1;
        this.t2 = t2;
        this.measure = measure;
        LOGGER.info(
                "Configure cluster builder with Distance " +
                        "measure [" + measure.getClass().toString() +
                        "] and T1," +
                        "T2 resp. {}, {}", t1, t2);
    }

    public boolean addPointToCanopies(Object[] point,
                                      Collection<SequenceCanopy> canopies) {

        boolean stronglyBound = false;
        for (SequenceCanopy sequenceCanopy : canopies) {
            double dist = measure.distance(sequenceCanopy.getCenter(), point);
            if (dist < t1) {
                sequenceCanopy.observe(point);
            }
            stronglyBound = stronglyBound || dist < t2;
        }
        if (!stronglyBound) {
            nextCanopyId++;
            canopies.add(new SequenceCanopy(point, nextCanopyId, measure));
        }
        return stronglyBound;
    }

}
