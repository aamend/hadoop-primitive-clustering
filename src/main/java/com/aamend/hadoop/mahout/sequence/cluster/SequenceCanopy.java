package com.aamend.hadoop.mahout.sequence.cluster;

import com.aamend.hadoop.mahout.sequence.distance.SequenceDistanceMeasure;
import com.aamend.hadoop.mahout.sequence.distance.SequenceDistanceMeasureCluster;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public class SequenceCanopy
        extends SequenceDistanceMeasureCluster {

    public SequenceCanopy(int[] center, int canopyId,
                          SequenceDistanceMeasure measure) {
        super(center, canopyId, measure);
        observe(center);
    }

    @Override
    public String toString() {
        return getIdentifier() + ": " + getCenter();
    }

    @Override
    public String getIdentifier() {
        return "C-" + getId();
    }

}
