package com.aamend.hadoop.mahout.sequence.cluster;

import com.aamend.hadoop.mahout.sequence.distance.DistanceMeasure;
import com.aamend.hadoop.mahout.sequence.distance.DistanceMeasureCluster;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public class Canopy
        extends DistanceMeasureCluster {

    public Canopy(int[] center, int canopyId,
                  DistanceMeasure measure) {
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
