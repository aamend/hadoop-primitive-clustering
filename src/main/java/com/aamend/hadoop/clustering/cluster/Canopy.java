package com.aamend.hadoop.clustering.cluster;

import com.aamend.hadoop.clustering.distance.DistanceMeasure;

import java.util.Arrays;

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

    public Canopy(int[] center, int canopyId, long observations,
                  DistanceMeasure measure) {
        super(center, canopyId, observations, measure);
        observe(center);
    }

    @Override
    public String toString() {
        return getIdentifier() + ": " + Arrays.toString(getCenter());
    }

    @Override
    public String getIdentifier() {
        return "C-" + getId();
    }

}
