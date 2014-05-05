package com.aamend.hadoop.mahout.sequence.distance;

import org.apache.mahout.common.parameters.Parametered;

public interface SequenceDistanceMeasure extends Parametered {

    double distance(int[] seq1, int[] seq2);

}
