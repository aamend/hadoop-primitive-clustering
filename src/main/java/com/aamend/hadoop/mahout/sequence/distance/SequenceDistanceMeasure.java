package com.aamend.hadoop.mahout.sequence.distance;

import org.apache.mahout.common.parameters.Parametered;

public interface SequenceDistanceMeasure extends Parametered {

    double distance(Object[] seq1, Object[] seq2);

}
