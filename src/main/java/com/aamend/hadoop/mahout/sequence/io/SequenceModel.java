package com.aamend.hadoop.mahout.sequence.io;

import org.apache.hadoop.io.Writable;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public interface SequenceModel<O> extends Writable {

    double pdf(O x);

    void observe(O x);

}
