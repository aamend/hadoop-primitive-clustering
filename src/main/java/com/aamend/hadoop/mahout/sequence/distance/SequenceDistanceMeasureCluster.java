package com.aamend.hadoop.mahout.sequence.distance;

import com.aamend.hadoop.mahout.sequence.cluster.SequenceAbstractCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.parameters.Parameter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public class SequenceDistanceMeasureCluster extends SequenceAbstractCluster {

    private SequenceDistanceMeasure measure;

    public SequenceDistanceMeasureCluster(Object[] point, int id,
                                          SequenceDistanceMeasure measure) {
        super(point, id);
        this.measure = measure;
    }

    @Override public Collection<Parameter<?>> getParameters() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public void createParameters(String prefix, Configuration jobConf) {
        // Nothing to do
    }

    @Override
    public void configure(Configuration job) {
        if (measure != null) {
            measure.configure(job);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String dm = in.readUTF();
        this.measure =
                ClassUtils.instantiateAs(dm, SequenceDistanceMeasure.class);
        super.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(measure.getClass().getName());
        super.write(out);
    }

    @Override
    public double pdf(ArrayPrimitiveWritable apw) {
        return 1 / (1 + measure.distance((Object[]) apw.get(), getCenter()));
    }

    @Override
    public String getIdentifier() {
        return "DMC:" + getId();
    }

    public SequenceDistanceMeasure getMeasure() {
        return measure;
    }

    public void setMeasure(SequenceDistanceMeasure measure) {
        this.measure = measure;
    }


}
