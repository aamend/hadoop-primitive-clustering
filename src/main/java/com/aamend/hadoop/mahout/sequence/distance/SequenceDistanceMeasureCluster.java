package com.aamend.hadoop.mahout.sequence.distance;

import com.aamend.hadoop.mahout.sequence.cluster.SequenceAbstractCluster;
import com.aamend.hadoop.mahout.sequence.io.SequenceWritable;
import org.apache.hadoop.conf.Configuration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public class SequenceDistanceMeasureCluster extends SequenceAbstractCluster {

    private SequenceDistanceMeasure measure;

    public SequenceDistanceMeasureCluster(int[] point, int id,
                                          SequenceDistanceMeasure measure) {
        super(point, id);
        this.measure = measure;
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
        try {
            Class<?> clazz = Class.forName(dm);
            Object obj = clazz.newInstance();
            measure = (SequenceDistanceMeasure) obj;
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        } catch (InstantiationException e) {
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }
        super.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(measure.getClass().getName());
        super.write(out);
    }

    @Override
    public double pdf(SequenceWritable apw) {
        return 1 / (1 + measure.distance(apw.get(), getCenter()));
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
