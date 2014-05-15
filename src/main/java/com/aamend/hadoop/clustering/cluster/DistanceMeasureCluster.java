package com.aamend.hadoop.clustering.cluster;

import com.aamend.hadoop.clustering.distance.DistanceMeasure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public class DistanceMeasureCluster extends AbstractCluster {

    private DistanceMeasure measure;

    public DistanceMeasureCluster(int[] point, int id,
                                  DistanceMeasure measure) {
        super(point, id);
        this.measure = measure;
    }

    public DistanceMeasureCluster(int[] point, int id, long observations,
                                  DistanceMeasure measure) {
        super(point, id, observations);
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
            measure = (DistanceMeasure) obj;
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
    public double pdf(ArrayPrimitiveWritable apw) {
        return 1.0d - measure.distance((int[]) apw.get(), getCenter());
    }

    @Override
    public String getIdentifier() {
        return "DMC:" + getId();
    }

    public DistanceMeasure getMeasure() {
        return measure;
    }

    public void setMeasure(DistanceMeasure measure) {
        this.measure = measure;
    }


}
