package com.aamend.hadoop.mahout.sequence.cluster;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public abstract class AbstractCluster implements Cluster {

    private int id;
    private int[] center;
    private long observations;

    private static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractCluster.class);

    protected AbstractCluster(int[] sequence, int id, long observations) {
        this.center = sequence;
        this.id = id;
        this.observations = observations;
    }

    protected AbstractCluster(int[] sequence, int id) {
        this.center = sequence;
        this.id = id;
        this.observations = 0L;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        ArrayPrimitiveWritable apw = new ArrayPrimitiveWritable(center);
        apw.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        ArrayPrimitiveWritable awp = new ArrayPrimitiveWritable();
        awp.readFields(in);
        center = (int[]) awp.get();
    }

    @Override
    public void observe(ArrayPrimitiveWritable x) {
        observe((int[]) x.get());
    }

    @Override
    public String asFormatString() {
        StringBuilder buf = new StringBuilder(50);
        buf.append(getIdentifier());
        buf.append("{n=").append(observations);
        if (getCenter() != null) {
            buf.append(" c=").append(Arrays.toString(getCenter()));
        }
        buf.append('}');
        return buf.toString();
    }

    public int getId() {
        return id;
    }

    public int[] getCenter() {
        return center;
    }

    public long getObservations() {
        return observations;
    }

    public void setObservations(long observations) {
        this.observations = observations;
    }

    public void observe(int[] x) {
        observations++;
        if (center == null) {
            center = x.clone();
        }
    }

    public abstract String getIdentifier();

}