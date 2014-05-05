package com.aamend.hadoop.mahout.sequence.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public abstract class SequenceAbstractCluster implements SequenceCluster {

    private int id;
    private long numObservations;
    private Object[] center;

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SequenceAbstractCluster.class);

    protected SequenceAbstractCluster(Object[] sequence, int id) {
        this.numObservations = 0;
        this.center = sequence;
        this.id = id;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeLong(numObservations);
        ArrayPrimitiveWritable apw = new ArrayPrimitiveWritable(center);
        apw.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.numObservations = in.readLong();
        ArrayPrimitiveWritable awp = new ArrayPrimitiveWritable();
        awp.readFields(in);
        center = (Object[]) awp.get();
    }

    @Override
    public void configure(Configuration job) {
        // nothing to do
    }

    @Override
    public void observe(ArrayPrimitiveWritable x) {
        observe((Object[]) x.get());
    }

    @Override
    public String asFormatString() {
        StringBuilder buf = new StringBuilder(50);
        buf.append(getIdentifier());
        buf.append("{n=").append(numObservations);
        if (getCenter() != null) {
            buf.append(" c=").append(formatSequence(getCenter()));
        }
        buf.append('}');
        return buf.toString();
    }

    public int getId() {
        return id;
    }

    public long getNumObservations() {
        return numObservations;
    }

    public Object[] getCenter() {
        return center;
    }

    public void observe(Object[] x) {
        numObservations++;
        if (center == null) {
            center = x.clone();
        }
    }

    public abstract String getIdentifier();

    public static String formatSequence(Object[] sequence) {
        StringBuilder buffer = new StringBuilder();
        buffer.append('[');
        for (Object element : sequence) {
            buffer.append(element.toString()).append(", ");
        }

        if (buffer.length() > 1) {
            buffer.setLength(buffer.length() - 2);
        }
        buffer.append(']');
        return buffer.toString();
    }

}