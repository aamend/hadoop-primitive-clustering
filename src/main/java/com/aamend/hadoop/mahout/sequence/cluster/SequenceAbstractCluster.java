package com.aamend.hadoop.mahout.sequence.cluster;

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
    private int[] center;

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SequenceAbstractCluster.class);

    protected SequenceAbstractCluster(int[] sequence, int id) {
        this.center = sequence;
        this.id = id;
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
        buf.append("{");
        if (getCenter() != null) {
            buf.append(" c=").append(formatSequence(getCenter()));
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

    public void observe(int[] x) {
        if (center == null) {
            center = x.clone();
        }
    }

    public abstract String getIdentifier();

    public static String formatSequence(int[] sequence) {
        StringBuilder buffer = new StringBuilder();
        buffer.append('[');
        for (int element : sequence) {
            buffer.append(element).append(", ");
        }

        if (buffer.length() > 1) {
            buffer.setLength(buffer.length() - 2);
        }
        buffer.append(']');
        return buffer.toString();
    }

}