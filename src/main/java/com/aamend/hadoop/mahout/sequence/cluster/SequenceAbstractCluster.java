package com.aamend.hadoop.mahout.sequence.cluster;

import com.aamend.hadoop.mahout.sequence.io.SequenceWritable;
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
    private int[] center;

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SequenceAbstractCluster.class);

    protected SequenceAbstractCluster(int[] sequence, int id) {
        this.numObservations = 0;
        this.center = sequence;
        this.id = id;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeLong(numObservations);
        SequenceWritable apw = new SequenceWritable(center);
        apw.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.numObservations = in.readLong();
        SequenceWritable awp = new SequenceWritable();
        awp.readFields(in);
        center = awp.get();
    }

    @Override
    public void observe(SequenceWritable x) {
        observe(x.get());
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

    public int[] getCenter() {
        return center;
    }

    public void observe(int[] x) {
        numObservations++;
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