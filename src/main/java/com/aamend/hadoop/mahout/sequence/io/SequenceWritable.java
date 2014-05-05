package com.aamend.hadoop.mahout.sequence.io;

import com.aamend.hadoop.mahout.sequence.cluster.SequenceAbstractCluster;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

public class SequenceWritable implements Writable {

    private int length;
    private int[] value;

    public SequenceWritable() {
        //empty constructor
    }

    public SequenceWritable(int[] value) {
        set(value);
    }

    public int[] get() {
        return value;
    }

    public void set(int[] value) {
        this.value = value;
        this.length = Array.getLength(value);
    }

    @Override
    public void write(DataOutput out) throws IOException {

        // write length
        out.writeInt(length);
        // write array
        for (int i = 0; i < length; i++)
            out.writeInt(value[i]);

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        // read and set the length of the array
        int length = in.readInt();
        if (length < 0) {
            throw new IOException("encoded array length is negative " + length);
        }
        this.length = length;

        // construct and read in the array
        value = new int[length];

        for (int i = 0; i < length; i++)
            value[i] = in.readInt();
    }

    @Override
    public String toString() {
        return SequenceAbstractCluster.formatSequence(value);
    }
}

