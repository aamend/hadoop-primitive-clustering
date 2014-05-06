package com.aamend.hadoop.mahout.sequence.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author: antoine.amend@tagman.com
 * Date: 19/02/14
 */
public class CampaignDateTupleWritable extends CampaignDateTuple
        implements Writable {


    public CampaignDateTupleWritable() {
    }

    public CampaignDateTupleWritable(long epoch, int cmpId, int event) {
        this.setCmpId(cmpId);
        this.setEpoch(epoch);
        this.setEvent(event);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.getEpoch());
        out.writeInt(this.getCmpId());
        out.writeInt(this.getEvent());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.setEpoch(in.readLong());
        this.setCmpId(in.readInt());
        this.setEvent(in.readInt());
    }
}
