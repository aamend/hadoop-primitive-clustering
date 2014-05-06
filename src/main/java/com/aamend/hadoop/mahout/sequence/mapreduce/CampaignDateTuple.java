package com.aamend.hadoop.mahout.sequence.mapreduce;

/**
 * Author: antoine.amend@tagman.com
 * Date: 19/02/14
 */
public class CampaignDateTuple implements Comparable {

    private long epoch;
    private int cmpId;
    private int event;

    public int getEvent() {
        return event;
    }

    public void setEvent(int event) {
        this.event = event;
    }

    public int getCmpId() {
        return cmpId;
    }

    public void setCmpId(int cmpId) {
        this.cmpId = cmpId;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public CampaignDateTuple(long epoch, int cmpId, int event) {
        this.setCmpId(cmpId);
        this.setEpoch(epoch);
        this.setEvent(event);
    }

    public CampaignDateTuple() {

    }

    @Override
    public int compareTo(Object o) {
        CampaignDateTuple that = (CampaignDateTuple) o;
        if (that.getEpoch() > this.getEpoch()) {
            return -1;
        } else if (that.getEpoch() < this.getEpoch()) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return epoch + ":" + cmpId + ":" + event;
    }
}
