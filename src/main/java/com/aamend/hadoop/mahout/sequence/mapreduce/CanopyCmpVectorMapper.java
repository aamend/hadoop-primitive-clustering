package com.aamend.hadoop.mahout.sequence.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Author: antoine.amend@tagman.com
 * Date: 06/03/14
 */
public class CanopyCmpVectorMapper
        extends Mapper<LongWritable, Text, Text, CampaignDateTupleWritable> {

    private static final Text KEY = new Text();
    public static final String CLIENT_ID_KEY = "client.id";
    public static final String BLACKLIST_CAMPAIGNS_KEY =
            "blacklisted.campaigns.ids";
    private static final CampaignDateTupleWritable VAL =
            new CampaignDateTupleWritable();

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CanopyCmpVectorMapper.class);

    private int clientId;
    private HashSet<Integer> blacklist;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        blacklist = new HashSet<Integer>();
        Configuration conf = context.getConfiguration();
        clientId = conf.getInt(CLIENT_ID_KEY, -1);
        if (clientId == -1) {
            throw new IOException(
                    "ClientID is null, please set " + CLIENT_ID_KEY +
                            " from configuration");
        } else {
            LOGGER.info("Fetching data for client id {}", clientId);
        }

        String strBlacklist = conf.get(BLACKLIST_CAMPAIGNS_KEY);
        if (StringUtils.isNotEmpty(strBlacklist)) {
            for (String str : strBlacklist.split(",")) {
                blacklist.add(Integer.parseInt(str));
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Map<String, String> map = extractKeyValuesFromLine(value.toString());
        String globalUserId = map.get("globaluserid");
        String tmClientId = map.get("tmclientid");
        String tmCampaignId = map.get("tmcampaignid");
        String strLogTime = map.get("logtime");
        String eventType = map.get("eventtype");

        try {

            // *********************
            // Filter
            // *********************

            if (StringUtils.isEmpty(globalUserId) ||
                    StringUtils.isEmpty(tmClientId)) {
                return;
            }
            if (globalUserId.equals("DNT") || globalUserId.equals("OPT_OUT")) {
                return;
            }
            if (Integer.parseInt(tmClientId) != this.clientId) {
                return;
            }

            long epoch = Long.parseLong(strLogTime);
            int cmpId;
            context.getCounter("DATA", eventType).increment(1L);
            if (eventType != null && eventType.equals("CONVERSION")) {
                cmpId = -1;
            } else {
                if (StringUtils.isEmpty(tmCampaignId)) {
                    return;
                } else {
                    cmpId = Integer.parseInt(tmCampaignId);
                    if (blacklist.contains(cmpId)) {
                        return;
                    }
                }
            }

            // *********************
            // Output
            // *********************

            KEY.set(globalUserId);
            VAL.setCmpId(cmpId);
            VAL.setEpoch(epoch);
            if (eventType.equals("CLICK")) {
                VAL.setEvent(1);
            } else if (eventType.equals("IMPRESSION")) {
                VAL.setEvent(2);
            } else if (eventType.equals("ARRIVAL")) {
                VAL.setEvent(3);
            }

            context.write(KEY, VAL);

        } catch (Exception e) {
            LOGGER.error("Exception raised while processing line {}. {}",
                    value.toString(), e);
            return;
        }
    }

    public static Map<String, String> extractKeyValuesFromLine(String line) {
        Map<String, String> map = new HashMap<String, String>();
        for (String strVal : line.split("&")) {
            String[] keyVal = strVal.split("=");
            try {
                String key = keyVal[0].toLowerCase();
                if (key.startsWith("_")) {
                    key = key.replaceFirst("_", "");
                }
                map.put(key, keyVal[1]);
            } catch (ArrayIndexOutOfBoundsException e) {
                // Do nothing
            }

        }
        return map;
    }

}
