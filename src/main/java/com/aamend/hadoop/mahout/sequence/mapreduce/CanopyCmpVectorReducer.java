package com.aamend.hadoop.mahout.sequence.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Author: antoine.amend@tagman.com
 * Date: 06/03/14
 */
public class CanopyCmpVectorReducer extends
        Reducer<Text, CampaignDateTupleWritable, Text, ArrayPrimitiveWritable> {

    public static Pattern REPEATED_CAMPAIGNS = Pattern.compile("(.+,)\\1+");

    private static final Text KEY = new Text();
    public static final String USER_JOURNEY_CONVERSION =
            "user.journey.conversion";
    public static final String USER_JOURNEY_NON_CONVERSION =
            "user.journey.non.conversions";
    public static final String MIN_CMP_KEY = "min.campaigns";

    private int minCampaigns;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        minCampaigns = context.getConfiguration().getInt(MIN_CMP_KEY, 3);
    }

    @Override
    protected void reduce(Text key, Iterable<CampaignDateTupleWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        // Iterate through all campaigns events
        List<CampaignDateTuple> campaigns = new ArrayList<CampaignDateTuple>();
        for (CampaignDateTupleWritable value : values) {
            CampaignDateTuple tuple =
                    new CampaignDateTuple(value.getEpoch(), value.getCmpId(),
                            value.getEvent());
            campaigns.add(tuple);
        }

        // Sort my campaigns by timestamp
        Collections.sort(campaigns);

        List<UserJourney> userJourneys = new ArrayList<UserJourney>();
        List<String> userJourney = new ArrayList<String>();
        long start = Long.MIN_VALUE;
        boolean first = true;
        String previous = "";

        // Iterate through all sorted campaigns events
        for (CampaignDateTuple tuple : campaigns) {

            if (first) {
                start = tuple.getEpoch();
                first = false;
            }

            int cmpId = tuple.getCmpId();
            int event = tuple.getEvent();

            // Ignore campaign if the same as previous
            // That will save some time on next REGEX matching process
            String current = cmpId + ":" + event;
            if (!current.equals(previous)) {
                previous = current;
                if (cmpId == -1) {
                    // Conversion event
                    long end = tuple.getEpoch();
                    double timeToConvert = (end - start) / (3600 * 24);
                    first = true;
                    if (userJourney.size() >= minCampaigns) {
                        //LOGGER.info("C1:"+userJourney);
                        List<Integer> filteredUserJourney =
                                removeRepeatedSequences(
                                        userJourney);
                        if (filteredUserJourney.size() >= minCampaigns) {
                            //LOGGER.info("C2:"+filteredUserJourney);
                            UserJourney uj =
                                    new UserJourney(filteredUserJourney,
                                            timeToConvert);
                            userJourneys.add(uj);
                            context.getCounter("DATA", USER_JOURNEY_CONVERSION)
                                    .increment(1L);
                        } else {
                            context.getCounter("REJECTED_DATA",
                                    USER_JOURNEY_CONVERSION).increment(1L);
                        }
                    } else {
                        context.getCounter("REJECTED_DATA",
                                USER_JOURNEY_CONVERSION)
                                .increment(1L);
                    }
                    userJourney = new ArrayList<String>();
                } else {
                    // Not a conversion, add this campaign to user journey
                    userJourney.add(cmpId + ":" + event);
                }
            }
        }

        // Add remaining campaign to non-conversion journey
        if (userJourney.size() >= minCampaigns) {
            //LOGGER.info("N1:"+userJourney);
            List<Integer> filteredUserJourney = removeRepeatedSequences(
                    userJourney);
            if (filteredUserJourney.size() >= minCampaigns) {
                //LOGGER.info("N2:"+filteredUserJourney);
                UserJourney uj = new UserJourney(filteredUserJourney,
                        Double.POSITIVE_INFINITY);
                userJourneys.add(uj);
                context.getCounter("DATA", USER_JOURNEY_NON_CONVERSION)
                        .increment(1L);
            } else {
                context.getCounter("REJECTED_DATA", USER_JOURNEY_NON_CONVERSION)
                        .increment(1L);
            }
        } else {
            context.getCounter("REJECTED_DATA", USER_JOURNEY_NON_CONVERSION)
                    .increment(1L);
        }

        // Iterate over all (filtered) conversions and non-conversions journeys
        int journeyId = 0;
        for (UserJourney uj : userJourneys) {
            int[] array = uj.toArray();
            journeyId++;
            KEY.set(key + "_" + journeyId);
            context.write(KEY, new ArrayPrimitiveWritable(array));
        }
    }

    private static List<Integer> removeRepeatedSequences(List<String> list) {

        // Get String on which duplicates will be removed
        StringBuilder sb = new StringBuilder();
        for (String cmp : list) {
            sb.append(cmp).append(",");
        }

        String filtered = removeRepeatedStringSequences(sb.toString());
        String[] subs = filtered.split(",");
        List<Integer> newList = new ArrayList<Integer>(subs.length);
        for (String s : subs) {
            if (StringUtils.isNotEmpty(s)) {
                newList.add(Integer.parseInt(s.split(":")[0]));
            }
        }
        return newList;
    }

    private static String removeRepeatedStringSequences(String text) {
        Matcher matcher = REPEATED_CAMPAIGNS.matcher(text);
        String sub = text;
        if (matcher.find()) {
            sub = matcher.replaceFirst(matcher.group(1));
            return removeRepeatedStringSequences(sub);
        } else {
            return sub;
        }
    }

    public class UserJourney {

        public List<Integer> getCampaigns() {
            return campaigns;
        }

        public double getTimeToConvert() {
            return timeToConvert;
        }

        private List<Integer> campaigns;
        private double timeToConvert;

        public UserJourney(List<Integer> campaigns, double timeToConvert) {
            this.campaigns = campaigns;
            this.timeToConvert = timeToConvert;
        }

        public int[] toArray() {

            int[] array = new int[campaigns.size()];
            for (int i = 0; i < campaigns.size(); i++) {
                array[i] = campaigns.get(i);
            }
            return array;
        }
    }
}
