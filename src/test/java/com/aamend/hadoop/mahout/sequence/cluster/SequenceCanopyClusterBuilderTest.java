package com.aamend.hadoop.mahout.sequence.cluster;

import com.aamend.hadoop.mahout.sequence.distance.LevenshteinSequenceDistanceMeasure;
import com.aamend.hadoop.mahout.sequence.distance.SequenceDistanceMeasure;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.ClassUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;

/**
 * Created by antoine on 05/05/14.
 */
@RunWith(JUnit4.class)
public class SequenceCanopyClusterBuilderTest {

    private static Logger LOGGER = LoggerFactory.getLogger
            (SequenceCanopyClusterBuilderTest.class);
    private List<Object[]> sequences = new ArrayList<Object[]>();
    private final Collection<SequenceCanopy> canopies = Lists.newArrayList();
    private SequenceCanopyClusterBuilder builder;

    @Before
    public void prepare() throws FileNotFoundException {

        Configuration conf = new Configuration();
        SequenceDistanceMeasure measure = ClassUtils.instantiateAs(
                LevenshteinSequenceDistanceMeasure.class,
                SequenceDistanceMeasure.class);
        conf.set(SequenceCanopyConfigKeys.DISTANCE_MEASURE_KEY,
                measure.getClass().getName());
        conf.setFloat(SequenceCanopyConfigKeys.MAX_DISTANCE_MEASURE, 0.1f);

        builder = SequenceCanopyConfigKeys.configureClusterBuilder(conf);

        String inputUrl = getClass().getResource("canopies.input").getFile();
        File inputFile = new File(inputUrl);
        Scanner in = new Scanner(inputFile);

        while (in.hasNext()) {
            String line = in.nextLine();
            LOGGER.info(line);
            List<Integer> list = new ArrayList<Integer>();
            for (String id : line.split(",")) {
                list.add(Integer.parseInt(id));
            }

            Object[] sequence = new Object[list.size()];
            for (int i = 0; i < list.size(); i++) {
                sequence[i] = list.get(i);
            }
            sequences.add(sequence);
        }
    }


    @Test
    public void cluster() {

        for (Object[] sequence : sequences) {
            builder.addPointToCanopies(sequence, canopies);
        }
        LOGGER.info("{} canopies created", canopies.size());
        Assert.assertEquals("3 canopies must have been created", canopies.size
                (), 3);
        for (SequenceCanopy canopy : canopies) {
            LOGGER.info(canopy.asFormatString());
        }
    }


}
