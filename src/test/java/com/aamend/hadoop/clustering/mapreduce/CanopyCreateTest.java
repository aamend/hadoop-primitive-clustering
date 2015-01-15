package com.aamend.hadoop.clustering.mapreduce;

import com.aamend.hadoop.clustering.cluster.Canopy;
import com.aamend.hadoop.clustering.cluster.CanopyWritable;
import com.aamend.hadoop.clustering.cluster.Cluster;
import com.aamend.hadoop.clustering.distance.DistanceMeasure;
import com.aamend.hadoop.clustering.distance.LevenshteinDistanceMeasure;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

@RunWith(JUnit4.class)
public class CanopyCreateTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanopyCreateTest.class);

    private MapReduceDriver<WritableComparable, ArrayPrimitiveWritable, Text, CanopyWritable, Text, CanopyWritable>
            mapReduceDriver;
    private DistanceMeasure measure;

    @Before
    public void setUp() throws IOException {

        measure = new LevenshteinDistanceMeasure();
        CanopyCreateInitMapper mapper = new CanopyCreateInitMapper();
        CanopyCreateReducer reducer = new CanopyCreateReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
        mapReduceDriver.withAll(getInputList());

    }

    @Test
    public void createCanopies() throws IOException {

        Configuration conf = mapReduceDriver.getConfiguration();
        conf.set(Canopy.CLUSTER_MEASURE, measure.getClass().getName());
        conf.setFloat(Canopy.CLUSTER_T1, 0.1f);
        conf.setFloat(Canopy.CLUSTER_T2, 0.08f);

        List<Pair<Text, CanopyWritable>> results = mapReduceDriver.run();
        for (Pair<Text, CanopyWritable> result : results) {
            Cluster ap = result.getSecond().get();
            LOGGER.info("Cluster : {}", ap.asFormattedString());
        }
        Assert.assertEquals("8 clusters should have been created", 8, results.size());
        LOGGER.info("{} clusters have been created", results.size());
    }

    @Test
    public void createCanopiesLargerT1T2() throws IOException {

        Configuration conf = mapReduceDriver.getConfiguration();
        conf.set(Canopy.CLUSTER_MEASURE, measure.getClass().getName());
        conf.setFloat(Canopy.CLUSTER_T1, 0.25f);
        conf.setFloat(Canopy.CLUSTER_T2, 0.15f);

        List<Pair<Text, CanopyWritable>> results = mapReduceDriver.run();
        for (Pair<Text, CanopyWritable> result : results) {
            Cluster ap = result.getSecond().get();
            LOGGER.info("Cluster : {}", ap.asFormattedString());
        }
        Assert.assertEquals("4 clusters should have been created", 4, results.size());
        LOGGER.info("{} clusters have been created", results.size());
    }

    @Test
    public void createCanopiesLargestT1T2() throws IOException {

        Configuration conf = mapReduceDriver.getConfiguration();
        conf.set(Canopy.CLUSTER_MEASURE, measure.getClass().getName());
        conf.setFloat(Canopy.CLUSTER_T1, 1.0f);
        conf.setFloat(Canopy.CLUSTER_T2, 0.95f);

        List<Pair<Text, CanopyWritable>> results = mapReduceDriver.run();
        for (Pair<Text, CanopyWritable> result : results) {
            Cluster ap = result.getSecond().get();
            LOGGER.info("Cluster : {}", ap.asFormattedString());
        }
        Assert.assertEquals("1 clusters should have been created", 1, results.size());
        LOGGER.info("{} clusters have been created", results.size());
    }

    private List<Pair<WritableComparable, ArrayPrimitiveWritable>> getInputList()
            throws FileNotFoundException {

        List<Pair<WritableComparable, ArrayPrimitiveWritable>>
                inputList = new ArrayList<Pair<WritableComparable, ArrayPrimitiveWritable>>();
        String inputUrl = getClass().getResource("canopies.input").getFile();
        File inputFile = new File(inputUrl);
        Scanner in = new Scanner(inputFile);

        while (in.hasNext()) {
            String line = in.nextLine();
            int[] ap = new int[8];
            int i = 0;
            for (String str : line.split(",")) {
                ap[i] = Integer.parseInt(str);
                i++;
            }

            Pair<WritableComparable, ArrayPrimitiveWritable> inputPair =
                    new Pair<WritableComparable, ArrayPrimitiveWritable>(
                            new Text("dummy"),
                            new ArrayPrimitiveWritable(ap));
            inputList.add(inputPair);

        }

        return inputList;
    }
}
