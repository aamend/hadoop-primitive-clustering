package com.aamend.hadoop.clustering.mapreduce;

import com.aamend.hadoop.clustering.cluster.CanopyConfigKeys;
import com.aamend.hadoop.clustering.distance.DistanceMeasure;
import com.aamend.hadoop.clustering.distance.LevenshteinDistanceMeasure;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
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
import java.util.*;

/**
 * Author: antoine.amend@tagman.com
 * Date: 13/12/13
 */
@RunWith(JUnit4.class)
public class CanopyCreateTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CanopyCreateTest.class);

    private
    MapReduceDriver<Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable>
            mapReduceDriver;
    private DistanceMeasure measure;

    @Before
    public void setUp() throws IOException {

        measure = new LevenshteinDistanceMeasure();
        ClusterCreateMapper mapper = new ClusterCreateMapper();
        ClusterCreateReducer reducer = new ClusterCreateReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
        mapReduceDriver.withAll(getInputList());

    }

    @Test
    public void createCanopies() throws IOException {

        Configuration conf = mapReduceDriver.getConfiguration();
        conf.set(CanopyConfigKeys.CLUSTER_MEASURE,
                measure.getClass().getName());
        conf.setFloat(CanopyConfigKeys.CLUSTER_T1, 0.1f);
        conf.setFloat(CanopyConfigKeys.CLUSTER_T2, 0.08f);

        Set<String> clusters = new HashSet<String>();
        List<Pair<Text, ArrayPrimitiveWritable>> results =
                mapReduceDriver.run();
        for (Pair<Text, ArrayPrimitiveWritable> result : results) {
            int[] ap = (int[]) result.getSecond().get();
            String str = Arrays.toString(ap);
            if (!clusters.contains(str)) {
                clusters.add(str);
                LOGGER.info("Cluster center : {}", str);
            }
        }
        Assert.assertEquals("8 clusters should have been created", 8,
                clusters.size());
        LOGGER.info("{} clusters have been created", clusters.size());
    }

    @Test
    public void createCanopiesLargerT1T2() throws IOException {

        Configuration conf = mapReduceDriver.getConfiguration();
        conf.set(CanopyConfigKeys.CLUSTER_MEASURE,
                measure.getClass().getName());
        conf.setFloat(CanopyConfigKeys.CLUSTER_T1, 0.25f);
        conf.setFloat(CanopyConfigKeys.CLUSTER_T2, 0.15f);

        Set<String> clusters = new HashSet<String>();
        List<Pair<Text, ArrayPrimitiveWritable>> results =
                mapReduceDriver.run();
        for (Pair<Text, ArrayPrimitiveWritable> result : results) {
            int[] ap = (int[]) result.getSecond().get();
            String str = Arrays.toString(ap);
            if (!clusters.contains(str)) {
                clusters.add(str);
                LOGGER.info("Cluster center : {}", str);
            }
        }
        Assert.assertEquals("4 cluster should have been created", 4,
                clusters.size());
        LOGGER.info("{} clusters have been created", clusters.size());
    }

    @Test
    public void createCanopiesLargestT1T2() throws IOException {

        Configuration conf = mapReduceDriver.getConfiguration();
        conf.set(CanopyConfigKeys.CLUSTER_MEASURE,
                measure.getClass().getName());
        conf.setFloat(CanopyConfigKeys.CLUSTER_T1, 1.0f);
        conf.setFloat(CanopyConfigKeys.CLUSTER_T2, 0.95f);

        Set<String> clusters = new HashSet<String>();
        List<Pair<Text, ArrayPrimitiveWritable>> results =
                mapReduceDriver.run();
        for (Pair<Text, ArrayPrimitiveWritable> result : results) {
            int[] ap = (int[]) result.getSecond().get();
            String str = Arrays.toString(ap);
            if (!clusters.contains(str)) {
                clusters.add(str);
                LOGGER.info("Cluster center : {}", str);
            }
        }
        Assert.assertEquals("1 cluster should have been created", 1,
                clusters.size());
        LOGGER.info("{} clusters have been created", clusters.size());
    }

    private List<Pair<Text, ArrayPrimitiveWritable>> getInputList()
            throws
            FileNotFoundException {

        List<Pair<Text, ArrayPrimitiveWritable>>
                inputList =
                new ArrayList<Pair<Text, ArrayPrimitiveWritable>>();
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

            Pair<Text, ArrayPrimitiveWritable> inputPair =
                    new Pair<Text, ArrayPrimitiveWritable>(
                            new Text("dummy"),
                            new ArrayPrimitiveWritable(ap));
            inputList.add(inputPair);

        }

        return inputList;
    }
}
