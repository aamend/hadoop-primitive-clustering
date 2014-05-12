package com.aamend.hadoop.mahout.sequence;

import com.aamend.hadoop.mahout.sequence.distance.SequenceLevenshteinDistanceMeasure;
import com.aamend.hadoop.mahout.sequence.job.SequenceClusteringDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by antoine on 12/05/14.
 */
public class MainClass {

    public static void main(String[] args)
            throws InterruptedException, IOException, ClassNotFoundException {
        MainClass m = new MainClass();
        m.run();
    }

    public void run()
            throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        SequenceClusteringDriver
                .buildClusters(conf, new Path("sequences"), new Path("tmp"), 24,
                        new SequenceLevenshteinDistanceMeasure(), 0.35f, 0.3f);

        SequenceClusteringDriver
                .clusterData(conf, new Path("sequences"), new Path("tmp"),
                        new SequenceLevenshteinDistanceMeasure(), 0.35f, 0.6f,
                        24);
    }

}
