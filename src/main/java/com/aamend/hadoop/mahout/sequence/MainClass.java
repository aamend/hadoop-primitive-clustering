package com.aamend.hadoop.mahout.sequence;

import com.aamend.hadoop.mahout.sequence.distance.LevenshteinDistanceMeasure;
import com.aamend.hadoop.mahout.sequence.job.ClusteringDriver;
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
        ClusteringDriver
                .buildClusters(conf, new Path("sequences"), new Path("tmp"),
                        12,
                        new LevenshteinDistanceMeasure(), 0.35f, 0.3f);

        ClusteringDriver
                .clusterData(conf, new Path("sequences"), new Path("tmp"),
                        new LevenshteinDistanceMeasure(), 0.35f, 0.6f,
                        12);
    }

}
