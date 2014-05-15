package com.aamend.hadoop.clustering.array;

import com.aamend.hadoop.clustering.array.distance.DistanceMeasure;
import com.aamend.hadoop.clustering.array.distance.LevenshteinDistanceMeasure;
import com.aamend.hadoop.clustering.array.job.ClusterDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by antoine on 12/05/14.
 */
public class MainClass {

    private static Logger LOGGER = LoggerFactory.getLogger(MainClass.class);

    public static void main(String[] args) throws Exception {
        MainClass m = new MainClass();
        m.run();
    }

    public void run() throws Exception {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://tagman");
        conf.set("mapred.job.tracker", "am0hd01.hosts.tagman.com:8021");
        conf.set("dfs.client.failover.proxy.provider.tagman",
                "org.apache.hadoop.hdfs.server." +
                        "namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.nameservices", "tagman");
        conf.set("dfs.ha.namenodes.tagman", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.tagman.nn1",
                "am0hdnn01.hosts.tagman.com:8020");
        conf.set("dfs.namenode.rpc-address.tagman.nn2",
                "am0hdnn02.hosts.tagman.com:8020");
        conf.set("mapred.child.java.opts", "-Xmx1024M");

        addMyselfToDistributedCache(conf);

        DistanceMeasure measure = new LevenshteinDistanceMeasure();
        Path output = new Path("tmp");
        Path input = new Path("sequences");

        LOGGER.info("Rebuilding clusters from {}", input);

        long canopies = ClusterDriver.buildClusters(conf, input, output, 28,
                measure, 0.33f, 0.3f, 10);
        if (canopies > 0) {
            LOGGER.info("Clustering data from {}", input);
            ClusterDriver.clusterData(conf, input, output, measure, 0.33f, 28);

        }
    }

    private void addMyselfToDistributedCache(Configuration conf)
            throws Exception {

        String jar = this.getClass()
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .getPath();

        FileSystem fs = FileSystem.get(conf);

        File jarFile = new File(jar);
        File jarDir = jarFile.getParentFile();

        File[] files = jarDir.listFiles();
        for (File file : files) {
            if (file.getName().contains(".jar")) {
                LOGGER.info("Adding [{}] jar file to distributed cache",
                        file.getName());
                Path jarPath = new Path("/tmp/" + file.getName());
                if (fs.exists(jarPath)) {
                    fs.delete(jarPath, true);
                }

                fs.copyFromLocalFile(new Path(file.getPath()), jarPath);
                DistributedCache.addFileToClassPath(jarPath, conf);
            }
        }
    }
}
