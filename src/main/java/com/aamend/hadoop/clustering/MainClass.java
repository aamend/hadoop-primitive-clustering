package com.aamend.hadoop.clustering;

import com.aamend.hadoop.clustering.distance.DistanceMeasure;
import com.aamend.hadoop.clustering.distance.LevenshteinDistanceMeasure;
import com.aamend.hadoop.clustering.job.CanopyDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by antoine on 12/05/14.
 */
public class MainClass extends Configured implements Tool {

    private static Logger LOGGER = LoggerFactory.getLogger(MainClass.class);
    private Configuration conf;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MainClass(), args);
        System.exit(res);
    }

    public long buildClusters(String input, String output) throws Exception {
        DistanceMeasure measure = new LevenshteinDistanceMeasure();
        LOGGER.info("Rebuilding clusters from {}", input);
        return CanopyDriver.buildClusters(conf, new Path(input), new Path(output), 14, measure, 0.33f, 0.3f, 10);
    }

    @Override
    public int run(String[] args) throws Exception {
        if(args == null || args.length < 2){
            throw new IllegalArgumentException("Input / Output path must be supplied");
        }
        String input = args[0];
        String output = args[1];
        return (int) buildClusters(input, output);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
