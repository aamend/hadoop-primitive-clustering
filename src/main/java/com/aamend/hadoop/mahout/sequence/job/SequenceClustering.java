package com.aamend.hadoop.mahout.sequence.job;

import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopyConfigKeys;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCluster;
import com.aamend.hadoop.mahout.sequence.distance.SequenceDistanceMeasure;
import com.aamend.hadoop.mahout.sequence.distance.SequenceLevenshteinDistanceMeasure;
import com.aamend.hadoop.mahout.sequence.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Created by antoine on 06/05/14.
 */
public class SequenceClustering {

    private static final float T1 = 0.33f;
    private static final float T2 = 0.3f;
    private static final int CLIENT_ID = 93;
    private static final int REDUCERS = 28;
    private static final int MIN_CAMPAIGNS = 6;
    private static final String BLACKLIST = "15";

    private static final Logger LOGGER = LoggerFactory.getLogger(
            SequenceClustering.class);

    public static void main(String[] args) throws Exception {
        SequenceClustering clustering = new SequenceClustering();
        clustering.run();
    }

    public void run() throws Exception {

        // Create my configuration
        Configuration conf = new Configuration();

        // ... first with hadoop related params ...
        conf.set("fs.defaultFS", "hdfs://tagman");
        conf.set("mapred.job.tracker", "am0hd01.hosts.tagman.com:8021");
        conf.set("dfs.client.failover.proxy.provider.tagman",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.nameservices", "tagman");
        conf.set("dfs.ha.namenodes.tagman", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.tagman.nn1",
                "am0hdnn01.hosts.tagman.com:8020");
        conf.set("dfs.namenode.rpc-address.tagman.nn2",
                "am0hdnn02.hosts.tagman.com:8020");
        conf.set("mapred.child.java.opts", "-Xmx1024M");

        addJarToDistributedCache(SequenceClustering.class, conf);

        SequenceDistanceMeasure measure =
                new SequenceLevenshteinDistanceMeasure();

        conf.set(SequenceCanopyConfigKeys.DISTANCE_MEASURE_KEY,
                measure.getClass().getName());
        conf.setFloat(SequenceCanopyConfigKeys.T1_KEY, T1);
        conf.setFloat(SequenceCanopyConfigKeys.T2_KEY, T2);
        conf.setFloat(SequenceCanopyConfigKeys.MAX_DISTANCE_MEASURE, T1);
        conf.set(CanopyCmpVectorMapper.BLACKLIST_CAMPAIGNS_KEY, BLACKLIST);
        conf.setInt(CanopyCmpVectorMapper.CLIENT_ID_KEY, CLIENT_ID);
        conf.setInt(CanopyCmpVectorReducer.MIN_CMP_KEY, MIN_CAMPAIGNS);


        // Declare my Input / Output directory structure
        String inputDir = "/www/loggers/backup/tmdata";
        String uuid = UUID.randomUUID().toString().toUpperCase();
        Path tmp1 = new Path("/tmp" + uuid + "_1");
        Path tmp2 = new Path("/tmp/" + uuid + "_2");

        // Prepare job
        Job arrayJob = new Job(conf, "Create Arrays");
        arrayJob.setJarByClass(CanopyCmpVectorMapper.class);
        arrayJob.setMapperClass(CanopyCmpVectorMapper.class);
        arrayJob.setReducerClass(CanopyCmpVectorReducer.class);
        arrayJob.setNumReduceTasks(REDUCERS);
        arrayJob.setMapOutputKeyClass(Text.class);
        arrayJob.setMapOutputValueClass(CampaignDateTupleWritable.class);
        arrayJob.setOutputKeyClass(Text.class);
        arrayJob.setOutputValueClass(ArrayPrimitiveWritable.class);
        arrayJob.setInputFormatClass(TextInputFormat.class);
        arrayJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(arrayJob, new Path(inputDir));
        SequenceFileOutputFormat.setOutputPath(arrayJob, tmp1);

        // Submit job
        if (!arrayJob.waitForCompletion(true)) {
            throw new IOException("MapReduce execution failed, please check " +
                    arrayJob.getTrackingURL());
        }


        // Prepare job iteration
        int reducers = REDUCERS / 2;
        int numIterations =
                (int) Math.floor(Math.log(reducers) / Math.log(2)) + 1;
        float t1It = T1 / (numIterations + 1);
        float t2It = T2 / (numIterations + 1);
        float t1 = t1It;
        float t2 = t2It;
        int iteration = 0;

        Path sequences = tmp1;
        Path output = new Path(tmp2, SequenceCluster.INITIAL_CLUSTERS_DIR);

        boolean last = false;
        while (reducers >= 0 && !last) {

            iteration++;
            if (reducers == 0)
                last = true;

            LOGGER.info("Job      : {}/{}", iteration, numIterations);
            LOGGER.info("T1       : {}", t1);
            LOGGER.info("T2       : {}", t2);
            LOGGER.info("Input    : {}", sequences.toString());
            LOGGER.info("Output   : {}", output.toString());
            LOGGER.info("Reducers : {}", reducers);

            // Add job specific configuration
            conf.setFloat(SequenceCanopyConfigKeys.T1_KEY, t1);
            conf.setFloat(SequenceCanopyConfigKeys.T2_KEY, t2);
            conf.setFloat(SequenceCanopyConfigKeys.MAX_DISTANCE_MEASURE, t1);

            // Prepare job
            Job canopyJob = new Job(conf,
                    "Create clusters - " + iteration + "/" + numIterations);
            canopyJob.setMapperClass(SequenceCanopyCreateMapper.class);
            canopyJob.setCombinerClass(SequenceCanopyCreateCombiner.class);
            canopyJob.setReducerClass(SequenceCanopyCreateReducer.class);
            canopyJob.setJarByClass(SequenceClustering.class);
            canopyJob.setNumReduceTasks(reducers);
            canopyJob.setMapOutputKeyClass(Text.class);
            canopyJob.setMapOutputValueClass(ArrayPrimitiveWritable.class);
            canopyJob.setOutputKeyClass(Text.class);
            canopyJob.setOutputValueClass(ArrayPrimitiveWritable.class);
            canopyJob.setInputFormatClass(SequenceFileInputFormat.class);
            canopyJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileInputFormat.addInputPath(canopyJob, sequences);
            SequenceFileOutputFormat.setOutputPath(canopyJob, output);

            // Submit job
            if (!canopyJob.waitForCompletion(true)) {
                throw new IOException(
                        "MapReduce execution failed, please check " +
                                canopyJob.getTrackingURL());
            }

            // Get 2 times less reducers at next step
            reducers = reducers / 2;

            // Get slightly larger clusters at next step
            t1 += t1It;
            t2 += t2It;

            // Output of previous job will be input as next one
            sequences = output;

            String newDir = SequenceCluster.CLUSTERS_DIR + iteration;
            if (reducers == 0) {
                newDir += SequenceCluster.FINAL_ITERATION_SUFFIX;
            }
            output = new Path(tmp2, newDir);

        }


    }

    private void addJarToDistributedCache(Class classToAdd, Configuration conf)
            throws Exception {

        String jar = classToAdd
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .getPath();

        FileSystem fs = FileSystem.get(conf);
        File jarFile = new File(jar);
        Path jarPath = new Path("/tmp/" + jarFile.getName());
        if (fs.exists(jarPath)) {
            fs.delete(jarPath, true);
        }

        fs.copyFromLocalFile(new Path(jar), jarPath);
        System.out.println("Added [" + classToAdd + "] to distributed cache");
        DistributedCache.addFileToClassPath(jarPath, conf);

    }
}
