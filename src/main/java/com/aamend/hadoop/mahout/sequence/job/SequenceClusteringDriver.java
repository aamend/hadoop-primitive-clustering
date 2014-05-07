package com.aamend.hadoop.mahout.sequence.job;

import com.aamend.hadoop.mahout.sequence.cluster.SequenceCanopyConfigKeys;
import com.aamend.hadoop.mahout.sequence.cluster.SequenceCluster;
import com.aamend.hadoop.mahout.sequence.distance.SequenceDistanceMeasure;
import com.aamend.hadoop.mahout.sequence.mapreduce.SequenceCanopyCreateCombiner;
import com.aamend.hadoop.mahout.sequence.mapreduce.SequenceCanopyCreateMapper;
import com.aamend.hadoop.mahout.sequence.mapreduce.SequenceCanopyCreateReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SequenceClusteringDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            SequenceClusteringDriver.class);

    /**
     * Build a directory of Canopy clusters from the input arguments.
     * Create clusters using several Map-Reduce jobs (at least 2). At each
     * iteration, the number of reducers is 2 times smaller (until reached 0)
     * while clusters' size get slightly higher (until T1,
     * T2). Last job is a Map-only job that merge created clusters into a
     * single sequence file.
     *
     * @param conf    the Configuration
     * @param input   the Path to the directory containing input vectors
     * @param output  the Path for all output directories
     * @param measure the DistanceMeasure
     * @param finalT1 the double T1 distance metric
     * @param finalT2 the double T2 distance metric
     */
    public static long buildClusters(Configuration conf, Path input,
                                     Path output, int reducers,
                                     SequenceDistanceMeasure measure,
                                     float finalT1, float finalT2)
            throws IOException, InterruptedException, ClassNotFoundException {


        conf.set(SequenceCanopyConfigKeys.DISTANCE_MEASURE_KEY,
                measure.getClass().getName());
        conf.setFloat(SequenceCanopyConfigKeys.T1_KEY, finalT1);
        conf.setFloat(SequenceCanopyConfigKeys.T2_KEY, finalT2);
        conf.setFloat(SequenceCanopyConfigKeys.MAX_DISTANCE_MEASURE, finalT1);

        // Prepare job iteration
        int numIterations =
                (int) Math.floor(Math.log(reducers) / Math.log(2)) + 2;
        float t1It = finalT1 / (numIterations + 1);
        float t2It = finalT2 / (numIterations + 1);
        float t1 = t1It;
        float t2 = t2It;
        int iteration = 0;

        Path clustersInput = input;
        Path clustersOutput =
                new Path(output, SequenceCluster.INITIAL_CLUSTERS_DIR);

        boolean last = false;
        long canopies = 0L;
        while (reducers >= 0 && !last) {

            iteration++;
            if (reducers == 0)
                last = true;

            LOGGER.info("Job      : {}/{}", iteration, numIterations);
            LOGGER.info("T1       : {}", t1);
            LOGGER.info("T2       : {}", t2);
            LOGGER.info("Input    : {}", clustersInput.toString());
            LOGGER.info("Output   : {}", clustersOutput.toString());
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
            canopyJob.setJarByClass(SequenceClusteringDriver.class);
            canopyJob.setNumReduceTasks(reducers);
            canopyJob.setMapOutputKeyClass(Text.class);
            canopyJob.setMapOutputValueClass(ArrayPrimitiveWritable.class);
            canopyJob.setOutputKeyClass(Text.class);
            canopyJob.setOutputValueClass(ArrayPrimitiveWritable.class);
            canopyJob.setInputFormatClass(SequenceFileInputFormat.class);
            canopyJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileInputFormat.addInputPath(canopyJob, clustersInput);
            SequenceFileOutputFormat.setOutputPath(canopyJob, clustersOutput);

            // Submit job
            if (!canopyJob.waitForCompletion(true)) {
                throw new IOException(
                        "MapReduce execution failed, please check " +
                                canopyJob.getTrackingURL());
            }

            canopies = canopyJob.getCounters()
                    .findCounter(SequenceCanopyCreateReducer.COUNTER,
                            SequenceCanopyCreateReducer.COUNTER_CANOPY)
                    .getValue();

            // Get 2 times less reducers at next step
            reducers = reducers / 2;

            // Get slightly larger clusters at next step
            t1 += t1It;
            t2 += t2It;

            // Output of previous job will be input as next one
            clustersInput = clustersOutput;

            String newDir;
            if (reducers == 0) {
                newDir = SequenceCluster.CLUSTERS_DIR + 0 +
                        SequenceCluster.FINAL_ITERATION_SUFFIX;
            } else {
                newDir = SequenceCluster.CLUSTERS_DIR + iteration;
            }
            clustersOutput = new Path(output, newDir);

        }

        return canopies;

    }

}
