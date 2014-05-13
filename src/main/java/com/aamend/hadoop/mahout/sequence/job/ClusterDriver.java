package com.aamend.hadoop.mahout.sequence.job;

import com.aamend.hadoop.mahout.sequence.cluster.CanopyConfigKeys;
import com.aamend.hadoop.mahout.sequence.cluster.Cluster;
import com.aamend.hadoop.mahout.sequence.distance.DistanceMeasure;
import com.aamend.hadoop.mahout.sequence.mapreduce.ClusterCreateMapper;
import com.aamend.hadoop.mahout.sequence.mapreduce.ClusterCreateReducer;
import com.aamend.hadoop.mahout.sequence.mapreduce.ClusterDataMapper;
import com.aamend.hadoop.mahout.sequence.mapreduce.ClusterDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ClusterDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            ClusterDriver.class);

    /**
     * Build a directory of Canopy clusters from the input arguments.
     * Create clusters using several Map-Reduce jobs (at least 2). At each
     * iteration, the number of reducers is 2 times smaller (until reached 0)
     * while clusters' size get slightly higher (until T1,
     * T2). Last job is a Map-only job that merge created clusters into a
     * single sequence file.
     *
     * @param conf     the Configuration
     * @param input    the Path to the directory containing input arrays
     * @param output   the Path for all output directories
     * @param reducers the number of reducers to use
     * @param measure  the DistanceMeasure
     * @param finalT1  the double T1 distance metric
     * @param finalT2  the double T2 distance metric
     * @return the number of created clusters
     */
    public static long buildClusters(Configuration conf, Path input,
                                     Path output, int reducers,
                                     DistanceMeasure measure,
                                     float finalT1, float finalT2)
            throws IOException, InterruptedException, ClassNotFoundException {

        // Prepare job iteration
        int numIterations =
                (int) Math.floor(Math.log(reducers) / Math.log(2)) + 1;
        float t1It = finalT1 / numIterations;
        float t2It = finalT2 / numIterations;
        float t1 = t1It;
        float t2 = t2It;
        int iteration = 0;
        long canopies = 0;

        Path clustersInput = input;
        Path clustersOutput = new Path(output, Cluster.INITIAL_CLUSTERS_DIR);

        while (reducers >= 1) {

            iteration++;
            int remaining = numIterations - iteration;

            LOGGER.info("Job      : {}/{}", iteration, numIterations);
            LOGGER.info("T1       : {}", t1);
            LOGGER.info("T2       : {}", t2);
            LOGGER.info("Input    : {}", clustersInput.toString());
            LOGGER.info("Output   : {}", clustersOutput.toString());
            LOGGER.info("Reducers : {}", reducers);

            // Add job specific configuration
            conf.set(CanopyConfigKeys.DISTANCE_MEASURE_KEY,
                    measure.getClass().getName());
            conf.setFloat(CanopyConfigKeys.T1_KEY, t1);
            conf.setFloat(CanopyConfigKeys.T2_KEY, t2);
            conf.setFloat(CanopyConfigKeys.MAX_DISTANCE_MEASURE, t1);

            // Prepare job
            Job canopyJob = new Job(conf,
                    "Create clusters - " + iteration + "/" + numIterations);
            canopyJob.setMapperClass(ClusterCreateMapper.class);
            canopyJob.setReducerClass(ClusterCreateReducer.class);
            canopyJob.setJarByClass(ClusterDriver.class);
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
                    .findCounter(ClusterCreateReducer.COUNTER,
                            ClusterCreateReducer.COUNTER_CANOPY)
                    .getValue();

            // Get 2 times less reducers at next step
            reducers = reducers / 2;

            // Get slightly larger clusters at next step
            t1 += t1It;
            t2 += t2It;

            // Output of previous job will be input as next one
            String newDir;
            if (reducers == 1) {
                // Output of previous job will be input as next one
                newDir = Cluster.CLUSTERS_DIR + 0 +
                        Cluster.FINAL_ITERATION_SUFFIX;
            } else {
                // Output of previous job will be input as next one
                newDir = Cluster.CLUSTERS_DIR + iteration;
            }

            clustersInput = clustersOutput;
            clustersOutput = new Path(output, newDir);

        }

        LOGGER.info("{} clusters created, available in {}", canopies,
                clustersOutput);

        return canopies;

    }

    /**
     * Build a directory of Canopy clusters from the input arguments and, if
     * requested, cluster the input vectors using these clusters
     *
     * @param conf     the Configuration
     * @param input    the Path to the directory containing input arrays
     * @param output   the Path for all output directories
     * @param measure  the DistanceMeasure
     * @param finalT1  the double T1 distance metric used for clustering
     * @param minPdf   the minimum probability to belong to a cluster
     * @param minObs   the minimum number of observations per cluster
     * @param reducers the number of reducers to use
     */
    public static void clusterData(Configuration conf, Path input,
                                   Path output,
                                   DistanceMeasure measure,
                                   float finalT1, float minPdf, int minObs,
                                   int reducers)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Retrieve cluster information
        FileSystem fs = FileSystem.get(conf);
        String finalDir = Cluster.CLUSTERS_DIR + 0 +
                Cluster.FINAL_ITERATION_SUFFIX;
        Path finalClusterPath = new Path(output, finalDir);
        Path finalDataPath = new Path(output, Cluster.CLUSTERED_POINTS_DIR);

        // Make sure cluster directory exist
        if (!fs.exists(finalClusterPath))
            throw new IOException(
                    "Clusters directory [" + finalClusterPath +
                            "] does not exist");

        // Retrieve cluster's files (in theory only one
        FileStatus[] fss = fs.listStatus(finalClusterPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                String name = path.getName();
                return name.contains("part");
            }
        });

        // Make sure cluster's files exist
        if (fss.length == 0)
            throw new IOException(
                    "Clusters sequence files do not exist in directory [" +
                            finalClusterPath + "]");

        // Add each cluster file (in theory only one) to hadoop distributed cache
        for (FileStatus fileStatus : fss) {
            LOGGER.info("Adding cluster file [" + fileStatus.getPath() +
                    "] to distributed cache");
            DistributedCache.addCacheFile(fileStatus.getPath().toUri(), conf);
        }

        // Add job specific configuration
        conf.set(CanopyConfigKeys.DISTANCE_MEASURE_KEY,
                measure.getClass().getName());
        conf.setFloat(CanopyConfigKeys.MAX_DISTANCE_MEASURE, finalT1);
        conf.setFloat(CanopyConfigKeys.MIN_PDF, minPdf);

        // Prepare job
        Job clusterJob = new Job(conf, "Cluster data");
        clusterJob.setMapperClass(ClusterDataMapper.class);
        clusterJob.setReducerClass(ClusterDataReducer.class);
        clusterJob.setJarByClass(ClusterDriver.class);
        clusterJob.setNumReduceTasks(reducers);
        clusterJob.setMapOutputKeyClass(Text.class);
        clusterJob.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        clusterJob.setOutputKeyClass(Text.class);
        clusterJob.setOutputValueClass(ArrayPrimitiveWritable.class);
        clusterJob.setInputFormatClass(SequenceFileInputFormat.class);
        clusterJob.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(clusterJob, input);
        FileOutputFormat.setOutputPath(clusterJob, finalDataPath);

        // Submit job
        if (!clusterJob.waitForCompletion(true)) {
            throw new IOException(
                    "MapReduce execution failed, please check " +
                            clusterJob.getTrackingURL());
        }

        LOGGER.info("Clustered points available on {}", finalClusterPath);

    }


}
