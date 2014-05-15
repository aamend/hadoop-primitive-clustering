package com.aamend.hadoop.mahout.sequence.job;

import com.aamend.hadoop.mahout.sequence.cluster.CanopyConfigKeys;
import com.aamend.hadoop.mahout.sequence.cluster.Cluster;
import com.aamend.hadoop.mahout.sequence.distance.DistanceMeasure;
import com.aamend.hadoop.mahout.sequence.mapreduce.*;
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
import java.util.UUID;

public class ClusterDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            ClusterDriver.class);

    /**
     * Build a directory of Canopy clusters from the input arguments.
     * Create clusters using several Map-Reduce jobs (at least 2). At each
     * iteration, the number of reducers is 2 times smaller (until reached 1)
     * while clusters' size get slightly higher (until T1, T2).
     *
     * @param conf     the Configuration
     * @param input    the Path to the directory containing input arrays
     * @param output   the Path for all output directories
     * @param reducers the number of reducers to use
     * @param measure  the DistanceMeasure
     * @param finalT1  the double T1 distance metric
     * @param finalT2  the double T2 distance metric
     * @param minObs   the minimum observation per cluster
     * @return the number of created clusters
     */
    public static long buildClusters(Configuration conf, Path input,
                                     Path output, int reducers,
                                     DistanceMeasure measure,
                                     float finalT1, float finalT2, long minObs)
            throws IOException, InterruptedException, ClassNotFoundException {

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            throw new IOException("Output path " + output + " already exists");
        }

        // Prepare job iteration
        int numIterations =
                (int) Math.floor(Math.log(reducers) / Math.log(2)) + 1;
        float t1It = finalT1 / numIterations;
        float t2It = finalT2 / numIterations;
        float t1 = t1It;
        float t2 = t2It;
        int iteration = 0;
        long canopies = 0;

        // An additional iteration will be needed to filter out clusters
        numIterations++;

        Path clustersInput = input;
        Path tmpPath = new Path("/tmp/" + Cluster.INITIAL_CLUSTERS_DIR +
                UUID.randomUUID().toString().toUpperCase());
        Path clustersOutput = new Path(tmpPath, Cluster.INITIAL_CLUSTERS_DIR);

        while (reducers >= 1) {

            iteration++;

            LOGGER.info("Job      : {}/{}", iteration, numIterations);
            LOGGER.info("T1       : {}", t1);
            LOGGER.info("T2       : {}", t2);
            LOGGER.info("Input    : {}", clustersInput.toString());
            LOGGER.info("Output   : {}", clustersOutput.toString());
            LOGGER.info("Reducers : {}", reducers);

            // Add job specific configuration
            conf.set(CanopyConfigKeys.DISTANCE_KEY,
                    measure.getClass().getName());
            conf.setFloat(CanopyConfigKeys.T1_KEY, t1);
            conf.setFloat(CanopyConfigKeys.T2_KEY, t2);
            conf.setFloat(CanopyConfigKeys.MAX_DISTANCE_MEASURE, t1);

            // Prepare job
            Job createJob = new Job(conf,
                    "Create clusters - " + iteration + "/" + numIterations);
            createJob.setMapperClass(ClusterCreateMapper.class);
            createJob.setReducerClass(ClusterCreateReducer.class);
            createJob.setJarByClass(ClusterDriver.class);
            createJob.setNumReduceTasks(reducers);
            createJob.setMapOutputKeyClass(Text.class);
            createJob.setMapOutputValueClass(ArrayPrimitiveWritable.class);
            createJob.setOutputKeyClass(Text.class);
            createJob.setOutputValueClass(ArrayPrimitiveWritable.class);
            createJob.setInputFormatClass(SequenceFileInputFormat.class);
            createJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileInputFormat.addInputPath(createJob, clustersInput);
            SequenceFileOutputFormat.setOutputPath(createJob, clustersOutput);

            // Submit job
            if (!createJob.waitForCompletion(true)) {
                throw new IOException(
                        "MapReduce execution failed, please check " +
                                createJob.getTrackingURL());
            }

            canopies = createJob.getCounters()
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
            clustersOutput = new Path(tmpPath, newDir);

        }

        if (canopies == 0) {
            LOGGER.warn("Could not build any canopy");
            return 0;
        }

        LOGGER.info(
                "Now filtering {} canopies with less than {} observations",
                canopies, minObs);

        // Retrieve cluster information
        String finalDir = Cluster.CLUSTERS_DIR + 0 +
                Cluster.FINAL_ITERATION_SUFFIX;
        Path tmpClusterPath = new Path(tmpPath, finalDir);
        Path finalClusterPath = new Path(output, Cluster.CLUSTERS_FINAL_DIR);

        // Make sure cluster directory exist
        if (!fs.exists(tmpClusterPath))
            throw new IOException(
                    "Clusters directory [" + tmpClusterPath +
                            "] does not exist");

        // Retrieve cluster's files (in theory only one
        FileStatus[] fss = fs.listStatus(tmpClusterPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                String name = path.getName();
                return name.contains("part");
            }
        });

        // Make sure cluster's file exists
        if (fss.length == 0)
            throw new IOException(
                    "Clusters sequence files do not exist in directory [" +
                            tmpClusterPath + "]");

        // Add each cluster file (in theory only one) to hadoop distributed cache
        for (FileStatus fileStatus : fss) {
            LOGGER.info("Adding cluster file [" + fileStatus.getPath() +
                    "] to distributed cache");
            DistributedCache.addCacheFile(fileStatus.getPath().toUri(), conf);
        }

        // Add job specific configuration
        conf.set(CanopyConfigKeys.DISTANCE_KEY, measure.getClass().getName());
        conf.setFloat(CanopyConfigKeys.MAX_DISTANCE_MEASURE, finalT1);
        conf.setLong(CanopyConfigKeys.MIN_OBS, minObs);

        // Prepare job
        iteration++;

        LOGGER.info("Job      : {}/{}", iteration, numIterations);
        LOGGER.info("Input    : {}", input.toString());
        LOGGER.info("Output   : {}", finalClusterPath.toString());
        LOGGER.info("Reducers : 1");

        Job filterJob = new Job(conf,
                "Create clusters - " + iteration + "/" + numIterations);
        filterJob.setMapperClass(ClusterFilterMapper.class);
        filterJob.setReducerClass(ClusterFilterReducer.class);
        filterJob.setJarByClass(ClusterDriver.class);
        filterJob.setNumReduceTasks(1);
        filterJob.setMapOutputKeyClass(Text.class);
        filterJob.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        filterJob.setOutputKeyClass(Text.class);
        filterJob.setOutputValueClass(ArrayPrimitiveWritable.class);
        filterJob.setInputFormatClass(SequenceFileInputFormat.class);
        filterJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(filterJob, input);
        SequenceFileOutputFormat.setOutputPath(filterJob, finalClusterPath);

        // Submit job
        if (!filterJob.waitForCompletion(true)) {
            throw new IOException(
                    "MapReduce execution failed, please check " +
                            filterJob.getTrackingURL());
        }

        canopies = filterJob.getCounters()
                .findCounter(ClusterFilterReducer.COUNTER,
                        ClusterFilterReducer.COUNTER_CANOPY)
                .getValue();

        LOGGER.info("{} Clusters available on {}", canopies, tmpClusterPath);
        return canopies;

    }

    /**
     * Build a directory of Canopy clusters from the input arguments and, if
     * requested, cluster the input vectors using these clusters
     *
     * @param conf          the Configuration
     * @param input         the Path to the directory containing input arrays
     * @param output        the Path for all output directories
     * @param measure       the DistanceMeasure
     * @param minSimilarity the minimum similarity to cluster data
     * @param reducers      the number of reducers to use
     */
    public static void clusterData(Configuration conf, Path input,
                                   Path output,
                                   DistanceMeasure measure,
                                   float minSimilarity, int reducers)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Retrieve cluster information
        FileSystem fs = FileSystem.get(conf);
        Path clusterPath = new Path(output, Cluster.CLUSTERS_FINAL_DIR);
        Path dataPath = new Path(output, Cluster.CLUSTERED_POINTS_DIR);

        // Make sure cluster directory exist
        if (!fs.exists(clusterPath))
            throw new IOException(
                    "Clusters directory [" + clusterPath +
                            "] does not exist");

        // Retrieve cluster's files (in theory only one
        FileStatus[] fss = fs.listStatus(clusterPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                String name = path.getName();
                return name.contains("part");
            }
        });

        // Make sure cluster's file exists
        if (fss.length == 0)
            throw new IOException(
                    "Clusters sequence files do not exist in directory [" +
                            clusterPath + "]");

        // Add each cluster file (in theory only one) to hadoop distributed cache
        for (FileStatus fileStatus : fss) {
            LOGGER.info("Adding cluster file [" + fileStatus.getPath() +
                    "] to distributed cache");
            DistributedCache.addCacheFile(fileStatus.getPath().toUri(), conf);
        }

        // Add job specific configuration
        conf.set(CanopyConfigKeys.DISTANCE_KEY, measure.getClass().getName());
        conf.setFloat(CanopyConfigKeys.MIN_SIMILARITY, minSimilarity);
        conf.setFloat(CanopyConfigKeys.MAX_DISTANCE_MEASURE, minSimilarity);

        // Prepare job
        Job job = new Job(conf, "Clustering data");
        job.setMapperClass(ClusterDataMapper.class);
        job.setReducerClass(ClusterDataReducer.class);
        job.setJarByClass(ClusterDriver.class);
        job.setNumReduceTasks(reducers);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArrayPrimitiveWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, dataPath);

        // Submit job
        if (!job.waitForCompletion(true)) {
            throw new IOException(
                    "MapReduce execution failed, please check " +
                            job.getTrackingURL());
        }

        LOGGER.info("Clustered points available on {}", clusterPath);

    }


}
