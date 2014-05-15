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
     * iteration, the number of reducers is 2 times smaller (until reached 1).
     *
     * @param conf     the Configuration
     * @param input    the Path containing input arrays
     * @param output   the final Path where clusters / data will be written to
     * @param reducers the number of reducers to use (at least 1)
     * @param measure  the DistanceMeasure
     * @param t1       the double CLUSTER_T1 distance metric
     * @param t2       the double CLUSTER_T2 distance metric
     * @param cf       the minimum observations per cluster
     * @return the number of created clusters
     */
    public static long buildClusters(Configuration conf, Path input,
                                     Path output, int reducers,
                                     DistanceMeasure measure,
                                     float t1, float t2, long cf)
            throws IOException, InterruptedException, ClassNotFoundException {

        // Mount FileSystem
        FileSystem fileSystem = FileSystem.get(conf);

        // Compute number of required iterations
        if (reducers < 1) {
            throw new IllegalArgumentException(
                    "Number of reducers must be greater or equals to 1");
        }
        double num = Math.floor(Math.log(reducers) / Math.log(2)) + 1;
        int itTotal = (int) num;
        int it = 0;
        long canopies = 0;

        float itT1Increase = t1 / itTotal;
        float itT2Increase = t2 / itTotal;
        float itT1 = itT1Increase;
        float itT2 = itT2Increase;

        itTotal++;

        // Prepare input, output and temporary path
        String uuid = UUID.randomUUID().toString().toUpperCase();
        Path finPath = new Path(output, Cluster.CLUSTERS_FINAL_DIR);
        Path tmpPath = new Path("/tmp/" + Cluster.CLUSTERS_TMP_DIR + uuid);
        Path itOPath = new Path(tmpPath, Cluster.CLUSTERS_TMP_DIR + it);
        Path itIPath = input;

        // Make sure output path does not exist
        if (fileSystem.exists(output)) {
            throw new IOException("Output path " + output + " already exists");
        }

        // Start MapReduce iteration
        // Read initial arrays and cluster them
        // Read clusters' arrays and cluster them
        // ../..
        // Until one final reducer
        while (reducers >= 1) {

            it++;

            LOGGER.info("Job      : {}/{}", it, itTotal);
            LOGGER.info("T1       : {}", itT1);
            LOGGER.info("T2       : {}", itT2);
            LOGGER.info("Reducers : {}", reducers);
            LOGGER.info("Input    : {}", itIPath.toString());
            LOGGER.info("Output   : {}", itOPath.toString());

            // Add job specific configuration
            String measureClass = measure.getClass().getName();
            conf.set(CanopyConfigKeys.CLUSTER_MEASURE, measureClass);
            conf.setFloat(CanopyConfigKeys.CLUSTER_T1, itT1);
            conf.setFloat(CanopyConfigKeys.CLUSTER_T2, itT2);
            conf.setFloat(CanopyConfigKeys.MAX_DISTANCE, itT1);

            // Prepare job
            String name = "Create clusters - " + it + "/" + itTotal;
            Job createJob = new Job(conf, name);
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
            SequenceFileInputFormat.addInputPath(createJob, itIPath);
            SequenceFileOutputFormat.setOutputPath(createJob, itOPath);

            // Submit job
            if (!createJob.waitForCompletion(true)) {
                throw new IOException(
                        "MapReduce execution failed, please check " +
                                createJob.getTrackingURL());
            }

            // Retrieve counters
            canopies = createJob.getCounters().findCounter(
                    ClusterCreateReducer.COUNTER,
                    ClusterCreateReducer.COUNTER_CANOPY).getValue();

            // Get 2 times less reducers at next step
            reducers = reducers / 2;

            // Get slightly larger clusters at next step
            itT1 += itT1Increase;
            itT2 += itT2Increase;

            // Output of previous job will be input as next one
            itIPath = itOPath;
            itOPath = new Path(tmpPath, Cluster.CLUSTERS_TMP_DIR + it);

        }

        // Make sure we have at least one canopy created
        if (canopies == 0)
            throw new IOException(
                    "Could not build any canopy. " +
                            "Please check your input arrays " +
                            "and / or your {T1,T2} parameters");

        // Retrieve cluster's files (in theory only one)
        FileStatus[] fss = fileSystem.listStatus(itOPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                String name = path.getName();
                return name.contains("part-r");
            }
        });
        if (fss.length == 0) {
            throw new IOException(
                    "Clusters sequence file(s) do not exist in directory [" +
                            itOPath + "]");
        }

        // Start second set of MapReduce
        // Read initial arrays and cluster them
        // Filter out clusters with less than MIN_OBSERVATION arrays

        it++;

        LOGGER.info("Job      : {}/{}", it, itTotal);
        LOGGER.info("Reducers : 1");
        LOGGER.info("Input    : {}", input.toString());
        LOGGER.info("Output   : {}", finPath.toString());

        // Add each cluster file to distributed cache
        for (FileStatus fs : fss) {
            LOGGER.info("Cache    : {}", fs.getPath());
            DistributedCache.addCacheFile(fs.getPath().toUri(), conf);
        }

        // Add job specific configuration
        String measureClass = measure.getClass().getName();
        conf.set(CanopyConfigKeys.CLUSTER_MEASURE, measureClass);
        conf.setLong(CanopyConfigKeys.MIN_OBSERVATIONS, cf);
        conf.setFloat(CanopyConfigKeys.MAX_DISTANCE, t1);

        // Prepare next job
        String name = "Create clusters - " + it + "/" + itTotal;
        Job filterJob = new Job(conf, name);
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
        SequenceFileOutputFormat.setOutputPath(filterJob, finPath);

        // Submit job
        if (!filterJob.waitForCompletion(true)) {
            throw new IOException(
                    "MapReduce execution failed, please check " +
                            filterJob.getTrackingURL());
        }

        // Retrieve counters
        canopies = filterJob.getCounters().findCounter(
                ClusterFilterReducer.COUNTER,
                ClusterFilterReducer.COUNTER_CANOPY).getValue();

        // Make sure we have at least one canopy created
        if (canopies == 0)
            throw new IOException(
                    "Could not build any canopy. " +
                            "Please check your input arrays " +
                            "and / or your {T1,T2} parameters");

        LOGGER.info("{} canopies available on {}", canopies, finPath);
        return canopies;

    }

    /**
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
        FileSystem fileSystem = FileSystem.get(conf);
        Path dataPath = new Path(output, Cluster.CLUSTERED_POINTS_DIR);
        Path clusterPath = new Path(output, Cluster.CLUSTERS_FINAL_DIR);

        // Make sure cluster directory exist
        if (!fileSystem.exists(clusterPath))
            throw new IOException(
                    "Clusters directory [" + clusterPath +
                            "] does not exist");

        // Make sure data directory does not exist
        if (fileSystem.exists(dataPath))
            throw new IOException(
                    "Data directory [" + clusterPath +
                            "] already exists");

        // Retrieve cluster's files (in theory only one)
        FileStatus[] fss = fileSystem.listStatus(clusterPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                String name = path.getName();
                return name.contains("part-r");
            }
        });
        if (fss.length == 0) {
            throw new IOException(
                    "Clusters sequence file(s) do not exist in directory [" +
                            clusterPath + "]");
        }

        LOGGER.info("Job      : 1/1");
        LOGGER.info("Reducers : {}", reducers);
        LOGGER.info("Input    : {}", input.toString());
        LOGGER.info("Output   : {}", dataPath.toString());

        // Add each cluster file to distributed cache
        for (FileStatus fs : fss) {
            LOGGER.info("Cache    : {}", fs.getPath());
            DistributedCache.addCacheFile(fs.getPath().toUri(), conf);
        }

        // Add job specific configuration
        String measureClass = measure.getClass().getName();
        conf.set(CanopyConfigKeys.CLUSTER_MEASURE, measureClass);
        conf.setFloat(CanopyConfigKeys.MIN_SIMILARITY, minSimilarity);
        conf.setFloat(CanopyConfigKeys.MAX_DISTANCE, 1 - minSimilarity);

        // Prepare job
        Job clusterJob = new Job(conf, "Clustering data - 1/1");
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
        FileOutputFormat.setOutputPath(clusterJob, dataPath);

        // Submit job
        if (!clusterJob.waitForCompletion(true)) {
            throw new IOException(
                    "MapReduce execution failed, please check " +
                            clusterJob.getTrackingURL());
        }

        // Retrieve counters
        long clusteredPoints = clusterJob.getCounters().findCounter(
                ClusterDataMapper.COUNTER,
                ClusterDataMapper.COUNTER_CLUSTERED).getValue();

        // Retrieve counters
        long nonClusteredPoints = clusterJob.getCounters().findCounter(
                ClusterDataMapper.COUNTER,
                ClusterDataMapper.COUNTER_NON_CLUSTERED).getValue();

        if (nonClusteredPoints > 0)
            LOGGER.warn("{} points could not have been clustered",
                    nonClusteredPoints);

        // Make sure we have at least one point clustered
        if (clusteredPoints == 0) {
            LOGGER.error(
                    "Could not cluster any data. " +
                            "Please check both your input arrays " +
                            "and clusters' centers");
        } else {
            LOGGER.info("{} points have been clustered - Data available on {}",
                    clusteredPoints, clusterPath);
        }
    }


}
