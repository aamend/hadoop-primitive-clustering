package com.aamend.hadoop.clustering.job;

import com.aamend.hadoop.clustering.cluster.Canopy;
import com.aamend.hadoop.clustering.cluster.CanopyWritable;
import com.aamend.hadoop.clustering.cluster.Cluster;
import com.aamend.hadoop.clustering.distance.DistanceMeasure;
import com.aamend.hadoop.clustering.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.UUID;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public class CanopyDriver {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CanopyDriver.class);

    /**
     * Build a directory of Canopies from the input arguments.
     * Create clusters using several Map-Reduce jobs (at least 2 - driven by
     * the initial number of reducers). At each iteration, the number of
     * reducers is 2 times smaller (until reached 1) while {T1,T2} parameters
     * gets slightly larger (starts with half of the required size).
     * Note the extensive use of /tmp directory at each iteration.
     * Clustering algorithm is defined according to the supplied DistanceMeasure
     * (can be a custom measure implementing DistanceMeasure assuming it is available
     * on Hadoop classpath). At final step, canopies with less than CF observations will be rejected.
     * <p/>
     * - MAP_1    :     Read initial arrays and cluster them.
     * Output cluster's centers as array
     * <p/>
     * - REDUCE_1 :     Recompute cluster's center by minimizing distance.
     * Output cluster's centers as array
     * <p/>
     * - ../..
     * <p/>
     * - MAP_N    :     Read cluster's centers arrays and re-cluster them.
     * Output cluster's centers as array
     * <p/>
     * - REDUCE_N :     Recompute cluster's center by minimizing distance.
     * Output cluster's centers as array
     * <p/>
     * <p/>
     * - Input should be of <key>WritableComparable</key> and <value>ArrayPrimitiveWritable</value>
     * - Input should be of <format>SequenceFile</format>
     * <p/>
     * - Output will be of <key>Text (dummy)</key> and <value>CanopyWritable</value>
     * - Output will be of <format>SequenceFile</format>
     *
     * @param conf     the Hadoop Configuration
     * @param input    the Path containing input arrays
     * @param output   the final Path where clusters / data will be written to
     * @param reducers the number of reducers to use (at least 1)
     * @param measure  the DistanceMeasure
     * @param t1       the double CLUSTER_T1 distance metric
     * @param t2       the double CLUSTER_T2 distance metric
     * @param cf       the minimum observations per cluster
     * @return the number of created canopies
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
            throw new IllegalArgumentException("Number of reducers must be greater or equals to 1");
        }

        double num = Math.floor(Math.log(reducers) / Math.log(2)) + 1;
        int itTotal = (int) num;
        int it = 0;
        long canopies = 0;

        // Compute how {T1,T2} will be increased after each iteration
        float minT1 = t1 * 0.5f;
        float minT2 = t2 * 0.5f;
        float itT1Increase = minT1 / (itTotal - 1);
        float itT2Increase = minT2 / (itTotal - 1);
        float itT1 = minT1;
        float itT2 = minT2;

        // Prepare input, output and temporary path
        Path tmp = new Path("/tmp/CLUSTERING-" + UUID.randomUUID().toString().toUpperCase());
        Path itOPath = new Path(tmp, Cluster.CLUSTERS_TMP_DIR + it);
        Path itIPath = input;

        // Make sure output path does not exist
        if (fileSystem.exists(output)) {
            throw new IOException("Output path " + output + " already exists");
        }

        // Make sure tmp path exists
        if (fileSystem.exists(tmp)) {
            fileSystem.mkdirs(tmp);
        }

        // Start job iteration
        while (reducers >= 1) {

            it++;
            boolean lastIteration = (reducers == 1);

            // Add job specific configuration
            String measureClass = measure.getClass().getName();
            conf.set(Canopy.CLUSTER_MEASURE, measureClass);
            conf.setFloat(Canopy.CLUSTER_T1, itT1);
            conf.setFloat(Canopy.CLUSTER_T2, itT2);
            conf.setFloat(Canopy.MAX_DISTANCE, itT1);
            conf.setBoolean(Canopy.LAST_ITERATION, lastIteration);
            conf.setLong(Canopy.MIN_OBSERVATIONS, cf);

            // Write last iteration to final directory
            if (lastIteration) {
                itOPath = output;
            }

            String name = "Create clusters - " + it + "/" + itTotal;
            LOGGER.info("************************************");
            LOGGER.info("Job      : {}", name);
            LOGGER.info("Reducers : {}", reducers);
            LOGGER.info("Input    : {}", itIPath.toString());
            LOGGER.info("Output   : {}", itOPath.toString());
            LOGGER.info("T1       : {}", round(itT1));
            LOGGER.info("T2       : {}", round(itT2));
            LOGGER.info("Last.it  : {}", lastIteration);
            LOGGER.info("MinObs.  : {}", cf);
            LOGGER.info("************************************");

            // Prepare job
            Job createJob = new Job(conf, name);
            if (it == 1) {
                createJob.setMapperClass(CanopyCreateInitMapper.class);
            } else {
                createJob.setMapperClass(CanopyCreateMapper.class);
            }

            createJob.setReducerClass(CanopyCreateReducer.class);
            createJob.setJarByClass(CanopyDriver.class);
            createJob.setNumReduceTasks(reducers);
            createJob.setMapOutputKeyClass(Text.class);
            createJob.setMapOutputValueClass(CanopyWritable.class);
            createJob.setOutputKeyClass(Text.class);
            createJob.setOutputValueClass(CanopyWritable.class);
            createJob.setInputFormatClass(SequenceFileInputFormat.class);
            createJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileInputFormat.addInputPath(createJob, itIPath);
            SequenceFileOutputFormat.setOutputPath(createJob, itOPath);

            // Submit job
            if (!createJob.waitForCompletion(true)) {
                throw new IOException("MapReduce execution failed, please check " + createJob.getTrackingURL());
            }

            // Retrieve counters
            canopies = createJob.getCounters().findCounter(
                    CanopyCreateReducer.COUNTER,
                    CanopyCreateReducer.COUNTER_CANOPY).getValue();

            // Make sure we have at least one canopy created
            if (canopies == 0) {
                LOGGER.error("Could not build any canopy. " +
                        "Please check your input arrays and / or your {T1,T2} parameters");
                if (fileSystem.exists(tmp)) {
                    fileSystem.delete(tmp, true);
                }
                return 0;
            }

            // Get 2 times less reducers at next step
            reducers = reducers / 2;

            // Get slightly larger clusters at next step
            itT1 = itT1 + itT1Increase;
            itT2 = itT2 + itT2Increase;

            // Output of previous job will be input as next one
            itIPath = itOPath;
            itOPath = new Path(tmp, Cluster.CLUSTERS_TMP_DIR + it);
        }

        // Delete temporary directory
        if (fileSystem.exists(tmp)) {
            fileSystem.delete(tmp, true);
        }

        // Make sure we have at least one canopy created
        if (canopies == 0) {
            LOGGER.error("Could not build any canopy. " +
                    "Please check your input arrays and / or your {T1,T2} parameters");
            return 0;
        } else {
            LOGGER.info("{} canopies available on {}", canopies, output);
        }

        return canopies;

    }

    /**
     * Retrieve the most probable cluster a point should belongs to.
     * If not 100% identical to cluster's center, cluster data if and only if
     * his similarity to cluster's center is greater than X%. Canopies (created
     * at previous steps) are added to Distributed cache. The output Key will be
     * the ID of the cluster a point belongs to, and value will be the original Key
     * of the cluster (can be any class WritableComparable).
     * <p/>
     * - Input should be of <key>WritableComparable</key> and <value>ArrayPrimitiveWritable</value>
     * - Input should be of <format>SequenceFile</format>
     * <p/>
     * - Output will be of <key>IntWritable</key> and <value>ObjectWritable</value>
     * - Output will be of <format>SequenceFile</format>
     *
     * @param conf          the Configuration
     * @param inputData     the Path containing input arrays
     * @param dataPath      the final Path where data will be written to
     * @param clusterPath   the path where clusters have been written
     * @param measure       the DistanceMeasure
     * @param minSimilarity the minimum similarity to cluster data
     * @param reducers      the number of reducers to use (at least 1)
     */
    public static void clusterData(Configuration conf, Path inputData,
                                   Path dataPath, Path clusterPath,
                                   DistanceMeasure measure,
                                   float minSimilarity, int reducers)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Retrieve cluster information
        FileSystem fileSystem = FileSystem.get(conf);

        // Make sure cluster directory exists
        if (!fileSystem.exists(clusterPath)) {
            throw new IOException("Clusters directory [" + clusterPath + "] does not exist");
        }

        // Make sure data directory does not exist
        if (fileSystem.exists(dataPath)) {
            throw new IOException("Data directory [" + dataPath + "] already exists");
        }

        // Retrieve cluster's files (in theory only one)
        FileStatus[] fss = fileSystem.listStatus(clusterPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                String name = path.getName();
                return name.contains("part-r");
            }
        });

        if (fss.length == 0) {
            throw new IOException("Clusters sequence file(s) do not exist in directory [" + clusterPath + "]");
        }

        String name = "Clustering data - 1/1";
        LOGGER.info("************************************");
        LOGGER.info("Job      : {}", name);
        LOGGER.info("Reducers : {}", reducers);
        LOGGER.info("Input    : {}", inputData.toString());
        LOGGER.info("Output   : {}", dataPath.toString());
        LOGGER.info("MinSim.  : {}", minSimilarity);
        LOGGER.info("MaxDis.  : {}", minSimilarity);

        // Add each cluster file to distributed cache
        conf.set(ClusterDataMapper.CLUSTERS_FINAL_DIR_CONF, clusterPath.toString());
        for (FileStatus fs : fss) {
            LOGGER.info("Cache    : {}", fs.getPath());
            DistributedCache.addCacheFile(fs.getPath().toUri(), conf);
        }

        LOGGER.info("************************************");

        // Add job specific configuration
        String measureClass = measure.getClass().getName();
        conf.set(Canopy.CLUSTER_MEASURE, measureClass);
        conf.setFloat(Canopy.MIN_SIMILARITY, minSimilarity);
        conf.setFloat(Canopy.MAX_DISTANCE, minSimilarity);

        // Prepare job
        Job clusterJob = new Job(conf, name);
        clusterJob.setMapperClass(ClusterDataMapper.class);
        clusterJob.setReducerClass(ClusterDataReducer.class);
        clusterJob.setJarByClass(CanopyDriver.class);
        clusterJob.setNumReduceTasks(reducers);
        clusterJob.setMapOutputKeyClass(IntWritable.class);
        clusterJob.setMapOutputValueClass(ObjectWritable.class);
        clusterJob.setOutputKeyClass(IntWritable.class);
        clusterJob.setOutputValueClass(ObjectWritable.class);
        clusterJob.setInputFormatClass(SequenceFileInputFormat.class);
        clusterJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(clusterJob, inputData);
        SequenceFileOutputFormat.setOutputPath(clusterJob, dataPath);

        // Submit job
        if (!clusterJob.waitForCompletion(true)) {
            throw new IOException("MapReduce execution failed, please check " + clusterJob.getTrackingURL());
        }

        // Retrieve counters
        long clusteredPoints = clusterJob.getCounters().findCounter(
                ClusterDataMapper.COUNTER,
                ClusterDataMapper.COUNTER_CLUSTERED).getValue();

        // Make sure we have at least one point clustered
        if (clusteredPoints == 0) {
            LOGGER.error("Could not cluster any data. Please check both your input arrays and clusters' centers");
        } else {
            LOGGER.info("{} points have been clustered - Data available on {}", clusteredPoints, dataPath);
        }
    }

    private static float round(float val) {
        DecimalFormat df = new DecimalFormat("#.##");
        return Float.valueOf(df.format(val));
    }

}
