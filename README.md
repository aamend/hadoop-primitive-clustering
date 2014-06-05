Hadoop Primitive Array Clustering
==============

Hadoop implementation of Canopy Clustering using Levenshtein distance algorithm and other non-mathematical distance measures (such as Jaccard coefficient).

Difference with Mahout
----

One of the major limitation of Mahout is that the clustering algorithms (K-Means or Canopy clustering) use a mathematical approach in order to compute Clusters' centers. Each time a new point is added to a cluster, Mahout framework recomputes cluster's center as an average of data points.

```
NewCenter[i] = Sum(Vectors)[i] / observations
```

As a result, only purely mathematical *DistanceMeasure* can be used. But...
- What if your data set is composed of non-mathematical primitive data points (**char**, **boolean**) ?
- What if an average of points does not make any sense for your business ? 
- Or simply what if you wish to use a non (or less) mathematical distance measure ? 

Motivations
----

I had to create canopies for sequences of IDs (Integer). Let's take the following example with 2 vectors V1 and V2.

```
V1={0:123, 1:23, 2:55,  3:141, 4:22}
V2={0:23,  1:55, 2:141, 3:22}
```
These vectors are totally different using most of standard Mathematical measures Mahout provides (e.g. *Euclidean*). I can still change the way my vectors are created, but none of the solution I tried were considering my arrays as a **sequence of IDs** and furthermore a sequence of IDs **where the order matters**. *Levensthein* metric (that is usually used for fuzzy string matching) is a perfect match as it compares sequences of IDs and not only IDs as numbers. 

- I had to create a new set of *DistanceMeasure* taking arrays as Input parameters.

Besides, assuming both of them belongs to a same cluster, does a new cluster's center V' (made as an average of points in both V1 and V2) makes sense for sequence analysis ? 
```
V'={0:(23+123)/2, 1:(55+23)/2, 2:(141+55)/2, 3:(22+141)/2, 4:(0+22)/1}
```
- I had to find a way to override Mahout cluster's center computation. Instead of computing an average of data points, I find the point Pi that minimizes the distance across all cluster's data points. 

Pseudo code:
```
Point min_point = Pi
float min_dist  = Infinity
For each point Pi
  For each point Pj
     Compute distance Pi->Pj
     Update min_point if distance < min_dist

Center = minimum
```

Distance Measures
----

Supported distance measures are 
- com.aamend.hadoop.clustering.distance.LevenshteinDistance measure
- com.aamend.hadoop.clustering.distance.TanimotoDistance measure (a.k.a Jaccard Coefficient).
- Any DistanceMeasure implementing com.aamend.hadoop.clustering.distance.DistanceMeasure

Primitive Arrays
----

Only **Integer.class** is supported on Version 1.0. It is planned however to support any of the Java primitive arrays (**boolean**[], **char**[], **int**[], **double**[], **long**[], **float**[]). I invite you to actively contribute to this project.


Dependencies
----

Even though the project has been directly inspired by Mahout canopy clustering, it does not depend on any of Mahout libraries. Instead of using Mahout *Vector*, I use arrays of Integer, and instead of Mahout *VectorWritable*, I use Hadoop *ArrayPrimitiveWritable*. Simply add the maven dependency to your project. Releases versions should be available on Maven Central (synched from Sonatype). Even though this project (actively depends on Hadoop libraries) has been built around Hadoop CDH4 distribution, this can be easily overridden on client side by using maven "exclusion" tag in order to use any of the Hadoop versions / distributions.

```
    <dependency>
        <groupId>com.aamend.hadoop</groupId>
        <artifactId>hadoop-primitive-clustering</artifactId>
        <version>1.0</version>
    </dependency>
```


Usage
----

### Create canopies

Use *buildClusters* static method from *com.aamend.hadoop.clustering.job.CanopyDriver* class

```
     /**
     * @param conf     the Hadoop Configuration
     * @param input    the Path containing input PrimitiveArraysWritable
     * @param output   the final Path where clusters / data will be written to
     * @param reducers the number of reducers to use (at least 1)
     * @param measure  the DistanceMeasure
     * @param t1       the float CLUSTER_T1 distance metric
     * @param t2       the float CLUSTER_T2 distance metric
     * @param cf       the minimum observations per cluster
     * @return the number of created canopies
     */
    public static long buildClusters(Configuration conf, Path input,
                                     Path output, int reducers,
                                     DistanceMeasure measure,
                                     float t1, float t2, long cf)
```

This will build Canopies using several Map-Reduce jobs (at least 2, driven by the initial number of reducers). Firstly, because we need to keep track of each observed point per clusters in order to minimize intra-distance of data points (obviously cannot fit in memory), Secondly because the measure used here might be fairly inneficient using a single Map job (*Levenshtein* complexitiy is O(n\*m)). In order to allow a smooth run without any hot spot, at each iteration, the number of reducers is 2 times smaller (until reached 1) while {T1,T2} parameters gets slightly larger (starts with half of the required size). Clustering algorithm is defined according to the supplied *DistanceMeasure* (can be a custom measure implementing DistanceMeasure assuming it is available on Hadoop classpath). 

The **input** data should be a sequenceFile format using any key class (implementing *WritableComparable* interface) and value should be *ArrayPrimitiveWritable* (serializing integer array). 

The **output** will be a sequenceFile format using Cluster Id as key (*IntWritable*) and *com.aamend.hadoop.clustering.clusterCanopyWritable* as value.

### Cluster input data

Once canopies are created, use static *clusterData* method from *com.aamend.hadoop.clustering.job.CanopyDriver* class

```
     /**
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
```

This will retrieve the most probable clusters any point should belongs to. If not 100% identical to cluster's center, we cluster data if similarity is greater than X% (minSimilarity). Canopies (created at previous steps) are added to Distributed cache. 

The **output** will be a sequenceFile format using Cluster Id as key (*IntWritable*) and *ObjectWritable* as value (object pointing to your initial *WritableComparable* key so that you can keep track of which point belongs to which cluster)

License
----

Apache License, Version 2.0

Author
----

Antoine Amend
<antoine.amend@gmail.com>
