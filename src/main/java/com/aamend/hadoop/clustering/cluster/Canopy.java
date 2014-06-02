package com.aamend.hadoop.clustering.cluster;

import com.aamend.hadoop.clustering.distance.DistanceMeasure;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Author: antoine.amend@gmail.com
 * Date: 21/03/14
 */
public class Canopy implements Cluster {

    public static final String MIN_OBSERVATIONS = "min.cluster.observations";
    public static final String MIN_SIMILARITY = "min.cluster.similarity";
    public static final String CLUSTER_T1 = "cluster.t1";
    public static final String CLUSTER_T2 = "cluster.t2";
    public static final String CLUSTER_MEASURE = "cluster.measure";
    public static final String MAX_DISTANCE = "cluster.max.measure";
    public static final String LAST_ITERATION = "cluster.last.iteration";

    private int id;
    private long num;
    private int[] center;

    public Canopy(int id, int[] center) {
        num = 1;
        this.id = id;
        this.center = center;
    }

    public Canopy(int id, int[] center, long num) {
        this.id = id;
        this.center = center;
        this.num = num;
    }

    @Override
    public double pdf(int[] x, DistanceMeasure measure) {
        return 1 - measure.distance(center, x);
    }

    @Override
    public void observe(long number) {
        num += number;
    }

    @Override
    public void computeCenter(List<int[]> centers, DistanceMeasure measure) {

        if(centers.size() <= 1){
            return;
        }

        double[] averages = new double[centers.size()];
        for (int i = 0; i < centers.size(); i++) {
            double average = 0.0d;
            // Consider center i
            // Compute distance to other points
            for (int j = 0; j < centers.size(); j++) {
                if (j != i) {
                    average += measure.distance(centers.get(i), centers.get(j));
                }
            }
            averages[i] = average / (centers.size() - 1);
        }

        double min = Double.MAX_VALUE;
        int minIdx = 0;
        for (int i = 0; i < averages.length; i++) {
            double average = averages[i];
            if (average < min) {
                min = average;
                minIdx = i;
            }
        }

        this.center = centers.get(minIdx);

    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public int[] getCenter() {
        return center;
    }

    @Override
    public long getNum() {
        return num;
    }

    @Override
    public String asFormattedString() {
        StringBuilder sb = new StringBuilder(50);
        return sb.append("C-").append(id)
                .append(" {n:").append(num)
                .append(",c:").append(Arrays.toString(center))
                .append("}")
                .toString();
    }

    public static DistanceMeasure configureMeasure(Configuration conf) throws IOException {

        String className = conf.get(CLUSTER_MEASURE);
        if (StringUtils.isEmpty(className)) {
            throw new IllegalArgumentException(
                    "Distance measure is empty. " +
                            "It should be specified from Hadoop configuration"
            );
        }
        DistanceMeasure measure;
        try {
            Class<?> clazz = Class.forName(className);
            Object obj = clazz.newInstance();
            measure = (DistanceMeasure) obj;
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        } catch (InstantiationException e) {
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }

        // Configure distance measure
        measure.configure(conf);
        return measure;
    }

}
