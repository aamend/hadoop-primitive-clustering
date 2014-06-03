package com.aamend.hadoop.clustering.cluster;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

public class CanopyWritable implements Writable {

    private int id;
    private int[] center;
    private long observations;

    private Class<?> centerComponentType = null;
    private Class<?> centerDeclaredComponentType = null;
    private int centerLength;
    private Object centerObject;

    private static final Map<String, Class<?>> PRIMITIVE_NAMES =
            new HashMap<String, Class<?>>(16);

    static {
        PRIMITIVE_NAMES.put(boolean.class.getName(), boolean.class);
        PRIMITIVE_NAMES.put(byte.class.getName(), byte.class);
        PRIMITIVE_NAMES.put(char.class.getName(), char.class);
        PRIMITIVE_NAMES.put(short.class.getName(), short.class);
        PRIMITIVE_NAMES.put(int.class.getName(), int.class);
        PRIMITIVE_NAMES.put(long.class.getName(), long.class);
        PRIMITIVE_NAMES.put(float.class.getName(), float.class);
        PRIMITIVE_NAMES.put(double.class.getName(), double.class);
    }

    private static Class<?> getPrimitiveClass(String className) {
        return PRIMITIVE_NAMES.get(className);
    }

    private static void checkPrimitive(Class<?> componentType) {
        if (componentType == null) {
            throw new IllegalArgumentException("null component type not allowed");
        }
        if (!PRIMITIVE_NAMES.containsKey(componentType.getName())) {
            throw new IllegalArgumentException("input array component type "
                    + componentType.getName() + " is not a candidate primitive type");
        }
    }

    private void checkDeclaredComponentType(Class<?> componentType) {
        if ((centerDeclaredComponentType != null)
                && (componentType != centerDeclaredComponentType)) {
            throw new IllegalArgumentException("input array component type "
                    + componentType.getName() + " does not match declared type "
                    + centerDeclaredComponentType.getName());
        }
    }

    private static void checkArray(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("null value not allowed");
        }
        if (!value.getClass().isArray()) {
            throw new IllegalArgumentException("non-array value of class "
                    + value.getClass() + " not allowed");
        }
    }

    public CanopyWritable() {
    }

    @Override
    public String toString() {
        Cluster cluster = this.get();
        return cluster.asFormattedString();
    }

    public CanopyWritable(Cluster cluster) {
        set(cluster);
    }


    public Cluster get() {
        return new Canopy(id, center, observations);
    }

    public Class<?> getCenterComponentType() {
        return centerComponentType;
    }

    public Class<?> getCenterDeclaredComponentType() {
        return centerDeclaredComponentType;
    }

    public boolean isDeclaredComponentType(Class<?> componentType) {
        return componentType == centerDeclaredComponentType;
    }

    public void set(Cluster cluster) {
        Object value = cluster.getCenter();

        // Set center as Object
        checkArray(value);
        Class<?> componentType = value.getClass().getComponentType();
        checkPrimitive(componentType);
        checkDeclaredComponentType(componentType);
        this.centerComponentType = componentType;
        this.centerObject = value;
        this.centerLength = Array.getLength(value);

        // Set cluster
        this.id = cluster.getId();
        this.observations = cluster.getNum();
        this.center = (int[]) centerObject;
    }




    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(id);
        out.writeLong(observations);
        out.writeUTF(centerComponentType.getName());
        out.writeInt(centerLength);

        if (centerComponentType == Boolean.TYPE) {          // boolean
            writeBooleanArray(out);
        } else if (centerComponentType == Character.TYPE) { // char
            writeCharArray(out);
        } else if (centerComponentType == Byte.TYPE) {      // byte
            writeByteArray(out);
        } else if (centerComponentType == Short.TYPE) {     // short
            writeShortArray(out);
        } else if (centerComponentType == Integer.TYPE) {   // int
            writeIntArray(out);
        } else if (centerComponentType == Long.TYPE) {      // long
            writeLongArray(out);
        } else if (centerComponentType == Float.TYPE) {     // float
            writeFloatArray(out);
        } else if (centerComponentType == Double.TYPE) {    // double
            writeDoubleArray(out);
        } else {
            throw new IOException("Component type " + centerComponentType.toString()
                    + " is set as the output type, but no encoding is implemented for this type.");
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.id = in.readInt();
        this.observations = in.readLong();

        String className = in.readUTF();
        Class<?> componentType = getPrimitiveClass(className);
        if (componentType == null) {
            throw new IOException("encoded array component type "
                    + className + " is not a candidate primitive type");
        }
        checkDeclaredComponentType(componentType);
        this.centerComponentType = componentType;

        int length = in.readInt();
        if (length < 0) {
            throw new IOException("encoded array length is negative " + length);
        }
        this.centerLength = length;

        centerObject = Array.newInstance(componentType, length);

        if (componentType == Boolean.TYPE) {             // boolean
            readBooleanArray(in);
        } else if (componentType == Character.TYPE) {    // char
            readCharArray(in);
        } else if (componentType == Byte.TYPE) {         // byte
            readByteArray(in);
        } else if (componentType == Short.TYPE) {        // short
            readShortArray(in);
        } else if (componentType == Integer.TYPE) {      // int
            readIntArray(in);
        } else if (componentType == Long.TYPE) {         // long
            readLongArray(in);
        } else if (componentType == Float.TYPE) {        // float
            readFloatArray(in);
        } else if (componentType == Double.TYPE) {       // double
            readDoubleArray(in);
        } else {
            throw new IOException("Encoded type " + className
                    + " converted to valid component type " + componentType.toString()
                    + " but no encoding is implemented for this type.");
        }

        this.center = (int[]) centerObject;
    }

    private void writeBooleanArray(DataOutput out) throws IOException {
        boolean[] v = (boolean[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            out.writeBoolean(v[i]);
        }
    }

    private void writeCharArray(DataOutput out) throws IOException {
        char[] v = (char[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            out.writeChar(v[i]);
        }
    }

    private void writeByteArray(DataOutput out) throws IOException {
        out.write((byte[]) centerObject, 0, centerLength);
    }

    private void writeShortArray(DataOutput out) throws IOException {
        short[] v = (short[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            out.writeShort(v[i]);
        }
    }

    private void writeIntArray(DataOutput out) throws IOException {
        int[] v = (int[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            out.writeInt(v[i]);
        }
    }

    private void writeLongArray(DataOutput out) throws IOException {
        long[] v = (long[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            out.writeLong(v[i]);
        }
    }

    private void writeFloatArray(DataOutput out) throws IOException {
        float[] v = (float[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            out.writeFloat(v[i]);
        }
    }

    private void writeDoubleArray(DataOutput out) throws IOException {
        double[] v = (double[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            out.writeDouble(v[i]);
        }
    }

    private void readBooleanArray(DataInput in) throws IOException {
        boolean[] v = (boolean[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            v[i] = in.readBoolean();
        }
    }

    private void readCharArray(DataInput in) throws IOException {
        char[] v = (char[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            v[i] = in.readChar();
        }
    }

    private void readByteArray(DataInput in) throws IOException {
        in.readFully((byte[]) centerObject, 0, centerLength);
    }

    private void readShortArray(DataInput in) throws IOException {
        short[] v = (short[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            v[i] = in.readShort();
        }
    }

    private void readIntArray(DataInput in) throws IOException {
        int[] v = (int[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            v[i] = in.readInt();
        }
    }

    private void readLongArray(DataInput in) throws IOException {
        long[] v = (long[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            v[i] = in.readLong();
        }
    }

    private void readFloatArray(DataInput in) throws IOException {
        float[] v = (float[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            v[i] = in.readFloat();
        }
    }

    private void readDoubleArray(DataInput in) throws IOException {
        double[] v = (double[]) centerObject;
        for (int i = 0; i < centerLength; i++) {
            v[i] = in.readDouble();
        }
    }
}

