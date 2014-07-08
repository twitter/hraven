package com.twitter.hraven.util;

import java.io.*;
import org.apache.hadoop.io.*;

public class EnumWritable<E extends Enum<E>> implements WritableComparable<E> {
    private Class<E> cls;
    private E value;

    @SuppressWarnings("unchecked")
    public EnumWritable(E value) {
        this.cls = (Class<E>) value.getClass();
        this.value = value;
    }

    public E getValue() {
        return value;
    }

    public void readFields(DataInput input) throws IOException {
        value = WritableUtils.readEnum(input, cls);
    }

    public void write(DataOutput output) throws IOException {
        WritableUtils.writeEnum(output, value);
    }

    @Override
    public int compareTo(E o) {
      return this.value.compareTo(o);
    }
}