package io.bsrevanth2011.github.graveldb.db;

public interface Serializer<T> {
    byte[] serialize(T o);
}
