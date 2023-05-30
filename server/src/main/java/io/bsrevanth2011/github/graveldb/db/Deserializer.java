package io.bsrevanth2011.github.graveldb.db;

public interface Deserializer<T> {
    T deserialize(byte[] bytes);
}
