package cis5550.model;

import cis5550.kvs.Row;

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;

public interface Table {
    String getName();

    void setName(String name);

    Row get(String key);

    void put(String key, Row row);

    Set<String> getKeys();

    int countKeys();

    String generateMD5(String rowKey) throws NoSuchAlgorithmException;
    List<RowMeta> getRowMetas();
}
