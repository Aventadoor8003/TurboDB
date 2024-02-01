package cis5550.model;

import cis5550.kvs.Row;
import cis5550.tools.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemoryTable implements Table {
    private static final Logger logger = Logger.getLogger(MemoryTable.class);
    private final ConcurrentHashMap<String, NavigableMap<Integer, Row>> rows;
    private  String name;

    public MemoryTable(String name) {
        this.name = name;
        this.rows = new ConcurrentHashMap<>();
    }

    public String getName() {
        return name;
    }

    @Override
    public synchronized void setName(String name) {
        this.name = name;
    }

    /**
     * Get the row with the given key and version
     * @param key the key of the row
     * @param version the version of the row
     * @return the row with the given key and version
     */
    public Row get(String key, Integer version) {
        NavigableMap<Integer, Row> versions = rows.get(key);
        if (versions == null) return null;
        return version == null ? versions.lastEntry().getValue() : versions.get(version);
    }

    public void put(String key, Row row) {
        NavigableMap<Integer, Row> versions = rows.computeIfAbsent(key, k -> new ConcurrentSkipListMap<>());
        int nextVersion = versions.isEmpty() ? 1 : versions.lastKey() + 1;
        versions.put(nextVersion, row);
        logger.debug("Added row with key " + key + " and version " + nextVersion + " row content " + row);
    }

    /**
     * Get the newest version of the row with the given key
     * @param key the key of the row
     * @return the newest version of the row
     */
    public Row get(String key) {
        return get(key, null);
    }

    public int newestVersion(String key) {
        NavigableMap<Integer, Row> versions = rows.get(key);
        if (versions == null) return -1;
        return versions.lastKey();
    }

    public Set<String> getKeys() {
        return rows.keySet();
    }

    @Override
    public int countKeys() {
        return rows.size();
    }

    @Override
    public String generateMD5(String rowKey) throws NoSuchAlgorithmException {
        Row row = get(rowKey);
        if (row == null) {
            return null;
        }
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] hashInBytes = md.digest(row.toByteArray());
        StringBuilder sb = new StringBuilder();
        for (byte b : hashInBytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Override
    public List<RowMeta> getRowMetas() {
        List<RowMeta> rowMetas = new ArrayList<>();
        for(String key : rows.keySet()) {
            try {
                rowMetas.add(new RowMeta(key, generateMD5(key)));
            } catch (NoSuchAlgorithmException e) {
                logger.error("Error generating MD5 for row " + key, e);
                throw new RuntimeException(e);
            }
        }
        return rowMetas;
    }
}
