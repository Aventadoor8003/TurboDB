package cis5550.model;

import cis5550.kvs.Row;
import cis5550.tools.KeyEncoder;
import cis5550.tools.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Just store metadata in memory.
 * When data io is needed, read or write to disk
 */
public class PersistentTable implements Table {
    private String name;
    private final String storageDir;
    private static final Logger logger = Logger.getLogger(PersistentTable.class);
    private final ConcurrentMap<String, Object> locks = new ConcurrentHashMap<>();

    public PersistentTable(String name, String storageDir) {
        this.name = name;
        this.storageDir = storageDir;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public synchronized void setName(String name) {
        this.name = name;
    }

    @Override
    public Row get(String rowKey) {
        return readRowFromDisk(name, rowKey);
    }

    @Override
    public void put(String rowKey, Row row) {
        writeRowToDisk(name, rowKey, row);
    }

    @Override
    public Set<String> getKeys() {
        File tableDir = new File(storageDir, name);

        if (!tableDir.exists() || !tableDir.isDirectory()) {
            return new HashSet<>();
        }

        return Arrays.stream(Objects.requireNonNull(tableDir.listFiles()))
                .filter(File::isFile)
                .map(File::getName)
                .map(KeyEncoder::decode)
                .collect(Collectors.toSet());
    }

    @Override
    public int countKeys() {
        File tableDir = new File(storageDir, name);

        if (!tableDir.exists() || !tableDir.isDirectory()) {
            return 0;
        }

        return (int) Arrays.stream(Objects.requireNonNull(tableDir.listFiles()))
                .filter(File::isFile)
                .count();
    }

    @Override
    public String generateMD5(String rowKey) throws NoSuchAlgorithmException {
        File tableDir = new File(storageDir, name);
        File rowFile = new File(tableDir, KeyEncoder.encode(rowKey));
        if (!rowFile.exists()) {
            return null;
        }
        Row row = readRowFromDisk(name, rowKey);
        if(row == null) {
            throw new RuntimeException("Row " + rowKey + " does not exist");
        }
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] hashInBytes = md.digest(row.toByteArray());
        StringBuilder sb = new StringBuilder();
        for (byte b : hashInBytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public synchronized void destroy() throws IOException {
        Path tableDir = Paths.get(storageDir, name);
//        logger.debug("Deleting all files in " + tableDir.toAbsolutePath());

        // Delete all files in the table directory
        Files.list(tableDir).forEach(file -> {
            try {
//                logger.debug("Deleting file " + file.toAbsolutePath());
                Files.delete(file);
            } catch (IOException e) {
                logger.error("Error while deleting file " + file.toAbsolutePath(), e);
                throw new RuntimeException(e);
            }
        });
        Files.delete(tableDir);
    }

    private void writeRowToDisk(String table, String rowKey, Row rowObj) {
        Object lock = locks.computeIfAbsent(rowKey, k -> new Object());
        synchronized (lock) {

            File tableDir = new File(storageDir, table);
            if (!tableDir.exists()) {
                tableDir.mkdir();
            }
            File rowFile = new File(tableDir, KeyEncoder.encode(rowKey));
            logger.debug("Row " + rowKey + " will be written as " + KeyEncoder.encode(rowKey));
            try (FileOutputStream fos = new FileOutputStream(rowFile)) {
                fos.write(rowObj.toByteArray());
            } catch (IOException e) {
                logger.error("Error while writing row to disk", e);
            }
        }
        locks.remove(rowKey);
    }

    private Row readRowFromDisk(String tableName, String rowKey) {
        File tableDir = new File(storageDir, tableName);
        File rowFile = new File(tableDir, KeyEncoder.encode(rowKey));
        if (!rowFile.exists()) {
            return null;
        }
        try (RandomAccessFile raf = new RandomAccessFile(rowFile, "r")) {
            return Row.readFrom(raf);
        } catch (Exception e) {
            logger.error("Error while reading row from disk", e);
            return null;
        }
    }

    private static boolean fileExistsInDirectory(String directoryPath, String fileName) {
        File directory = new File(directoryPath);
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException(directoryPath + " is not a directory");
        }
        File fileToCheck = new File(directory, fileName);
        return fileToCheck.exists();
    }

    @Override
    public List<RowMeta> getRowMetas() {
        List<RowMeta> rowMetas = new ArrayList<>();
        for(String key : getKeys()) {
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
