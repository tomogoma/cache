package me.tomogoma.androidcache.leveldb;

import android.content.Context;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import me.tomogoma.androidcache.Cache;
import me.tomogoma.androidcache.Storable;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

/**
 * Created by tomogoma on 29/08/16.
 */
public class LevelDBCache implements Cache {

    static {
        System.setProperty("sun.arch.data.model", "32");
        System.setProperty("leveldb.mmap", "false");
    }

    private static final String CHARSET = "UTF-8";
    private static final Object mDbLock = new Object();

    private File mDbFile;

    private static class KeyValuePair {
        private byte[] key;
        private byte[] value;

        private KeyValuePair(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    public LevelDBCache(Context context, String dbName) {

        if (dbName == null || dbName.isEmpty()) {
            throw new IllegalArgumentException("Db file name was empty");
        }

        if (context == null) {
            throw new IllegalArgumentException("Null context");
        }

        String fileName = context.getFilesDir() + File.separator + dbName;
        mDbFile = new File(fileName);
    }

    @Override
    public void insert(Storable storable) throws IOException, InvalidKeyException {

        synchronized (mDbLock) {

            DB db = openDb();
            try {
                KeyValuePair kv = extractKeyValuePair(storable);
                db.put(kv.key, kv.value);
            } finally {
                if (db != null) {
                    db.close();
                }
            }
        }
    }

    @Override
    public void insert(Storable... storables) throws IOException, InvalidKeyException {

        synchronized (mDbLock) {

            DB db = openDb();
            WriteBatch batch = null;
            try {
                batch = db.createWriteBatch();
                for (Storable storable : storables) {
                    KeyValuePair kv = extractKeyValuePair(storable);
                    batch.put(kv.key, kv.value);
                }
                db.write(batch);
            } finally {

                if (db != null) {
                    db.close();
                }

                if (batch != null) {
                    batch.close();
                }
            }
        }
    }

    @Override
    public void delete(String key) throws IOException, InvalidKeyException {

        synchronized (mDbLock) {

            DB db = openDb();
            try {
                byte[] keyB = extractKey(key);
                db.delete(keyB);
            } finally {
                if (db != null) {
                    db.close();
                }
            }
        }
    }

    @Override
    public void deleteBatch(Set<String> keys) throws IOException, InvalidKeyException {

        synchronized (mDbLock) {

            DB db = openDb();
            WriteBatch batch = null;
            try {
                batch = db.createWriteBatch();
                for (String key : keys) {
                    byte[] keyB = extractKey(key);
                    batch.delete(keyB);
                }
                db.write(batch);
            } finally {

                if (db != null) {
                    db.close();
                }

                if (batch != null) {
                    batch.close();
                }
            }
        }
    }

    @Override
    public String get(String key) throws IOException, InvalidKeyException {

        synchronized (mDbLock) {

            DB db = openDb();
            try {
                byte[] keyB = extractKey(key);
                byte[] val = db.get(keyB);
                return toString(val);
            } finally {
                if (db != null) {
                    db.close();
                }
            }
        }
    }

    @Override
    public Map<String, String> getFirstCount(int count) throws IOException, InvalidKeyException {

        HashMap<String, String> storables = new HashMap<>();
        synchronized (mDbLock) {

            DB db = openDb();
            DBIterator iterator = null;
            try {
                iterator = db.iterator();
                int i = 0;
                for (iterator.seekToFirst(); iterator.hasNext() && i < count; iterator.next(), i++) {
                    byte[] keyB = iterator.peekNext().getKey();
                    byte[] valueB = iterator.peekNext().getValue();
                    String key = toString(keyB);
                    String value = toString(valueB);
                    storables.put(key, value);
                }
            } finally {
                if (db != null) {
                    db.close();
                }
                if (iterator != null) {
                    iterator.close();
                }
            }
        }

        return storables;
    }

    private KeyValuePair extractKeyValuePair(Storable storable) throws InvalidKeyException {

        byte[] keyB = extractKey(storable.getKey());
        byte[] valueB = new byte[0];
        String value = storable.getValue();
        if (value != null) {
            valueB = value.getBytes(Charset.forName(CHARSET));
        }

        return new KeyValuePair(keyB, valueB);
    }

    private byte[] extractKey(String key) throws InvalidKeyException {
        if (key == null || key.isEmpty()) {
            throw new InvalidKeyException("Key was empty");
        }
        return key.getBytes(Charset.forName(CHARSET));
    }

    private String toString(byte[] value) {
        if (value == null) {
            return null;
        }
        try {
            return new String(value, CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private DB openDb() throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        return factory.open(mDbFile, options);
    }
}
