package me.tomogoma.androidcache;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by tomogoma on 29/08/16.
 */
public interface Cache {

    void insert(Storable storable) throws IOException, InvalidKeyException;

    void insert(Storable... storables) throws IOException, InvalidKeyException;

    void delete(String key) throws IOException, InvalidKeyException;

    void deleteBatch(Set<String> keys) throws IOException, InvalidKeyException;

    String get(String key) throws IOException, InvalidKeyException;

    Map<String, String> getFirstCount(int count) throws IOException, InvalidKeyException;


    class InvalidKeyException extends Exception {

        public InvalidKeyException(String message) {
            super(message);
        }
    }
}
