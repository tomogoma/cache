package me.tomogoma.androidcache;

/**
 * Created by tomogoma on 29/08/16.
 */
public interface Storable {
    String getKey();
    String getValue();
    String toJson();
}
