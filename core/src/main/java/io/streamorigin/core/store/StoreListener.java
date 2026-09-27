package io.streamorigin.core.store;

/** Counts storage operations so experiments can report store reads per publish. */
public interface StoreListener {

    StoreListener NONE = new StoreListener() {
    };

    default void metaRead(int keys) {
    }

    default void dataRead(int chunks, int bytes) {
    }

    default void write(int chunks, int bytes) {
    }
}
