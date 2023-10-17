package com.microsoft.azure.kusto.ingest.utils;

import java.util.Collections;
import java.util.List;
import java.util.Random;

public class DefaultRandomProvider implements RandomProvider {
    public Random random;

    public DefaultRandomProvider(Random random) {
        this.random = random;
    }

    public DefaultRandomProvider() {
        this.random = new Random();
    }

    @Override
    public void shuffle(List<?> list) {
        Collections.shuffle(list, random);
    }
}
