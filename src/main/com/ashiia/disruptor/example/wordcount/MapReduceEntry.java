package com.ashiia.disruptor.example.wordcount;

import com.lmax.disruptor.AbstractEntry;
import com.lmax.disruptor.EntryFactory;

import java.util.HashMap;

public class MapReduceEntry extends AbstractEntry {
    private String mapValue;
    private HashMap<String, Integer> reduceValue;
    private HashMap<String, Integer> finalValue;

    public MapReduceEntry() {
    }

    public final static EntryFactory<MapReduceEntry> ENTRY_FACTORY = new EntryFactory<MapReduceEntry>() {
        public MapReduceEntry create() {
            return new MapReduceEntry();
        }
    };

    public String getMapValue() {
        return mapValue;
    }

    public void setMapValue(String mapValue) {
        this.mapValue = mapValue;
    }

    public HashMap<String, Integer> getReduceValue() {
        return reduceValue;
    }

    public void setReduceValue(HashMap<String, Integer> reduceValue) {
        this.reduceValue = reduceValue;
    }

    public HashMap<String, Integer> getFinalValue() {
        return finalValue;
    }

    public void setFinalValue(HashMap<String, Integer> finalValue) {
        this.finalValue = finalValue;
    }
}
