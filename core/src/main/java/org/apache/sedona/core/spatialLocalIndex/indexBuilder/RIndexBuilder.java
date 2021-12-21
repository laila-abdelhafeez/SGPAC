package org.apache.sedona.core.spatialLocalIndex.indexBuilder;

import org.apache.sedona.core.spatialLocalIndex.IndexBuilder;
import org.apache.sedona.core.spatialLocalIndex.RTree.RTeeIndex;
import org.apache.sedona.core.spatialLocalIndex.SpatialLocalIndex;
import org.locationtech.jts.geom.Envelope;

import java.util.List;

public class RIndexBuilder extends IndexBuilder {

    int capacity;

    public RIndexBuilder(int capacity, List<Envelope> partitionGrids) {
        setPartitionGrids(partitionGrids);
        setCapacity(capacity);
    }


    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public SpatialLocalIndex createIndex() {
        return new RTeeIndex(capacity);
    }
}
