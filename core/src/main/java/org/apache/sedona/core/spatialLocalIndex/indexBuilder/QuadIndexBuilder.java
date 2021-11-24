package org.apache.sedona.core.spatialLocalIndex.indexBuilder;

import org.locationtech.jts.geom.Envelope;
import org.apache.sedona.core.spatialLocalIndex.IndexBuilder;
import org.apache.sedona.core.spatialLocalIndex.SpatialLocalIndex;
import org.apache.sedona.core.spatialLocalIndex.quadTree.QuadLocalIndex;

import java.util.List;

public class QuadIndexBuilder extends IndexBuilder {

    int capacity;

    public QuadIndexBuilder(int capacity, List<Envelope> partitionGrids) {
        setPartitionGrids(partitionGrids);
        setCapacity(capacity);
    }


    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public SpatialLocalIndex createIndex() {
        return new QuadLocalIndex(capacity);
    }

}
