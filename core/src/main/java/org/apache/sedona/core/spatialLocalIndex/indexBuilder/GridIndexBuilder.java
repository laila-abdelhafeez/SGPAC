package org.apache.sedona.core.spatialLocalIndex.indexBuilder;

import org.locationtech.jts.geom.Envelope;
import org.apache.sedona.core.spatialLocalIndex.IndexBuilder;
import org.apache.sedona.core.spatialLocalIndex.SpatialLocalIndex;
import org.apache.sedona.core.spatialLocalIndex.gridIndex.GridLocalIndex;

import java.util.List;

public class GridIndexBuilder extends IndexBuilder {

    int numXCells, numYCells;

    public GridIndexBuilder(int numXCells, int numYCells, List<Envelope> partitionGrids) {
        setPartitionGrids(partitionGrids);
        setNumCells(numXCells, numYCells);
    }


    public void setNumCells(int numXCells, int numYCells) {
        this.numXCells = numXCells;
        this.numYCells = numYCells;
    }

    @Override
    public SpatialLocalIndex createIndex() {
        return new GridLocalIndex(numXCells, numYCells);
    }

}
