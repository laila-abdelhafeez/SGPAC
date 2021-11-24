package org.apache.sedona.core.spatialLocalIndex.gridIndex;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GridCell implements Serializable {

    private List<Object> items;
    private Envelope boundary;

    public GridCell() {
        items = new ArrayList<>();
    }

    public void setBoundary(Envelope boundary) {
        this.boundary = boundary;
    }

    public boolean isInCell(Geometry itemGeometry) {
        if(itemGeometry instanceof Point) {
            return boundary.contains(itemGeometry.getCoordinate());
        }
        return boundary.intersects(itemGeometry.getEnvelopeInternal());
    }

    public void insertItem(Object item) {
        items.add(item);
    }

    public List getItems() { return items; }

    public Integer getCount() {return items.size(); }

    public List getItemsInBoundary(Envelope searchBoundary) {

        List result = new ArrayList();

        for(Object item : items) {
            if(item instanceof Point && searchBoundary.contains(((Point) item).getCoordinate())) {
                result.add(item);
            }
            else if(searchBoundary.intersects(((Geometry)item).getEnvelopeInternal())) {
                result.add(item);
            }
        }
        return result;
    }

    public Envelope getBoundary() {
        return boundary;
    }
}
