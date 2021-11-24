package org.apache.sedona.core.spatialOperator.rangeQuery;

import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;

public class RangeQuery<U extends Geometry> implements Serializable {
    private boolean considerBoundaryIntersection;
    U queryGeometry;

    public RangeQuery(boolean considerBoundaryIntersection, U queryGeometry) {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
        this.queryGeometry = queryGeometry;
    }

    public boolean match(Geometry spatialObject, Geometry queryWindow) {
        if (considerBoundaryIntersection) {
            return queryWindow.intersects(spatialObject);
        }
        else {
            return queryWindow.covers(spatialObject);
        }
    }
}
