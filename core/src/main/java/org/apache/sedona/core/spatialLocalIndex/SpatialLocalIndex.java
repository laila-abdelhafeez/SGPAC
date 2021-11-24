
package org.apache.sedona.core.spatialLocalIndex;

import org.locationtech.jts.geom.*;

import java.io.Serializable;
import java.util.Set;


public interface SpatialLocalIndex extends Serializable {

    // State of grid cell after intersecting with query polygon
    enum PolygonIntersectState { WITHIN, INTERSECT, OUTSIDE, INVALID}

    // Method type for range query
    enum QueryMethod { FILTER_REFINE, DECOMPOSITION, MODIFIED_FILTER_REFINE, PREPROCESSED_DECOMPOSITION; }

    // Method type for range query
    enum EstimateLevel { LAYER_LEVEL, POLYGON_LEVEL; }

    // Class for determining the state of the grid and getting the clipped polygon
    class Decomposition {
        private PolygonIntersectState state;
        private Geometry shape;

        public Decomposition(Envelope grid, Geometry query) {
            setState(grid, query);
        }


        private void setState(Envelope grid, Geometry query) {

            try {
                if (!grid.intersects(query.getEnvelopeInternal())) {
                    shape = null;
                    state = PolygonIntersectState.OUTSIDE;
                } else {
                    Coordinate[] coordinates = new Coordinate[5];
                    coordinates[0] = new Coordinate(grid.getMinX(), grid.getMinY());
                    coordinates[1] = new Coordinate(grid.getMinX(), grid.getMaxY());
                    coordinates[2] = new Coordinate(grid.getMaxX(), grid.getMaxY());
                    coordinates[3] = new Coordinate(grid.getMaxX(), grid.getMinY());
                    coordinates[4] = coordinates[0];

                    GeometryFactory geometryFactory = new GeometryFactory();
                    Geometry gridGeometry = geometryFactory.createPolygon(coordinates);

                    Geometry decomposedPolygon = query.intersection(gridGeometry);

                    if (!decomposedPolygon.isEmpty()) {
                        if(decomposedPolygon.equalsExact(gridGeometry)) {
                            state = PolygonIntersectState.WITHIN;
                            shape = gridGeometry;
                        }
                        else {
                            state = PolygonIntersectState.INTERSECT;
                            shape = decomposedPolygon;
                        }

                    } else {
                        state = PolygonIntersectState.OUTSIDE;
                        shape = null;
                    }
                }
            } catch (Exception e) {

                e.printStackTrace();
                state = PolygonIntersectState.INVALID;
                shape = null;
            }

        }

        public PolygonIntersectState getState() {
            return state;
        }

        public Geometry getShape() {
            return shape;
        }
    }

    void setBoundary(Envelope boundary);
    void insertItem(Geometry item);


    Set query(Envelope searchEnvelope);

    Set query(Geometry searchPolygon, QueryMethod method);
    Set count(Geometry searchPolygon, QueryMethod method);

    Set query(GeometryCollection polygonLayer, QueryMethod method);
    Set count(GeometryCollection polygonLayer, QueryMethod method);
    Set count(GeometryCollection polygonLayer, EstimateLevel level);

    Set estimate(GeometryCollection polygonLayer, QueryMethod method);
    Set actualOperations(GeometryCollection polygonLayer, QueryMethod method);

    Set count();
}
