
package org.apache.sedona.core.spatialLocalIndex;

import org.locationtech.jts.geom.*;

import java.io.Serializable;
import java.util.Set;


public interface SpatialLocalIndex extends Serializable {

    // State of grid cell after intersecting with query polygon
    enum PolygonIntersectState { WITHIN, INTERSECT, OUTSIDE, INVALID}

    // Method type for range query
    enum QueryMethod { FILTER_REFINE, DECOMPOSITION, MODIFIED_FILTER_REFINE, PREPROCESSED_DECOMPOSITION, INDEX_QUERY, ALL; }

    // Method type for range query
    enum EstimateLevel { LAYER_LEVEL, POLYGON_LEVEL; }

    // Class for determining the state of the grid and getting the clipped polygon
    class Decomposition {
        private PolygonIntersectState state;
        private Geometry shape;

        public Decomposition(Envelope grid, Geometry query) {
            setState(grid, query);
        }

        private Geometry createEnvelopeGeometry(Envelope envelope) {
            Coordinate[] coordinates = new Coordinate[5];
            coordinates[0] = new Coordinate(envelope.getMinX(), envelope.getMinY());
            coordinates[1] = new Coordinate(envelope.getMinX(), envelope.getMaxY());
            coordinates[2] = new Coordinate(envelope.getMaxX(), envelope.getMaxY());
            coordinates[3] = new Coordinate(envelope.getMaxX(), envelope.getMinY());
            coordinates[4] = coordinates[0];

            GeometryFactory geometryFactory = new GeometryFactory();
            return geometryFactory.createPolygon(coordinates);
        }


        private void setState(Envelope grid, Geometry query) {

            try {

                Geometry gridGeometry = createEnvelopeGeometry(grid);

                if(!grid.intersects(query.getEnvelopeInternal()) && !query.intersects(gridGeometry.getCentroid())) {
                    shape = null;
                    state = PolygonIntersectState.OUTSIDE;
                } else {
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
                        if (query.intersects(gridGeometry.getCentroid()) ) { // grid is a line or point
                            state = PolygonIntersectState.WITHIN;
                            shape = gridGeometry;
                        } else {
                            state = PolygonIntersectState.OUTSIDE;
                            shape = null;
                        }
                    }
                }
            } catch (Exception e) {

                e.printStackTrace();
                System.out.println(query);
                System.out.println(grid);
                System.out.println(e.getMessage());
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

    /*
    Returns a set of objects that lie within the search envelope
     */
    Set query(Envelope searchEnvelope);


    /*
    NOT IMPLEMENTED
    Returns a set of objects that lie within the search polygon using the specified query method
     */
    Set query(Geometry searchPolygon, QueryMethod method);

    /*
    Returns the count of data points that lie within the search polygon using the specified query method
     */
    Set count(Geometry searchPolygon, QueryMethod method);

    /*
    NOT IMPLEMENTED
    Returns a set of key-objects that lie within the search polygons using the specified query method.
    The key is the search polygon name and the objects within this polygon
     */
    Set query(GeometryCollection polygonLayer, QueryMethod method);

    /*
    Returns the count of data points that lie within the search polygon using the specified query method
    (KEY-COUNT)
    */
    Set count(GeometryCollection polygonLayer, QueryMethod method);
    /*
    Returns the count of data points that lie within the search polygon using estimation model
    (KEY-COUNT)
    */
    Set count(GeometryCollection polygonLayer, EstimateLevel level);

    /*
    Returns an estimate of operations for the polygon layer using the specified query method
     */
    Set estimate(GeometryCollection polygonLayer, QueryMethod method);

    /*
    Returns the actual number of operations for the polygon layer using the specified query method
     */
    Set actualOperations(GeometryCollection polygonLayer, QueryMethod method);

    /*
    Total number of data points in index
     */
    Set count();
}
