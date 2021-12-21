package org.apache.sedona.core.spatialLocalIndex.RTree;

import org.apache.sedona.core.spatialLocalIndex.SpatialLocalIndex;
import org.apache.spark.SparkEnv;
import org.locationtech.jts.geom.*;
import scala.Tuple2;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class RTeeIndex implements SpatialLocalIndex {

    private int capacity;
    private MSTRtree rtree;
    private Envelope boundary;

    public RTeeIndex(int capacity) {
        this.capacity = capacity;
        this.rtree = new MSTRtree(capacity);
    }

    @Override
    public void setBoundary(Envelope boundary) {
        this.boundary = boundary;
    }

    @Override
    public void insertItem(Geometry item) {
        rtree.insert(item.getEnvelopeInternal(), item);
    }

    @Override
    public Set query(Envelope searchEnvelope) {
        return new HashSet(rtree.query(searchEnvelope));
    }

    @Override
    public Set query(Geometry searchPolygon, QueryMethod method) {
        return null;
    }

    @Override
    public Set count(Geometry searchPolygon, QueryMethod method) {
        return null;
    }

    @Override
    public Set query(GeometryCollection polygonLayer, QueryMethod method) {
        return null;
    }

    @Override
    public Set count(GeometryCollection polygonLayer, QueryMethod method) {
        String executorId = SparkEnv.get().executorId();
        long start_time = System.nanoTime();
        double duration;
        String log;

        Set<Tuple2<String, Integer>> results = new HashSet<Tuple2<String, Integer>>();
        int numberOfGeometries = polygonLayer.getNumGeometries();

        switch (method) {
            case FILTER_REFINE:
                for(int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    String geometryName = geometry.getUserData().toString();
                    int size = filterRefineCount(geometry);
                    results.add(new Tuple2(geometryName, size));
                }
                break;

            case INDEX_QUERY:
                for(int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    String geometryName = geometry.getUserData().toString();
                    int size = indexQueryCount(geometry);
                    results.add(new Tuple2(geometryName, size));
                }
                break;

            case DECOMPOSITION:
                for(int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    String geometryName = geometry.getUserData().toString();
                    int size = decompositionCount(geometry);
                    results.add(new Tuple2(geometryName, size));
                }
                break;

            case ALL:
                for(int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    String geometryName = geometry.getUserData().toString();
                    int fil = filterRefineCount(geometry);
                    int ind = indexQueryCount(geometry);
                    int dec = decompositionCount(geometry);
                    System.out.println("Filter-refine: " + fil + "\tOracle: " + ind + "\tDecomposition: " + dec);
                    results.add(new Tuple2(geometryName, fil));
                }
                break;
        }

        duration = (System.nanoTime() - start_time)/1e+9;
        duration /= 60;

//        log = "POLYGON-LAYER\t" + method.name() + executorId + "\t" + results.size() + "\t" + duration";

//        System.out.println(log);
        return results;
    }

    @Override
    public Set count(GeometryCollection polygonLayer, EstimateLevel level) {
        return null;
    }

    @Override
    public Set estimate(GeometryCollection polygonLayer, QueryMethod method) {
        return null;
    }

    @Override
    public Set actualOperations(GeometryCollection polygonLayer, QueryMethod method) {
        return null;
    }

    @Override
    public Set count() {
        return null;
    }

    private Integer filterRefineCount(Geometry searchPolygon) {
        Set result = new HashSet();
        List<Geometry> candidateSet = rtree.query(searchPolygon.getEnvelopeInternal());
        for (Geometry tempResult : candidateSet) {
            if(searchPolygon.contains(tempResult) || searchPolygon.intersects(tempResult)) {
                result.add(tempResult);
            }
        }
        return result.size();
    }

    private Integer indexQueryCount(Geometry searchPolygon) {

        MSTRtree polygonIndex = new MSTRtree(2);

        GeometryFactory factory = new GeometryFactory();
        Coordinate[] coordinates = searchPolygon.getCoordinates();

        for(int i = 0; i+1 < coordinates.length; ++i) {
            Coordinate[] LineCoordinates = new Coordinate[2];
            LineCoordinates[0] = coordinates[i];
            LineCoordinates[1] = coordinates[i+1];
            LineString line = factory.createLineString(LineCoordinates);
            polygonIndex.insert(line.getEnvelopeInternal(), line);
        }

        return this.rtree.count(polygonIndex, searchPolygon);

    }

    private Integer decompositionCount(Geometry searchPolygon) {

        Decomposition partitionIntersection = new Decomposition(boundary, searchPolygon);

        switch (partitionIntersection.getState()) {
            case WITHIN:
                return rtree.getAllDataCount();
            case INTERSECT:
                return rtree.count(partitionIntersection.getShape());
            default:
                return 0;
        }

    }
}
