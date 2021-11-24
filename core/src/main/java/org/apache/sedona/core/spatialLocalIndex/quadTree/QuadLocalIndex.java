package org.apache.sedona.core.spatialLocalIndex.quadTree;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.apache.spark.SparkEnv;
import org.apache.sedona.core.spatialLocalIndex.SpatialLocalIndex;
import scala.Tuple2;

import java.util.HashSet;
import java.util.Set;

public class QuadLocalIndex implements SpatialLocalIndex {


    private int capacity;
    private QuadNode root;

    private int totalNumberOfData;
    private int intersection;
    private int sum;

    public QuadLocalIndex(int capacity) {
        this.capacity = capacity;
        this.root = null;
        this.totalNumberOfData = 0;
    }

    @Override
    public void setBoundary(Envelope boundary) {
        this.root = new QuadNode(boundary, 0, capacity);
    }

    @Override
    public void insertItem(Geometry item) {
        totalNumberOfData += root.insertItem(item);
    }
    @Override
    public Set query(Geometry searchPolygon, QueryMethod method) {

        Set<Geometry> result = null;
        switch (method) {

            case FILTER_REFINE:
                result = filterRefine(searchPolygon);
                break;

            case DECOMPOSITION:
                result = decomposition(searchPolygon);
                break;

            case PREPROCESSED_DECOMPOSITION:
                break;

        }

        return result;
    }

    private Set filterRefine(Geometry searchPolygon) {

        Set result = new HashSet<>();

        Set<Geometry> candidateSet = query(searchPolygon.getEnvelopeInternal());

        for (Geometry tempResult : candidateSet) {
            if(searchPolygon.contains(tempResult)) {
                result.add(tempResult);
            }
        }

        return result;
    }


    private Set getAllData() {
        return root.getAllItems();
    }


    private Set startDecomposition(QuadNode currentRoot, Geometry searchPolygon) {

        Set<Geometry> result = new HashSet<>();
        QuadNode[] nodes = currentRoot.getChildren();

        if(nodes == null) return result;

        for(QuadNode node : nodes) {
            Decomposition decompositionState = new Decomposition(node.getBoundary(), searchPolygon);
            intersection++;
            switch (decompositionState.getState()) {

                case WITHIN:
                    result.addAll(node.getAllItems());
                    break;

                case INTERSECT:
                    Geometry newSearchPolygon = decompositionState.getShape();
                    newSearchPolygon.setUserData(searchPolygon.getUserData());

                    if(node.isLeaf()) {
                        Set<Geometry> candidateSet = node.getAllItems();

                        if(newSearchPolygon instanceof GeometryCollection) {
                            int num_geometries = newSearchPolygon.getNumGeometries();
                            for (int i = 0; i < num_geometries; i++) {
                                Geometry geometry = newSearchPolygon.getGeometryN(i);
                                for(Geometry item : candidateSet) {
                                    if(geometry.contains(item) || geometry.intersects(item)) {
                                        result.add(item);
                                    }
                                }
                            }
                        } else {
                            for(Geometry item : candidateSet) {
                                if(newSearchPolygon.contains(item) || newSearchPolygon.intersects(item)) {
                                    result.add(item);
                                }
                            }
                        }

                    } else {
                        result.addAll(startDecomposition(node, newSearchPolygon));
                    }

                    break;

                case OUTSIDE:
                    // DO NOTHING
                    break;

            }
        }
        return result;

    }

    private Set decomposition(Geometry originalSearchPolygon) {

        long start_time = System.nanoTime();
        double duration;
        String log;

        String executorId = SparkEnv.get().executorId();
        String name = (String) originalSearchPolygon.getUserData();
        int n = originalSearchPolygon.getNumPoints();

        Set<Geometry> result = new HashSet<>();

        Geometry searchPolygon;
        Decomposition partitionIntersection = new Decomposition(root.getBoundary(), originalSearchPolygon);

        switch (partitionIntersection.getState()) {
            case WITHIN:
                result = getAllData();
                duration = (System.nanoTime() - start_time)/1e+9;
                duration /= 60;
                log = "DECOMPOSITION\t" + executorId + "\t" + name + "\t" + n + "\t" + result.size() + "\t" + 1 + "\t" + totalNumberOfData + "\t" + duration + "\n";
                System.out.println(log);
                return result;

            case INTERSECT:
                searchPolygon = partitionIntersection.getShape();
                n = searchPolygon.getNumPoints();
                intersection = 1;
                sum = 0;
                result = startDecomposition(root, searchPolygon);
                duration = (System.nanoTime() - start_time)/1e+9;
                duration /= 60;
                log = "DECOMPOSITION\t" + executorId + "\t" + name + "\t" + n + "\t" + sum + "\t" + intersection + "\t" + totalNumberOfData + "\t" + duration + "\n";
                System.out.println(log);
                return result;

            case INVALID:
                duration = (System.nanoTime() - start_time)/1e+9;
                duration /= 60;
                log = "Invalid Geometry: " + originalSearchPolygon + "\t" + duration;
                System.out.println(log);
                return result;

            default:
                duration = (System.nanoTime() - start_time)/1e+9;
                duration /= 60;
                log = "DECOMPOSITION\t" + executorId + "\t" + name + "\t" + n + "\t" + 0 + "\t" + 1 + "\t" + totalNumberOfData + "\t" + duration + "\n";
                System.out.println(log);
                return result;
        }


    }

    @Override
    public Set query(Envelope searchEnvelope) {
        return null;
    }

    @Override
    public Set query(GeometryCollection polygonLayer, QueryMethod method) {

        String executorId = SparkEnv.get().executorId();
        long start_time = System.nanoTime();
        double duration;
        String log;

        Set results = new HashSet();

        int numberOfGeometries = polygonLayer.getNumGeometries();

        for (int i = 0; i < numberOfGeometries; i++) {

            Geometry geometry = polygonLayer.getGeometryN(i);
            String geometryName = geometry.getUserData().toString();

            results.add(new Tuple2<String, Set>(geometryName, decomposition(geometry)));
        }

        duration = (System.nanoTime() - start_time)/1e+9;
        duration /= 60;

        log = "POLYGON-LAYER\t" + executorId + "\t" + duration + "\n";
        System.out.println(log);

        return results;
    }

    @Override
    public Set count(GeometryCollection polygonLayer, QueryMethod method) {

        String executorId = SparkEnv.get().executorId();
        long start_time = System.nanoTime();
        double duration;
        String log;

        Set results = new HashSet();

        int numberOfGeometries = polygonLayer.getNumGeometries();

        for (int i = 0; i < numberOfGeometries; i++) {

            Geometry geometry = polygonLayer.getGeometryN(i);
            String geometryName = geometry.getUserData().toString();
            int size = decomposition(geometry).size();

            results.add(new Tuple2<String, Integer>(geometryName, size));
        }

        duration = (System.nanoTime() - start_time)/1e+9;
        duration /= 60;

        log = "POLYGON-LAYER\t" + polygonLayer.getUserData() + "\t" + executorId + "\t" + results.size() + "\t" + duration + "\n";
        System.out.println(log);

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
    public Set count(Geometry searchPolygon, QueryMethod method) {
        return null;
    }

    @Override
    public Set count() {
        Set<Object> result = new HashSet<>();
        result.add(totalNumberOfData);
        return result;
    }
}
