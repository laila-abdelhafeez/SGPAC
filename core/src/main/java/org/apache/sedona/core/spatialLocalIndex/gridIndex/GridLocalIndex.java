package org.apache.sedona.core.spatialLocalIndex.gridIndex;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.apache.spark.SparkEnv;
import org.apache.sedona.core.spatialLocalIndex.SpatialLocalIndex;
import scala.Tuple2;

import java.util.*;

import javafx.util.Pair;


public class GridLocalIndex implements SpatialLocalIndex {

    private GridCells cells;
    private Envelope gridBoundary;
    final private int numXCells, numYCells;
    int length, height;
    boolean countsAvailable = false;
    private HashMap<Pair<Integer, Integer>, Integer> counts = new HashMap<>();

    public GridLocalIndex(int numXCells, int numYCells) {
        this.numXCells = numXCells;
        this.numYCells = numYCells;
    }

    @Override
    public void setBoundary(Envelope gridBoundary){
        int min_x = (int) Math.floor(gridBoundary.getMinX());
        int max_x = (int) Math.ceil(gridBoundary.getMaxX());

        int min_y = (int) Math.floor(gridBoundary.getMinY());
        int max_y = (int) Math.ceil(gridBoundary.getMaxY());

        length = (int) Math.ceil( (double) (max_x - min_x) / numXCells);
        height = (int) Math.ceil( (double) (max_y - min_y) / numYCells);

        max_x = min_x + length*numXCells;
        max_y = min_y + height*numYCells;

        this.gridBoundary = new Envelope(min_x, max_x, min_y, max_y);
        this.cells = new GridCells();
    }

    public void setCounts() {
        for (Map.Entry<Pair<Integer, Integer>, GridCell> entry : cells.getCells().entrySet()) {
            counts.put(entry.getKey(), entry.getValue().getCount());
        }
        countsAvailable = true;
    }

    private Set getAllData() {

        Set<Geometry> result = new HashSet<>();
        for(GridCell cell : cells.getCells().values()) {
            result.addAll(cell.getItems());
        }
        return result;
    }

    @Override
    public void insertItem(Geometry item) {

        double min_x = item.getEnvelopeInternal().getMinX();
        double min_y = item.getEnvelopeInternal().getMinY();

        int origin_x = (int)gridBoundary.getMinX();
        int origin_y = (int)gridBoundary.getMinY();

        int x_index = (int) Math.ceil((min_x - gridBoundary.getMinX()) / length) - 1;
        int y_index = (int) Math.ceil((min_y - gridBoundary.getMinY()) / height) - 1;

        if(cells.isExist(x_index, y_index)) {
            cells.getCell(x_index, y_index).insertItem(item);
        } else {
            GridCell cell = new GridCell();
            cell.setBoundary(new Envelope(origin_x + x_index * length, origin_x + (x_index + 1) * length,
                    origin_y + y_index * height, origin_y + (y_index + 1) * height));
            cell.insertItem(item);
            cells.addCell(x_index, y_index, cell);
        }
    }

    @Override
    public Set query(Envelope searchEnvelope) {

        Set<Object> result = new HashSet<>();

        int x_start_index = (int) Math.ceil((searchEnvelope.getMinX() - gridBoundary.getMinX()) / length) - 1;
        int y_start_index = (int) Math.ceil((searchEnvelope.getMinY() - gridBoundary.getMinY()) / height) - 1;

        int x_end_index = (int) Math.ceil((searchEnvelope.getMaxX() - gridBoundary.getMinX()) / length) - 1;
        int y_end_index = (int) Math.ceil((searchEnvelope.getMaxY() - gridBoundary.getMinY()) / height) - 1;

        for(int j = y_start_index; j <= y_end_index; j++) {
            if(cells.isExist(x_start_index, j)) {
                result.addAll(cells.getCell(x_start_index, j).getItemsInBoundary(searchEnvelope));
            }
            if(cells.isExist(x_end_index, j)) {
                result.addAll(cells.getCell(x_end_index, j).getItemsInBoundary(searchEnvelope));
            }
        }

        for(int i = x_start_index + 1; i < x_end_index; i++) {
            if(cells.isExist(i, y_start_index)) {
                result.addAll(cells.getCell(i, y_start_index).getItemsInBoundary(searchEnvelope));
            }

            if(cells.isExist(i, y_end_index)) {
                result.addAll(cells.getCell(i, y_end_index).getItemsInBoundary(searchEnvelope));
            }
        }

        for(int i = x_start_index + 1; i < x_end_index; i++){
            for(int j = y_start_index + 1; j < y_end_index; j++){
                if(cells.isExist(i, j)) {
                    result.addAll(cells.getCell(i, j).getItems());
                }
            }
        }

        return result;
    }

    @Override
    public Set query(Geometry searchPolygon, QueryMethod method) {
        return null;
    }

    @Override
    public Set query(GeometryCollection polygonLayer, QueryMethod method) {
        return null;
    }

    @Override
    public Set count() {
        setCounts();
        Set<Object> result = new HashSet<>();
        int totalNumberOfData = 0;
        for(Integer count : counts.values()) {
            totalNumberOfData += count;
        }
        result.add(totalNumberOfData);
        return result;
    }

    @Override
    public Set count(Geometry searchPolygon, QueryMethod method)  {
        Set<Integer> result = new HashSet<>();
        switch (method) {
            case FILTER_REFINE:
                result.add(filterRefineCount(searchPolygon));
                break;
            case DECOMPOSITION:
                result.add(decompositionCount(searchPolygon));
                break;
            case PREPROCESSED_DECOMPOSITION:
                break;
        }
        return result;
    }

    @Override
    public Set count(GeometryCollection polygonLayer, QueryMethod method)  {

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

            case MODIFIED_FILTER_REFINE:
                for(int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    String geometryName = geometry.getUserData().toString();
                    int size = modifiedFilterRefineCount(geometry);
                    results.add(new Tuple2(geometryName, size));
                }
                break;

            case DECOMPOSITION:
                for (int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    String geometryName = geometry.getUserData().toString();
                    int size = decompositionCount(geometry);
                    results.add(new Tuple2(geometryName, size));
                }
                break;

        }

        duration = (System.nanoTime() - start_time)/1e+9;
        duration /= 60;

        log = "POLYGON-LAYER\t" + method.name() + executorId + "\t"
                + polygonLayer.getUserData() + "\t" + results.size() + "\t" + duration + "\n";

        System.out.println(log);
        return results;
    }

    @Override
    public Set count(GeometryCollection polygonLayer, EstimateLevel level) {

        if(!countsAvailable) setCounts();
        int numberOfGeometries = polygonLayer.getNumGeometries();

        if(level == EstimateLevel.LAYER_LEVEL) {
            double fr_estimate = 0;
            double dec_estimate = 0;
            long start_time = System.nanoTime();
            double duration;
            for (int i = 0; i < numberOfGeometries; ++i) {
                Geometry geometry = polygonLayer.getGeometryN(i);
                fr_estimate += estimatePolygon(geometry, QueryMethod.FILTER_REFINE);
                dec_estimate += estimatePolygon(geometry, QueryMethod.DECOMPOSITION);
            }
            duration = (System.nanoTime() - start_time)/1e+9;
            duration /= 60;
            System.out.println("ESTIMATE \t" + numberOfGeometries + "\t" + duration + "\t" + fr_estimate + "\t" + dec_estimate);
            if(fr_estimate < dec_estimate) return count(polygonLayer, QueryMethod.FILTER_REFINE);
            else return count(polygonLayer, QueryMethod.DECOMPOSITION);
        } else {
            Set<Tuple2<String, Integer>> results = new HashSet<Tuple2<String, Integer>>();

            for (int i = 0; i < numberOfGeometries; ++i) {
                Geometry geometry = polygonLayer.getGeometryN(i);
                String geometryName = geometry.getUserData().toString();

                double fr_estimate = estimatePolygon(geometry, QueryMethod.FILTER_REFINE);
                double dec_estimate = estimatePolygon(geometry, QueryMethod.DECOMPOSITION);

                if(fr_estimate < dec_estimate) {
                    int size = filterRefineCount(geometry);
                    results.add(new Tuple2(geometryName, size));
                } else {
                    int size = decompositionCount(geometry);
                    results.add(new Tuple2(geometryName, size));
                }
            }
            return results;
        }
    }

    private double estimatePolygon(Geometry geometry, QueryMethod method) {

        double candidate_set;
        double n;
        double operations;

        switch (method) {
            case FILTER_REFINE: {
                candidate_set = getCountInMBR(geometry.getEnvelopeInternal())._1;
                n = geometry.getNumPoints();
                operations = candidate_set*n;
//                System.out.println("FR-ESTIMATE \t" + n + "\t" + candidate_set + "\t" + operations);
                return operations;
            }

            case DECOMPOSITION: {
                Tuple2<Integer, Integer> countIntersectTuple = getCountInMBR(geometry.getEnvelopeInternal());
                n = geometry.getNumPoints();
                candidate_set = countIntersectTuple._1;
                int cell_counts = countIntersectTuple._2;
                double intersect = cell_counts*n*Math.log10(n);
                double modified_n = n/cell_counts;
                operations = modified_n*candidate_set + intersect;
//                System.out.println("DEC-ESTIMATE \t" + modified_n + "\t" + candidate_set + "\t" + cell_counts + "\t" + n + "\t" + intersect + "\t" + operations);
                return operations;
            }

            default:
                return 0.0;

        }
    }

    @Override
    public Set estimate(GeometryCollection polygonLayer, QueryMethod method) {

        Set<Double> results = new HashSet<Double>();
        int numberOfGeometries = polygonLayer.getNumGeometries();
        double operations = 0;

        if(!countsAvailable) setCounts();

        switch (method) {
            case FILTER_REFINE:
                for(int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    operations += estimatePolygon(geometry, QueryMethod.FILTER_REFINE);
                }
                results.add(operations);
                break;

            case MODIFIED_FILTER_REFINE:
                for(int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    operations += estimatePolygon(geometry, QueryMethod.MODIFIED_FILTER_REFINE);
                }
                results.add(operations);
                break;

            case DECOMPOSITION:
                for (int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    operations += estimatePolygon(geometry, QueryMethod.DECOMPOSITION);
                }
                results.add(operations);
                break;

        }

        return results;
    }

    @Override
    public Set actualOperations(GeometryCollection polygonLayer, QueryMethod method) {
        Set<Double> results = new HashSet<Double>();
        int numberOfGeometries = polygonLayer.getNumGeometries();
        double operations = 0;

        switch (method) {
            case FILTER_REFINE:
                for(int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    operations += filterRefineOperation(geometry);
                }
                results.add(operations);
                break;

            case DECOMPOSITION:
                for(int i = 0; i < numberOfGeometries; ++i) {
                    Geometry geometry = polygonLayer.getGeometryN(i);
                    operations += decompositionCountOperations(geometry);
                }
                results.add(operations);
                break;

        }

        return results;
    }

    private Set refineCount(Geometry searchPolygon, GridCell gridCell) {
        Set result = new HashSet();
        List<Geometry> candidateSet = gridCell.getItems();

        for (Geometry tempResult : candidateSet) {
            if(searchPolygon.intersects(tempResult)) {
                result.add(tempResult);
            }
        }
        return result;
    }

    private Integer filterRefineCount(Geometry searchPolygon) {
        int result = 0;
        Envelope searchEnvelope = searchPolygon.getEnvelopeInternal();

        int x_start_index = (int) Math.ceil((searchEnvelope.getMinX() - gridBoundary.getMinX()) / length) - 1;
        int y_start_index = (int) Math.ceil((searchEnvelope.getMinY() - gridBoundary.getMinY()) / height) - 1;

        int x_end_index = (int) Math.ceil((searchEnvelope.getMaxX() - gridBoundary.getMinX()) / length) - 1;
        int y_end_index = (int) Math.ceil((searchEnvelope.getMaxY() - gridBoundary.getMinY()) / height) - 1;

        for(int i = x_start_index; i <= x_end_index; i++){
            for(int j = y_start_index; j <= y_end_index; j++) {
                if (cells.isExist(i, j)) {
                    result += refineCount(searchPolygon, cells.getCell(i, j)).size();
                }
            }
        }

        return result;
    }

    private Double filterRefineOperation(Geometry searchPolygon) {
        Envelope searchEnvelope = searchPolygon.getEnvelopeInternal();

        int x_start_index = (int) Math.ceil((searchEnvelope.getMinX() - gridBoundary.getMinX()) / length) - 1;
        int y_start_index = (int) Math.ceil((searchEnvelope.getMinY() - gridBoundary.getMinY()) / height) - 1;

        int x_end_index = (int) Math.ceil((searchEnvelope.getMaxX() - gridBoundary.getMinX()) / length) - 1;
        int y_end_index = (int) Math.ceil((searchEnvelope.getMaxY() - gridBoundary.getMinY()) / height) - 1;

        int candidate_set = 0;

        for(int i = x_start_index; i <= x_end_index; i++){
            for(int j = y_start_index; j <= y_end_index; j++) {
                if (cells.isExist(i, j)) {
                    candidate_set += cells.getCell(i,j).getCount();
                }
            }
        }

        return (double)candidate_set*searchPolygon.getNumPoints();
    }

    private Integer modifiedFilterRefineCount(Geometry originalSearchPolygon) {
        Decomposition partitionIntersection = new Decomposition(gridBoundary, originalSearchPolygon);
        if(partitionIntersection.getShape() != null) return filterRefineCount(partitionIntersection.getShape());
        return 0;
    }

    private Integer decompositionCount(Geometry originalSearchPolygon) {

        int result = 0;

        Geometry searchPolygon;
        Decomposition partitionIntersection = new Decomposition(gridBoundary, originalSearchPolygon);

        switch (partitionIntersection.getState()) {
            case WITHIN:
                result = getAllData().size();
                return result;
            case INTERSECT:
                searchPolygon = partitionIntersection.getShape();
                break;

            default:
                return result;
        }

        Envelope searchEnvelope = searchPolygon.getEnvelopeInternal();

        int x_start_index = (int) Math.ceil((searchEnvelope.getMinX() - gridBoundary.getMinX()) / length) - 1;
        int y_start_index = (int) Math.ceil((searchEnvelope.getMinY() - gridBoundary.getMinY()) / height) - 1;

        int x_end_index = (int) Math.ceil((searchEnvelope.getMaxX() - gridBoundary.getMinX()) / length) - 1;
        int y_end_index = (int) Math.ceil((searchEnvelope.getMaxY() - gridBoundary.getMinY()) / height) - 1;


        for(int i = x_start_index; i <= x_end_index; i++){
            for(int j = y_start_index; j <= y_end_index; j++){

                if(cells.isExist(i, j) && searchEnvelope.intersects(cells.getCell(i, j).getBoundary())) {

                    Decomposition decompositionState = new Decomposition(cells.getCell(i, j).getBoundary(), searchPolygon);

                    switch (decompositionState.getState()) {

                        case WITHIN:
                            result += cells.getCell(i, j).getItems().size();
                            break;

                        case INTERSECT:
                            decompositionState.getShape().setUserData(searchPolygon.getUserData());
                            result += refineCount(decompositionState.getShape(), cells.getCell(i, j)).size();
                            break;

                        case OUTSIDE:
                            break;

                    }
                }

            }
        }

        return result;
    }

    private Double decompositionCountOperations(Geometry originalSearchPolygon) {

        int refine = 0;
        double intersect = 0;

        Geometry searchPolygon;
        Decomposition partitionIntersection = new Decomposition(gridBoundary, originalSearchPolygon);
        intersect += originalSearchPolygon.getNumPoints()*Math.log(originalSearchPolygon.getNumPoints());

        if (partitionIntersection.getState() == PolygonIntersectState.INTERSECT) {
            searchPolygon = partitionIntersection.getShape();
        } else {
            return refine + intersect;
        }

        Envelope searchEnvelope = searchPolygon.getEnvelopeInternal();

        int x_start_index = (int) Math.ceil((searchEnvelope.getMinX() - gridBoundary.getMinX()) / length) - 1;
        int y_start_index = (int) Math.ceil((searchEnvelope.getMinY() - gridBoundary.getMinY()) / height) - 1;

        int x_end_index = (int) Math.ceil((searchEnvelope.getMaxX() - gridBoundary.getMinX()) / length) - 1;
        int y_end_index = (int) Math.ceil((searchEnvelope.getMaxY() - gridBoundary.getMinY()) / height) - 1;


        for(int i = x_start_index; i <= x_end_index; i++){
            for(int j = y_start_index; j <= y_end_index; j++){

                if(cells.isExist(i, j) && searchEnvelope.intersects(cells.getCell(i, j).getBoundary())) {

                    Decomposition decompositionState = new Decomposition(cells.getCell(i, j).getBoundary(), searchPolygon);
                    intersect += searchPolygon.getNumPoints() * Math.log10(searchPolygon.getNumPoints());

                    if (decompositionState.getState() == PolygonIntersectState.INTERSECT) {
                        decompositionState.getShape().setUserData(searchPolygon.getUserData());
                        refine += cells.getCell(i, j).getCount() * decompositionState.getShape().getNumPoints();
                    }
                }

            }
        }

        return refine+intersect;
    }

    private Tuple2<Integer, Integer> getCountInMBR(Envelope MBR) {
        int count = 0;
        int intersect = MBR.getArea() > length*height ? 1 : 0;

        int x_start_index = (int) Math.ceil((MBR.getMinX() - gridBoundary.getMinX()) / length) - 1;
        int y_start_index = (int) Math.ceil((MBR.getMinY() - gridBoundary.getMinY()) / height) - 1;

        int x_end_index = (int) Math.ceil((MBR.getMaxX() - gridBoundary.getMinX()) / length) - 1;
        int y_end_index = (int) Math.ceil((MBR.getMaxY() - gridBoundary.getMinY()) / height) - 1;

        for(int i = x_start_index; i <= x_end_index; i++){
            for(int j = y_start_index; j <= y_end_index; j++){
                if(cells.isExist(i, j)) {
                    count += counts.get(new Pair<>(i, j));
                    intersect += 1;
                }
            }
        }
        return new Tuple2<>(count, intersect);
    }



}
