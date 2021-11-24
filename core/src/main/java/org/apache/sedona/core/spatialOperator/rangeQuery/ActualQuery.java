package org.apache.sedona.core.spatialOperator.rangeQuery;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.sedona.core.spatialLocalIndex.SpatialLocalIndex;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class ActualQuery<U extends Geometry>
        implements FlatMapFunction2<Iterator<Tuple2<Integer, SpatialLocalIndex>>, Iterator<U>, Double>, Serializable {

    SpatialLocalIndex.QueryMethod queryMethod;

    public ActualQuery(SpatialLocalIndex.QueryMethod queryMethod) {
        this.queryMethod = queryMethod;
    }

    @Override
    public Iterator<Double> call(Iterator<Tuple2<Integer, SpatialLocalIndex>> indexIterator, Iterator<U> streamShapes)
            throws ExecutionException, InterruptedException {

        Set result = new HashSet();

        if (!indexIterator.hasNext() || !streamShapes.hasNext()) {
            return result.iterator();
        }

        SpatialLocalIndex treeIndex = indexIterator.next()._2;


        Geometry[] geometries;
        ArrayList<Geometry> geometryArrayList = new ArrayList<>();

        while (streamShapes.hasNext()) {
            Geometry streamShape = streamShapes.next();
            geometryArrayList.add(streamShape);
        }
        geometries = new Geometry[geometryArrayList.size()];
        for(int i = 0; i < geometryArrayList.size(); ++i) {
            geometries[i] = geometryArrayList.get(i);
        }

        GeometryCollection queryGeometry = new GeometryCollection(geometries, new GeometryFactory());

        result = treeIndex.actualOperations(queryGeometry, queryMethod);

        return result.iterator();
    }

}