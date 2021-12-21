/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core.spatialOperator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.rangeJudgement.RangeFilter;
import org.apache.sedona.core.rangeJudgement.RangeFilterUsingIndex;
import org.apache.sedona.core.spatialLocalIndex.SpatialLocalIndex;
import org.apache.sedona.core.spatialLocalIndex.SpatialLocalIndex.QueryMethod;
import org.apache.sedona.core.spatialOperator.rangeQuery.*;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.core.utils.CRSTransformation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class RangeQuery.
 */
public class RangeQuery
        implements Serializable
{

    public static <U extends Geometry, T extends Geometry> List<Double> LayerEstimate
            (SpatialRDD<T> data, SpatialRDD<U> queryPolygons, QueryMethod queryMethod) {
        FlatMapFunction2<Iterator<Tuple2<Integer, SpatialLocalIndex>>, Iterator<U>, Double> mapFunction;
        mapFunction = new EstimateQuery<>(queryMethod);
        return data.indexedPairRDD.zipPartitions(queryPolygons.spatialPartitionedRDD, mapFunction).collect();
    }

    public static <U extends Geometry, T extends Geometry> List<Double> LayerActual
            (SpatialRDD<T> data, SpatialRDD<U> queryPolygons, SpatialLocalIndex.QueryMethod queryMethod) {
        FlatMapFunction2<Iterator<Tuple2<Integer, SpatialLocalIndex>>, Iterator<U>, Double> mapFunction;
        mapFunction = new ActualQuery<>(queryMethod);

        return data.indexedPairRDD.zipPartitions(queryPolygons.spatialPartitionedRDD, mapFunction).collect();
    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<String, Integer> SpatialJoinFilterRefine
            (SpatialRDD<T> data, SpatialRDD<U> queryPolygons, boolean modified) {
        FlatMapFunction2<Iterator<Tuple2<Integer, SpatialLocalIndex>>, Iterator<U>, Tuple2<String, Integer>> mapFunction;

        if(modified) mapFunction = new SpatialJoinQuery<>(SpatialLocalIndex.QueryMethod.MODIFIED_FILTER_REFINE);
        else mapFunction = new SpatialJoinQuery<>(SpatialLocalIndex.QueryMethod.FILTER_REFINE);

        return data.indexedPairRDD.zipPartitions(queryPolygons.spatialPartitionedRDD, mapFunction)
                .mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerPair) throws Exception {
                        return stringIntegerPair;
                    }
                }).aggregateByKey(0, (Function2<Integer, Integer, Integer>) Integer::sum, (Function2<Integer, Integer, Integer>) Integer::sum);

    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<String, Integer> SpatialJoin
            (SpatialRDD<T> data, SpatialRDD<U> queryPolygons) {
        return data.spatialPartitionedRDD.zipPartitions(queryPolygons.spatialPartitionedRDD, new SpatialJoinNestedLoop<>())
                .mapToPair(new PairFunction<Pair<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Pair<String, Integer> stringIntegerPair) throws Exception {
                        return new Tuple2<>(stringIntegerPair.getKey(), stringIntegerPair.getValue());
                    }
                }).aggregateByKey(0, (Function2<Integer, Integer, Integer>) Integer::sum, (Function2<Integer, Integer, Integer>) Integer::sum);
    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<String, Integer> SpatialJoinDecomposition
            (SpatialRDD<T> data, SpatialRDD<U> queryPolygons)
    {
        FlatMapFunction2<Iterator<Tuple2<Integer, SpatialLocalIndex>>, Iterator<U>, Tuple2<String, Integer>> mapFunction;

        mapFunction = new SpatialJoinQuery<>(SpatialLocalIndex.QueryMethod.DECOMPOSITION);

        return data.indexedPairRDD.zipPartitions(queryPolygons.spatialPartitionedRDD, mapFunction)
                .mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerPair) throws Exception {
                        return stringIntegerPair;
                    }
                }).aggregateByKey(0, (Function2<Integer, Integer, Integer>) Integer::sum, (Function2<Integer, Integer, Integer>) Integer::sum);

    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<String, Integer> SpatialJoinOptimized
            (SpatialRDD<T> data, SpatialRDD<U> queryPolygons, SpatialLocalIndex.EstimateLevel estimateLevel)
    {
        FlatMapFunction2<Iterator<Tuple2<Integer, SpatialLocalIndex>>, Iterator<U>, Tuple2<String, Integer>> mapFunction;

        mapFunction = new SpatialJoinOptimized<>(estimateLevel);

        return data.indexedPairRDD.zipPartitions(queryPolygons.spatialPartitionedRDD, mapFunction)
                .mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerPair) throws Exception {
                        return stringIntegerPair;
                    }
                }).aggregateByKey(0, (Function2<Integer, Integer, Integer>) Integer::sum, (Function2<Integer, Integer, Integer>) Integer::sum);

    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<String, Integer> indexQueryPolygon
            (SpatialRDD<T> data, SpatialRDD<U> queryPolygons)
    {
        FlatMapFunction2<Iterator<Tuple2<Integer, SpatialLocalIndex>>, Iterator<U>, Tuple2<String, Integer>> mapFunction;

        mapFunction = new SpatialJoinQuery<>(QueryMethod.INDEX_QUERY);

        return data.indexedPairRDD.zipPartitions(queryPolygons.spatialPartitionedRDD, mapFunction)
                .mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerPair) throws Exception {
                        return stringIntegerPair;
                    }
                }).aggregateByKey(0, (Function2<Integer, Integer, Integer>) Integer::sum, (Function2<Integer, Integer, Integer>) Integer::sum);

    }

    public static <U extends Geometry, T extends Geometry> JavaPairRDD<String, Integer> testAll
            (SpatialRDD<T> data, SpatialRDD<U> queryPolygons)
    {
        FlatMapFunction2<Iterator<Tuple2<Integer, SpatialLocalIndex>>, Iterator<U>, Tuple2<String, Integer>> mapFunction;

        mapFunction = new SpatialJoinQuery<>(QueryMethod.ALL);

        return data.indexedPairRDD.zipPartitions(queryPolygons.spatialPartitionedRDD, mapFunction)
                .mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerPair) throws Exception {
                        return stringIntegerPair;
                    }
                }).aggregateByKey(0, (Function2<Integer, Integer, Integer>) Integer::sum, (Function2<Integer, Integer, Integer>) Integer::sum);

    }


    /**
     * Spatial range query. Return objects in SpatialRDD are covered/intersected by originalQueryGeometry
     *
     * @param spatialRDD the spatial RDD
     * @param originalQueryGeometry the original query window
     * @param considerBoundaryIntersection the consider boundary intersection
     * @param useIndex the use index
     * @return the java RDD
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(SpatialRDD<T> spatialRDD, U originalQueryGeometry, boolean considerBoundaryIntersection, boolean useIndex)
            throws Exception
    {
        U queryGeometry = originalQueryGeometry;
        if (spatialRDD.getCRStransformation()) {
            queryGeometry = CRSTransformation.Transform(spatialRDD.getSourceEpsgCode(), spatialRDD.getTargetEpgsgCode(), originalQueryGeometry);
        }

        if (useIndex == true) {
            if (spatialRDD.indexedRawRDD == null) {
                throw new Exception("[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
            }
            return spatialRDD.indexedRawRDD.mapPartitions(new RangeFilterUsingIndex(queryGeometry, considerBoundaryIntersection, true));
        }
        else {
            return spatialRDD.getRawSpatialRDD().filter(new RangeFilter(queryGeometry, considerBoundaryIntersection, true));
        }
    }

    /**
     * Spatial range query. Return objects in SpatialRDD are covered/intersected by queryWindow/Envelope
     *
     * @param spatialRDD the spatial RDD
     * @param queryWindow the original query window
     * @param considerBoundaryIntersection the consider boundary intersection
     * @param useIndex the use index
     * @return the java RDD
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(SpatialRDD<T> spatialRDD, Envelope queryWindow, boolean considerBoundaryIntersection, boolean useIndex)
            throws Exception
    {
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(queryWindow.getMinX(), queryWindow.getMinY());
        coordinates[1] = new Coordinate(queryWindow.getMinX(), queryWindow.getMaxY());
        coordinates[2] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMaxY());
        coordinates[3] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMinY());
        coordinates[4] = coordinates[0];
        GeometryFactory geometryFactory = new GeometryFactory();
        U queryGeometry = (U) geometryFactory.createPolygon(coordinates);
        return SpatialRangeQuery(spatialRDD, queryGeometry, considerBoundaryIntersection, useIndex);
    }

    /**
     * Spatial range query. Return objects in SpatialRDD cover/intersect by queryWindow/Envelope
     *
     * @param spatialRDD the spatial RDD
     * @param queryWindow the original query window
     * @param considerBoundaryIntersection the consider boundary intersection
     * @param useIndex the use index
     * @return the java RDD
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(Envelope queryWindow, SpatialRDD<T> spatialRDD, boolean considerBoundaryIntersection, boolean useIndex)
            throws Exception
    {
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(queryWindow.getMinX(), queryWindow.getMinY());
        coordinates[1] = new Coordinate(queryWindow.getMinX(), queryWindow.getMaxY());
        coordinates[2] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMaxY());
        coordinates[3] = new Coordinate(queryWindow.getMaxX(), queryWindow.getMinY());
        coordinates[4] = coordinates[0];
        GeometryFactory geometryFactory = new GeometryFactory();
        U queryGeometry = (U) geometryFactory.createPolygon(coordinates);
        return SpatialRangeQuery(queryGeometry, spatialRDD, considerBoundaryIntersection, useIndex);
    }

    /**
     * Spatial range query. Return objects in SpatialRDD cover/intersect originalQueryGeometry
     *
     * @param spatialRDD the spatial RDD
     * @param originalQueryGeometry the original query window
     * @param considerBoundaryIntersection the consider boundary intersection
     * @param useIndex the use index
     * @return the java RDD
     * @throws Exception the exception
     */
    public static <U extends Geometry, T extends Geometry> JavaRDD<T> SpatialRangeQuery(U originalQueryGeometry, SpatialRDD<T> spatialRDD, boolean considerBoundaryIntersection, boolean useIndex)
            throws Exception
    {
        U queryGeometry = originalQueryGeometry;
        if (spatialRDD.getCRStransformation()) {
            queryGeometry = CRSTransformation.Transform(spatialRDD.getSourceEpsgCode(), spatialRDD.getTargetEpgsgCode(), originalQueryGeometry);
        }

        if (useIndex == true) {
            if (spatialRDD.indexedRawRDD == null) {
                throw new Exception("[RangeQuery][SpatialRangeQuery] Index doesn't exist. Please build index on rawSpatialRDD.");
            }
            return spatialRDD.indexedRawRDD.mapPartitions(new RangeFilterUsingIndex(queryGeometry, considerBoundaryIntersection, false));
        }
        else {
            return spatialRDD.getRawSpatialRDD().filter(new RangeFilter(queryGeometry, considerBoundaryIntersection, false));
        }
    }
}
