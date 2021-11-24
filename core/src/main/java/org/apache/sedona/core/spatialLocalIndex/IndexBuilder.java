/*
 * FILE: IndexBuilder
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.apache.sedona.core.spatialLocalIndex;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

public class IndexBuilder<T extends Geometry>
        implements PairFlatMapFunction<Iterator<T>, Integer, SpatialLocalIndex> {

    private SpatialLocalIndex spatialIndex = null;
    private List<Envelope> partitionGrids = null;

    public SpatialLocalIndex createIndex() {
        return spatialIndex;
    }

    public void setPartitionGrids(List<Envelope> partitionGrids) {
        this.partitionGrids = partitionGrids;
    }

    @Override
    public Iterator<Tuple2<Integer, SpatialLocalIndex>> call(Iterator<T> objectIterator) throws Exception  {
        int partitionIndex = TaskContext.getPartitionId();

        spatialIndex = createIndex();
        spatialIndex.setBoundary(partitionGrids.get(partitionIndex));

        while (objectIterator.hasNext()) {
            Geometry spatialObject = objectIterator.next();
            spatialIndex.insertItem(spatialObject);
        }
        Set<Tuple2<Integer, SpatialLocalIndex>> result = new HashSet<>();
        result.add(new Tuple2<>(partitionIndex, spatialIndex));

        return result.iterator();
    }

}
