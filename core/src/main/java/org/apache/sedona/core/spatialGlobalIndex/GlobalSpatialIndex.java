/*
 * FILE: GlobalSpatialIndex
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
package org.apache.sedona.core.spatialGlobalIndex;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.apache.spark.Partitioner;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.joinJudgement.DedupParams;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

abstract public class GlobalSpatialIndex
        extends Partitioner
        implements Serializable
{

    protected final GridType gridType;
    protected final List<Envelope> grids;

    protected GlobalSpatialIndex(GridType gridType, List<Envelope> grids)
    {
        this.gridType = gridType;
        this.grids = Objects.requireNonNull(grids, "grids");
    }


    abstract public <T extends Geometry> Iterator<Tuple2<Integer, T>>
    placeObject(T spatialObject)
            throws Exception;


    @Nullable
    abstract public DedupParams getDedupParams();

    @Override
    public int getPartition(Object key)
    {
        return (int) key;
    }

    public List<Envelope> getGrids() { return grids; }

}
