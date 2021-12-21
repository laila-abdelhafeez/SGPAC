/*
 * FILE: GlobalQuadTree
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

import org.locationtech.jts.geom.*;
import org.apache.log4j.Logger;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.joinJudgement.DedupParams;
import org.apache.sedona.core.spatialGlobalIndex.quadTree.QuadTree;

import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.*;

public class GlobalQuadTree
        extends GlobalSpatialIndex
{
    private QuadTree quadTree;

    private final static Logger logger = Logger.getLogger(GlobalQuadTree.class);


    public GlobalQuadTree(QuadTree quadTree)
    {
        super(GridType.QUADTREE, getLeafGrids(quadTree));
        this.quadTree = quadTree;
        this.quadTree.dropElements();
    }

    @Override
    public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject)
            throws Exception
    {
        Objects.requireNonNull(spatialObject, "spatialObject");
        final Point point = spatialObject instanceof Point ? (Point) spatialObject : null;
        final Set<Tuple2<Integer, T>> result = new HashSet<>();
        final Envelope envelope = spatialObject.getEnvelopeInternal();

        if(point != null) {
            final List<QuadTree> matchedPartitions = quadTree.findZonesPoints(envelope);
            for (QuadTree matchedPartition : matchedPartitions) {
                if(matchedPartition.hasExtensions()) {
                    QuadTree scheduledExtension = matchedPartition.getScheduledExtension();
                    result.add(new Tuple2<>(scheduledExtension.getPartitionId(), spatialObject));
                } else {
                    result.add(new Tuple2<>(matchedPartition.getPartitionId(), spatialObject));
                }
            }
        } else {

            final List<QuadTree> matchedPartitions = quadTree.findZones(envelope);
            for (QuadTree partition : matchedPartitions) {
                result.add(new Tuple2<>(partition.getPartitionId(), spatialObject));
            }

        }


        return result.iterator();
    }



    @Nullable
    @Override
    public DedupParams getDedupParams()
    {
        return new DedupParams(grids);
    }

    @Override
    public int numPartitions()
    {
        return grids.size();
    }


    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof GlobalQuadTree)) {
            return false;
        }

        final GlobalQuadTree other = (GlobalQuadTree) o;
        return other.quadTree.equals(this.quadTree);
    }

    private static List<Envelope> getLeafGrids(QuadTree quadTree)
    {
        Objects.requireNonNull(quadTree, "quadTree");
        return quadTree.getLeafZones();
    }

}