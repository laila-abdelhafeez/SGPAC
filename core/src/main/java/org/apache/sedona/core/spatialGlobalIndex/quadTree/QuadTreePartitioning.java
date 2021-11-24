/*
 * FILE: QuadTreePartitioning
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
package org.apache.sedona.core.spatialGlobalIndex.quadTree;

import org.locationtech.jts.geom.Envelope;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.List;

public class QuadTreePartitioning
        implements Serializable
{
    private final QuadTree partitionTree;
    private final static Logger logger = Logger.getLogger(QuadTreePartitioning.class);


    public QuadTreePartitioning(List<Envelope> data, Envelope boundary, int capacity, int maxLevel)
            throws Exception
    {

        partitionTree = new QuadTree(boundary, 0, capacity, maxLevel);

        logger.info("Root = " + boundary.toString());

        int sum_inserted = 0;
        for (final Envelope dataPoint : data) {
            sum_inserted += partitionTree.insertItem(dataPoint);
        }

        logger.info("Total number of inserted data points = " + sum_inserted);
        logger.info("Total number of data points          = " + data.size());

        int totalSum = 0;
        List<QuadTree> leaves = partitionTree.getLeaves();
        for (QuadTree leave : leaves) {
            totalSum += leave.getNodeNum();
        }

        logger.info("Number of leaves                 = " + leaves.size());
        logger.info("Total number of points in leaves = " + totalSum);

        partitionTree.assignPartitionIds();

    }

    public QuadTree getPartitionTree()
    {
        return this.partitionTree;
    }
}
