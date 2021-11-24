/*
 * FILE: QuadTree
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
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class QuadTree
        implements Serializable
{
    private final int maxItemsPerZone;

    private final int maxLevel;

    private Integer partitionId = -1;

    private final int level;
    private int nodeNum = 0;

    private QuadTree[] regions = null;
    private List<QuadTree> extensions = null;
    private int availableExtensionIndex = -1;
    private int scheduledExtension = -1;

    private final List<QuadElement> nodes = new ArrayList<>();

    private final Envelope MBR;

    private static final int REGION_NW = 0;
    private static final int REGION_NE = 1;
    private static final int REGION_SW = 2;
    private static final int REGION_SE = 3;

    private final static Logger logger = Logger.getLogger(QuadTree.class);



    QuadTree(Envelope MBR, int level, int maxItemsPerZone, int maxLevel)
    {
        this.maxItemsPerZone = maxItemsPerZone;
        this.maxLevel = maxLevel;
        this.MBR = MBR;
        this.level = level;
    }

    public Envelope getMBR() {
        return MBR;
    }

    public List<QuadTree> getExtensions() {
        return extensions;
    }

    private QuadTree newQuadTree(Envelope MBR, int level)
    {
        return new QuadTree(MBR, level, this.maxItemsPerZone, this.maxLevel);
    }

    int getNodeNum() {
        return nodeNum;
    }

    private boolean split()
    {
        double newWidth = MBR.getWidth() / 2;
        double newHeight = MBR.getHeight() / 2;
        int newLevel = level + 1;

        if((newWidth > 1 && newHeight > 1) && !hasExtensions()) {

            QuadTree[] newRegions = new QuadTree[4];

            newRegions[REGION_NW] = newQuadTree(new Envelope(
                    MBR.getMinX(),
                    MBR.getMaxX() - newWidth,
                    MBR.getMinY() + newHeight,
                    MBR.getMaxY()
            ), newLevel);

            newRegions[REGION_NE] = newQuadTree(new Envelope(
                    MBR.getMinX() + newWidth,
                    MBR.getMaxX(),
                    MBR.getMinY() + newHeight,
                    MBR.getMaxY()
            ), newLevel);

            newRegions[REGION_SW] = newQuadTree(new Envelope(
                    MBR.getMinX(),
                    MBR.getMaxX() - newWidth,
                    MBR.getMinY(),
                    MBR.getMaxY() - newHeight
            ), newLevel);

            newRegions[REGION_SE] = newQuadTree(new Envelope(
                    MBR.getMinX() + newWidth,
                    MBR.getMaxX(),
                    MBR.getMinY(),
                    MBR.getMaxY() - newHeight
            ), newLevel);



            for (QuadElement node : nodes) {

                boolean found = false;

                for (QuadTree region : newRegions) {
                    if (region.MBR.contains(node.getEnvelope())) {
                        region.nodes.add(node);
                        region.nodeNum++;
                        found = true;
                    }
                }

                if(!found) {
                    for (QuadTree region : newRegions) {
                        if (region.MBR.intersects(node.getEnvelope())) {
                            region.nodes.add(node);
                            region.nodeNum ++;
                        }
                    }
                }

            }

            regions = newRegions;
            nodes.clear();
            return true;

        }
        return false;

    }

    private QuadTree extend() {

        QuadTree node;

        if(extensions == null){
            extensions = new ArrayList<>();
            node = newQuadTree(MBR, level);
            extensions.add(node);
            availableExtensionIndex = 0;

        } else if(extensions.get(availableExtensionIndex).nodeNum < maxItemsPerZone) {
            node = extensions.get(availableExtensionIndex);
        } else {
            node = newQuadTree(MBR, level);
            extensions.add(node);
            availableExtensionIndex++;
        }

        return node;
    }


    private QuadTree searchChildren(Envelope elementMBR) {

        QuadTree foundRegion = null;

        for (QuadTree region : regions) {
            if (region.MBR.contains(elementMBR)) {
                foundRegion = region.findRegion(elementMBR);
            }
        }

        if(foundRegion == null) {
            for (QuadTree region : regions) {
                if (region.MBR.intersects(elementMBR)) {
                    foundRegion = region.findRegion(elementMBR);
                }
            }
        }

        return foundRegion;
    }

    private QuadTree findRegion(Envelope elementMBR) {

        QuadTree foundRegion = null;

        if(isLeaf() && nodeNum < maxItemsPerZone) {
            foundRegion = this;

        } else if(isLeaf() && level < maxLevel) {
            boolean canSplit = split();

            if(canSplit) {
                foundRegion = searchChildren(elementMBR);

            } else {
                foundRegion = extend();
            }
        } else if(isLeaf()) {
            foundRegion = this;

        } else {
            foundRegion = searchChildren(elementMBR);
        }

        return foundRegion;
    }


    int insertItem(Envelope elementMBR) {
        QuadTree region = findRegion(elementMBR);
        if(region != null){
            region.nodes.add(new QuadElement(elementMBR));
            region.nodeNum++;
            return 1;
        } else {
            return 0;
        }
    }

    public void dropElements()
    {
        traverse(new Visitor()
        {
            @Override
            public boolean visit(QuadTree tree)
            {
                if(tree.isLeaf()) {
                    if(tree.hasExtensions()){
                        for(QuadTree extension : tree.extensions) {
                            extension.nodes.clear();
                        }
                    }
                }

                tree.nodes.clear();
                return true;
            }
        });
    }

    private interface Visitor
    {
        boolean visit(QuadTree tree);
    }

    private void traverse(Visitor visitor)
    {
        if (!visitor.visit(this)) {
            return;
        }

        if (regions != null) {
            regions[REGION_NW].traverse(visitor);
            regions[REGION_NE].traverse(visitor);
            regions[REGION_SW].traverse(visitor);
            regions[REGION_SE].traverse(visitor);
        }
    }


    private boolean isLeafNonZero()
    {
        return regions == null && nodeNum > 0;
    }

    private boolean isLeaf()
    {
        return regions == null;
    }


    public List<Envelope> getLeafZones()
    {
        final List<Envelope> leafZones = new ArrayList<>();
        traverse(new Visitor()
        {
            @Override
            public boolean visit(QuadTree tree)
            {
                if (tree.isLeafNonZero()) {
//                    tree.MBR.setUserData(tree.nodeNum);
                    leafZones.add(tree.MBR);
                    if(tree.hasExtensions()) {

                        for(QuadTree extension : tree.extensions) {
//                            extension.MBR.setUserData(extension.nodeNum);
                            leafZones.add(extension.MBR);
                        }

                    }
                }
                return true;
            }
        });

        return leafZones;
    }

    public int getTotalNumLeafNode()
    {
        final MutableInt leafCount = new MutableInt(0);
        traverse(new Visitor()
        {
            @Override
            public boolean visit(QuadTree tree)
            {
                if (tree.isLeafNonZero()) {
                    leafCount.increment();

                    if(tree.hasExtensions()) {
                        leafCount.add(tree.extensions.size());
                    }

                }
                return true;
            }
        });

        return leafCount.getValue();
    }

    public List<QuadTree> getLeaves()
    {
        final List<QuadTree> leaves = new ArrayList<>();
        traverse(new Visitor()
        {
            @Override
            public boolean visit(QuadTree tree)
            {
                if (tree.isLeafNonZero()) {
                    leaves.add(tree);

                    if(tree.hasExtensions()) {
                        leaves.addAll(tree.extensions);
                    }

                }
                return true;
            }
        });

        return leaves;
    }

    public List<QuadTree> getLeavesWithoutExtensions()
    {
        final List<QuadTree> leaves = new ArrayList<>();
        traverse(new Visitor()
        {
            @Override
            public boolean visit(QuadTree tree)
            {
                if (tree.isLeafNonZero()) {
                    leaves.add(tree);
                }
                return true;
            }
        });

        return leaves;
    }

    public List<QuadTree> findZonesPoints(Envelope queryMBR)
    {
        final List<QuadTree> matches = new ArrayList<>();
        final List<QuadTree> intersects = new ArrayList<>();

        traverse(new Visitor()
        {
            @Override
            public boolean visit(QuadTree tree)
            {
                if (tree.MBR.contains(queryMBR)) {
                    if(tree.isLeafNonZero()) {
                        matches.add(tree);
//                        if(tree.hasExtensions()) {
//                            matches.addAll(tree.extensions);
//                        }
                    }
                    return true;

                } else if (tree.MBR.intersects(queryMBR)) {
                    if(tree.isLeafNonZero()) {
                        matches.add(tree);
//                        if(tree.hasExtensions()) {
//                            matches.addAll(tree.extensions);
//                        }
                    }
                    return true;

                } else {
                    return false;
                }
            }
        });

        if(!matches.isEmpty()) return matches;
        return intersects;
    }

    public QuadTree getScheduledExtension() {
        scheduledExtension++;
        if(scheduledExtension > extensions.size()) scheduledExtension = 0;
        if(scheduledExtension == 0) return this;
        return extensions.get(scheduledExtension-1);
    }

    public List<QuadTree> findZones(Envelope queryMBR)
    {
        final List<QuadTree> matches = new ArrayList<>();
        final List<QuadTree> intersects = new ArrayList<>();

        traverse(new Visitor()
        {
            @Override
            public boolean visit(QuadTree tree)
            {
                if (tree.MBR.contains(queryMBR) || tree.MBR.intersects(queryMBR)) {
                    if(tree.isLeafNonZero()) {
                        matches.add(tree);
                        if(tree.hasExtensions()) {
                            matches.addAll(tree.extensions);
                        }
                    }
                    return true;

                } else {
                    return false;
                }
            }
        });

        if(!matches.isEmpty()) return matches;
        return intersects;
    }


    public boolean hasExtensions() {
        return extensions != null;
    }

    void assignPartitionIds()
    {
        traverse(new Visitor()
        {
            private int currentPartitionId = 0;

            @Override
            public boolean visit(QuadTree tree)
            {
                if (tree.isLeafNonZero()) {
                    tree.partitionId = currentPartitionId;
                    currentPartitionId++;

                    if(tree.hasExtensions()) {
                        for(QuadTree extension : tree.extensions) {
                            extension.partitionId = currentPartitionId;
                            currentPartitionId++;
                        }
                    }

                }
                return true;
            }
        });

    }


    public int getPartitionId(){
        return partitionId;
    }

}