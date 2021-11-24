package org.apache.sedona.core.spatialLocalIndex.quadTree;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.apache.commons.lang3.mutable.MutableInt;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class QuadNode implements Serializable {

    private static final int REGION_NW = 0;
    private static final int REGION_NE = 1;
    private static final int REGION_SW = 2;
    private static final int REGION_SE = 3;

    private Set<Geometry> items;

    private Envelope boundary;
    private int level;
    private int capacity;
    private int currSize;

    private QuadNode[] children = null;

    public boolean isLeaf() {
        return children == null;
    }

    public QuadNode[] getChildren() {
        return children;
    }
    public Envelope getBoundary() { return boundary; }

    private interface Visitor
    {
        boolean visit(QuadNode tree);
    }

    private void traverse(Visitor visitor)
    {
        if (!visitor.visit(this)) {
            return;
        }

        if (children != null) {
            children[REGION_NW].traverse(visitor);
            children[REGION_NE].traverse(visitor);
            children[REGION_SW].traverse(visitor);
            children[REGION_SE].traverse(visitor);
        }
    }

    public QuadNode(Envelope boundary, int level, int capacity) {
        this.boundary = boundary;
        this.level = level;
        this.capacity = capacity;
        items = new HashSet<>();
        this.currSize = 0;
    }

    public int insertItem(Geometry item) {
        QuadNode node = findNode(item.getEnvelopeInternal());
        if(node != null) {
            node.items.add(item);
            node.currSize++;
            return 1;
        }
        return 0;
    }

    private QuadNode findNode(Envelope elementMBR) {

        QuadNode foundNode = null;

        if(isLeaf() && currSize < capacity) {
            foundNode = this;
        } else if(isLeaf()) {
            boolean canSplit = split();

            if(canSplit) {
                foundNode = searchChildren(elementMBR);
            } else {
                foundNode = this;
            }

        }  else {
            foundNode = searchChildren(elementMBR);
        }

        return foundNode;
    }

    private QuadNode searchChildren(Envelope elementMBR) {

        QuadNode foundNode = null;

        for (QuadNode child : children) {
            if (child.boundary.contains(elementMBR)) {
                foundNode = child.findNode(elementMBR);
            }
        }

        if(foundNode == null) {
            for (QuadNode child : children) {
                if (child.boundary.intersects(elementMBR)) {
                    foundNode = child.findNode(elementMBR);
                }
            }
        }

        return foundNode;
    }

    private boolean split() {
        double newWidth = boundary.getWidth() / 2;
        double newHeight = boundary.getHeight() / 2;
        int newLevel = level + 1;

        if(newWidth > 1 && newHeight > 1) {

            children = new QuadNode[4];

            children[REGION_NW] = new QuadNode(new Envelope(
                    boundary.getMinX(),
                    boundary.getMaxX() - newWidth,
                    boundary.getMinY() + newHeight,
                    boundary.getMaxY()
            ), newLevel, capacity);

            children[REGION_NE] = new QuadNode(new Envelope(
                    boundary.getMinX() + newWidth,
                    boundary.getMaxX(),
                    boundary.getMinY() + newHeight,
                    boundary.getMaxY()
            ), newLevel, capacity);

            children[REGION_SW] = new QuadNode(new Envelope(
                    boundary.getMinX(),
                    boundary.getMaxX() - newWidth,
                    boundary.getMinY(),
                    boundary.getMaxY() - newHeight
            ), newLevel, capacity);

            children[REGION_SE] = new QuadNode(new Envelope(
                    boundary.getMinX() + newWidth,
                    boundary.getMaxX(),
                    boundary.getMinY(),
                    boundary.getMaxY() - newHeight
            ), newLevel, capacity);



            for (Geometry item : items) {
                for (QuadNode child : children) {
                    if (child.boundary.contains(item.getEnvelopeInternal()) || child.boundary.intersects(item.getEnvelopeInternal())) {
                        child.items.add(item);
                        child.currSize++;
                        break;
                    }
                }
            }

            items.clear();
            return true;

        }
        return false;

    }


    public int getTotalCount() {
        final MutableInt leafCount = new MutableInt(0);
        this.traverse(new Visitor() {
            @Override
            public boolean visit(QuadNode tree) {
                if (tree.isLeaf()) {
                    leafCount.add(tree.items.size());
                }
                return true;
            }
        });
        return leafCount.getValue();
    }

    public Set getAllItems() {
        final Set<Geometry> data = new HashSet<>();
        this.traverse(new Visitor() {
            @Override
            public boolean visit(QuadNode tree) {
                if (tree.isLeaf()) {
                    data.addAll(tree.items);
                }
                return true;
            }
        });
        return data;
    }
}
