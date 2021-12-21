package org.apache.sedona.core.spatialLocalIndex.RTree;

import org.apache.sedona.core.spatialLocalIndex.SpatialLocalIndex.Decomposition;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.strtree.AbstractNode;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class MSTRtree extends STRtree {

    public MSTRtree(int nodeCapacity) {
        super(nodeCapacity);
    }

    enum STATE {INSIDE, OUTSIDE, INTERSECTS}

    private Geometry createEnvelopeGeometry(Envelope envelope) {
        Coordinate[] coordinates = new Coordinate[5];
        coordinates[0] = new Coordinate(envelope.getMinX(), envelope.getMinY());
        coordinates[1] = new Coordinate(envelope.getMinX(), envelope.getMaxY());
        coordinates[2] = new Coordinate(envelope.getMaxX(), envelope.getMaxY());
        coordinates[3] = new Coordinate(envelope.getMaxX(), envelope.getMinY());
        coordinates[4] = coordinates[0];

        GeometryFactory geometryFactory = new GeometryFactory();
       return geometryFactory.createPolygon(coordinates);
    }

    private STATE intersects(MSTRtree queryIndex, AbstractNode node, double maxX) {

        GeometryFactory factory = new GeometryFactory();
        Envelope nodeEnvelope = (Envelope) node.getBounds();

        List<Object> candidateIntersections = queryIndex.query(nodeEnvelope);
        Geometry nodeGeometry = createEnvelopeGeometry(nodeEnvelope);

        boolean intersects = false;

        for(Object candidateItem : candidateIntersections) {
            Geometry candidateGeometry = (Geometry) candidateItem;
            if(nodeGeometry.intersects(candidateGeometry)) {
                intersects = true;
                break;
            }
        }

        if(intersects) return STATE.INTERSECTS;

        Coordinate[] coordinates = new Coordinate[2];
        coordinates[0] = new Coordinate(nodeEnvelope.getMaxX(), nodeEnvelope.getMaxY());
        coordinates[1] = new Coordinate(maxX, nodeEnvelope.getMaxY());
        LineString line = factory.createLineString(coordinates);
        List<Object> lineCandidates = queryIndex.query(line.getEnvelopeInternal());

        int numIntersections = 0;

        for(Object candidateItem : lineCandidates) {
            Geometry candidateGeometry = (Geometry) candidateItem;
            if(candidateGeometry.intersects(line)) ++numIntersections;
        }

        return numIntersections % 2 == 0 ? STATE.OUTSIDE : STATE.INSIDE;
    }


    public int count(MSTRtree queryIndex, Geometry queryPolygon) {
        build();
        if(isEmpty()) return 0;

        Stack<Tuple2<Object, Boolean>> nodes = new Stack<>();
        nodes.add(new Tuple2<>(root, false));
        return query(queryIndex, queryPolygon, nodes);
    }

    public int count(Geometry queryPolygon) {
        build();
        if(isEmpty()) return 0;

        Set<Geometry> result = new HashSet<>();
        query(queryPolygon, root, false, result);
        return result.size();
    }

    private void query(Geometry queryPolygon, Object node, boolean within, Set<Geometry> result) {
        if(node instanceof ItemBoundable) {
            ItemBoundable itemBoundable = (ItemBoundable) node;
            Geometry item = (Geometry) itemBoundable.getItem();
            if(within) {
                result.add(item);
            } else {
                if(queryPolygon instanceof GeometryCollection) {
                    GeometryCollection query = (GeometryCollection) queryPolygon;
                    for (int i = 0; i < query.getNumGeometries(); ++i) {
                        try {
                            if(query.getGeometryN(i).contains(item) || query.getGeometryN(i).intersects(item)){
                                result.add(item);
                            }
                        } catch (Exception ignore){}
                    }
                } else {
                    if(queryPolygon.contains(item) || queryPolygon.intersects(item)){
                        result.add(item);
                    }
                }
            }
        }

        if(node instanceof AbstractNode) {
            AbstractNode currentNode = (AbstractNode) node;
            if(within) {
                List childBoundables = currentNode.getChildBoundables();
                for(Object child : childBoundables) {
                    query(queryPolygon, child, true, result);
                }
            } else {
                Decomposition decomposition = new Decomposition((Envelope) currentNode.getBounds(), queryPolygon);
                switch (decomposition.getState()) {
                    case WITHIN:
                        for(Object child : currentNode.getChildBoundables()) {
                            query(decomposition.getShape(), child, true, result);
                        }
                        break;
                    case INTERSECT :
                        for(Object child : currentNode.getChildBoundables()) {
                            query(decomposition.getShape(), child, false, result);
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private int query(MSTRtree queryIndex, Geometry queryPolygon, Stack<Tuple2<Object, Boolean>> stack) {

        Envelope queryEnvelope = queryPolygon.getEnvelopeInternal();
        double maxX = queryEnvelope.getMaxX() + 0.01;
        Set<Geometry> result = new HashSet<>();



        while (!stack.isEmpty()) {

            Tuple2<Object, Boolean> entry = stack.pop();
            Object object = entry._1;
            Boolean included = entry._2;

            if(object instanceof AbstractNode) {
                AbstractNode node = (AbstractNode) object;
                if(included) {

                    List childBoundables = node.getChildBoundables();
                    for(Object child : childBoundables) {
                        stack.push(new Tuple2<>(child, true));
                    }
                } else {
                    switch (intersects(queryIndex, node, maxX)) {
                        case INSIDE :
                            for(Object child : node.getChildBoundables()) {
                                stack.push(new Tuple2<>(child, true));
                            }
                            break;
                        case INTERSECTS :
                            for(Object child : node.getChildBoundables()) {
                                stack.push(new Tuple2<>(child, false));
                            }
                            break;
                        default:
                            break;
                    }
                }
            } else if(object instanceof ItemBoundable) {
                ItemBoundable itemBoundable = (ItemBoundable) object;
                Geometry item = (Geometry) itemBoundable.getItem();

                if(included) {
                    result.add(item);
                } else if(queryPolygon.contains(item)){
                     result.add(item);
                }
            }
        }
        return result.size();
    }

    public int getAllDataCount() {
        build();
        if(isEmpty()) return 0;

        Set<Geometry> result = new HashSet<Geometry>(query(root.getBounds()));
        return result.size();
    }
}
