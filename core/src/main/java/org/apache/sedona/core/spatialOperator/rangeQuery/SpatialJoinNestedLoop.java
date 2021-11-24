package org.apache.sedona.core.spatialOperator.rangeQuery;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SpatialJoinNestedLoop<T extends Geometry, U extends Geometry>
        implements FlatMapFunction2<Iterator<T>, Iterator<U>, Pair<String, Integer>>, Serializable
{

    @Override
    public Iterator<Pair<String, Integer>> call(Iterator<T> iteratorObject, Iterator<U> iteratorWindow)
            throws Exception
    {
        List<Pair<String, Integer>> result = new ArrayList<>();

        List<T> queryObjects = new ArrayList<>();
        while (iteratorObject.hasNext()) {
            queryObjects.add(iteratorObject.next());
        }

        while (iteratorWindow.hasNext()) {
            U window = iteratorWindow.next();
            Envelope MBR = window.getEnvelopeInternal();
            int count = 0;
            for (T object : queryObjects) {
                if(MBR.intersects(object.getEnvelopeInternal())){
                    if (window.intersects(object)) {
                        count++;
                    }
                }
            }
            result.add(Pair.of(window.getUserData().toString(), count));
        }
        return result.iterator();
    }
}
