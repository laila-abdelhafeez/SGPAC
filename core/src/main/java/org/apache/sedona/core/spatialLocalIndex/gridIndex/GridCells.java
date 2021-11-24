package org.apache.sedona.core.spatialLocalIndex.gridIndex;

import javafx.util.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

public class GridCells implements Serializable {

    private HashMap<Pair<Integer, Integer>, GridCell> gridCells;

    public GridCells() {
        this.gridCells = new HashMap<>();
    }

    public void addCell(int x_index, int y_index, GridCell cell) {
        gridCells.put(new Pair<>(x_index, y_index), cell);
    }

    public boolean isExist(int x_index, int y_index) {
        return gridCells.containsKey(new Pair<>(x_index, y_index));
    }

    public GridCell getCell(int x_index, int y_index) {
        return gridCells.get(new Pair<>(x_index, y_index));
    }

    public Set<Pair<Integer, Integer>> getIndexes() { return gridCells.keySet(); }

    public HashMap<Pair<Integer, Integer>, GridCell> getCells() { return gridCells; }

}
