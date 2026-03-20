package com.example.myapp;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Data model for the serial message format grid.
 * Each row has an optional numeric key (0-255) and a sequence of data type specifiers.
 * Enforces key uniqueness, value range, type validity, and type continuity (no gaps).
 */
public class GridModel {

    public static final String[] VALID_TYPES = {
        "", "i8", "u8", "s0", "i16", "u16", "f32", "i32", "u32", "f64", "i64", "u64"
    };

    private static final Set<String> VALID_TYPE_SET = new HashSet<>(Arrays.asList(VALID_TYPES));

    private int cols; // total columns including key column
    private final List<Row> rows;

    public GridModel() {
        this.cols = 6; // 1 key + 5 type columns by default
        this.rows = new ArrayList<>();
        rows.add(new Row(cols - 1));
    }

    // --- Row class ---

    public static class Row {
        private Integer key;       // null = unassigned, 0-255
        private final String[] types;  // length = cols - 1

        public Row(int typeCols) {
            this.key = null;
            this.types = new String[typeCols];
            Arrays.fill(this.types, "");
        }

        public Integer getKey() { return key; }
        public String[] getTypes() { return types; }

        public String getType(int index) {
            if (index < 0 || index >= types.length) return "";
            return types[index];
        }

        /** Returns a display string like "i8, u16, f32" (non-blank types only). */
        public String getTypeSummary() {
            StringBuilder sb = new StringBuilder();
            for (String t : types) {
                if (t == null || t.isEmpty()) break;
                if (sb.length() > 0) sb.append(", ");
                sb.append(t);
            }
            return sb.toString();
        }

        public boolean isEmpty() {
            if (key != null) return false;
            for (String t : types) {
                if (t != null && !t.isEmpty()) return false;
            }
            return true;
        }
    }

    // --- Accessors ---

    public int getCols() { return cols; }
    public int getTypeCols() { return cols - 1; }
    public int getRowCount() { return rows.size(); }
    public Row getRow(int index) { return rows.get(index); }
    public List<Row> getRows() { return Collections.unmodifiableList(rows); }

    // --- Row operations ---

    public void addRow() {
        rows.add(new Row(cols - 1));
    }

    public void insertRow(int position) {
        if (position < 0) position = 0;
        if (position > rows.size()) position = rows.size();
        rows.add(position, new Row(cols - 1));
    }

    /** Removes row at position. Returns false if it's the last row (cannot delete). */
    public boolean removeRow(int position) {
        if (rows.size() <= 1) return false;
        if (position < 0 || position >= rows.size()) return false;
        rows.remove(position);
        return true;
    }

    // --- Key operations ---

    /**
     * Sets the key for a row. Returns null on success, or an error message.
     * Pass null to clear the key.
     */
    public String setKey(int rowIndex, Integer value) {
        if (value != null) {
            if (value < 0 || value > 255) {
                return "Key must be 0-255";
            }
            // Check uniqueness
            for (int i = 0; i < rows.size(); i++) {
                if (i == rowIndex) continue;
                if (value.equals(rows.get(i).key)) {
                    return "Key " + value + " is already used in row " + (i + 1);
                }
            }
        }
        rows.get(rowIndex).key = value;
        return null;
    }

    // --- Type operations ---

    /**
     * Sets a type in a row. Returns null on success, or an error message.
     * Enforces continuity: no blanks allowed before non-blank types.
     */
    public String setType(int rowIndex, int typeIndex, String value) {
        Row row = rows.get(rowIndex);
        if (typeIndex < 0 || typeIndex >= row.types.length) {
            return "Invalid type column index";
        }
        if (value == null) value = "";
        if (!VALID_TYPE_SET.contains(value)) {
            return "Invalid type: " + value;
        }

        // Temporarily set the value, then check continuity
        String oldValue = row.types[typeIndex];
        row.types[typeIndex] = value;

        if (!isContinuous(row)) {
            row.types[typeIndex] = oldValue;
            return "Types must be continuous (no gaps allowed)";
        }
        return null;
    }

    /** Check that non-blank types are contiguous from the left — no gaps. */
    private boolean isContinuous(Row row) {
        boolean seenBlank = false;
        for (String t : row.types) {
            boolean isBlank = (t == null || t.isEmpty());
            if (seenBlank && !isBlank) {
                return false; // gap detected
            }
            if (isBlank) seenBlank = true;
        }
        return true;
    }

    // --- JSON serialization ---

    /** Serializes to JSON. Rows are sorted by key (nulls last). */
    public JSONObject toJson() throws JSONException {
        // Sort rows: non-null keys ascending, null keys at end
        List<Row> sorted = new ArrayList<>(rows);
        Collections.sort(sorted, new Comparator<Row>() {
            @Override
            public int compare(Row a, Row b) {
                if (a.key == null && b.key == null) return 0;
                if (a.key == null) return 1;
                if (b.key == null) return -1;
                return Integer.compare(a.key, b.key);
            }
        });

        JSONObject root = new JSONObject();
        root.put("cols", cols);

        JSONArray dataArray = new JSONArray();
        for (Row row : sorted) {
            JSONObject rowObj = new JSONObject();
            if (row.key != null) {
                rowObj.put("key", row.key);
                JSONArray typesArray = new JSONArray();
                for (String t : row.types) {
                    typesArray.put(t != null ? t : "");
                }
                rowObj.put("types", typesArray);
            }
            // Empty rows → empty object {}
            dataArray.put(rowObj);
        }
        root.put("data", dataArray);
        return root;
    }

    /** Deserializes from JSON. Returns a new GridModel. */
    public static GridModel fromJson(String jsonString) throws JSONException {
        JSONObject root = new JSONObject(jsonString);
        int cols = root.getInt("cols");
        int typeCols = cols - 1;

        GridModel model = new GridModel();
        model.cols = cols;
        model.rows.clear();

        JSONArray dataArray = root.getJSONArray("data");
        for (int i = 0; i < dataArray.length(); i++) {
            JSONObject rowObj = dataArray.getJSONObject(i);
            Row row = new Row(typeCols);

            if (rowObj.has("key")) {
                row.key = rowObj.getInt("key");
            }
            if (rowObj.has("types")) {
                JSONArray typesArray = rowObj.getJSONArray("types");
                for (int j = 0; j < typesArray.length() && j < typeCols; j++) {
                    row.types[j] = typesArray.getString(j);
                }
            }
            model.rows.add(row);
        }

        // Ensure at least one row
        if (model.rows.isEmpty()) {
            model.rows.add(new Row(typeCols));
        }

        return model;
    }

    /** Creates a fresh default model. */
    public static GridModel createDefault() {
        return new GridModel();
    }
}
