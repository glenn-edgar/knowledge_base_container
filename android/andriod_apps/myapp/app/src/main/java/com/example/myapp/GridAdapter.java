package com.example.myapp;

import android.content.Context;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.Spinner;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

/**
 * RecyclerView adapter for the serial message format grid.
 * 
 * Visual layout per row: [Controls | Key | Type0 | Type1 | ... | TypeN-1]
 * Visual columns = grid.cols + 1 (controls column added at left).
 * 
 * Three view types:
 *   0 = controls (row number, insert/delete buttons)
 *   1 = key (numeric EditText, 0-255)
 *   2 = type (Spinner dropdown)
 */
public class GridAdapter extends RecyclerView.Adapter<RecyclerView.ViewHolder> {

    private static final int TYPE_CONTROLS = 0;
    private static final int TYPE_KEY = 1;
    private static final int TYPE_TYPE = 2;

    private final GridModel grid;
    private final Context context;
    private GridActionListener listener;

    public interface GridActionListener {
        void onInsertRow(int rowIndex);
        void onDeleteRow(int rowIndex);
        void onDataChanged();
    }

    public GridAdapter(Context context, GridModel grid) {
        this.context = context;
        this.grid = grid;
    }

    public void setListener(GridActionListener listener) {
        this.listener = listener;
    }

    /** Total visual columns = key + types + 1 for controls. */
    public int getVisualColCount() {
        return grid.getCols() + 1;
    }

    @Override
    public int getItemCount() {
        return grid.getRowCount() * getVisualColCount();
    }

    @Override
    public int getItemViewType(int position) {
        int col = position % getVisualColCount();
        if (col == 0) return TYPE_CONTROLS;
        if (col == 1) return TYPE_KEY;
        return TYPE_TYPE;
    }

    /** Convert flat adapter position to grid row index. */
    private int getRowIndex(int position) {
        return position / getVisualColCount();
    }

    /** Convert flat adapter position to type column index (0-based within types). */
    private int getTypeIndex(int position) {
        int col = position % getVisualColCount();
        return col - 2; // visual col 2 = type index 0
    }

    @NonNull
    @Override
    public RecyclerView.ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(parent.getContext());
        switch (viewType) {
            case TYPE_CONTROLS:
                return new ControlsViewHolder(
                    inflater.inflate(R.layout.grid_cell_controls, parent, false));
            case TYPE_KEY:
                return new KeyViewHolder(
                    inflater.inflate(R.layout.grid_cell_key, parent, false));
            case TYPE_TYPE:
            default:
                return new TypeViewHolder(
                    inflater.inflate(R.layout.grid_cell_type, parent, false));
        }
    }

    @Override
    public void onBindViewHolder(@NonNull RecyclerView.ViewHolder holder, int position) {
        int rowIndex = getRowIndex(position);

        switch (getItemViewType(position)) {
            case TYPE_CONTROLS:
                bindControls((ControlsViewHolder) holder, rowIndex);
                break;
            case TYPE_KEY:
                bindKey((KeyViewHolder) holder, rowIndex);
                break;
            case TYPE_TYPE:
                bindType((TypeViewHolder) holder, rowIndex, getTypeIndex(position));
                break;
        }
    }

    // ---- Controls cell ----

    private void bindControls(ControlsViewHolder vh, int rowIndex) {
        vh.rowNumber.setText(String.valueOf(rowIndex + 1));

        vh.insertBtn.setOnClickListener(v -> {
            if (listener != null) listener.onInsertRow(vh.getAbsoluteAdapterPosition() / getVisualColCount());
        });
        vh.deleteBtn.setOnClickListener(v -> {
            if (listener != null) listener.onDeleteRow(vh.getAbsoluteAdapterPosition() / getVisualColCount());
        });
    }

    static class ControlsViewHolder extends RecyclerView.ViewHolder {
        TextView rowNumber;
        ImageButton insertBtn, deleteBtn;

        ControlsViewHolder(View v) {
            super(v);
            rowNumber = v.findViewById(R.id.row_number);
            insertBtn = v.findViewById(R.id.btn_insert_row);
            deleteBtn = v.findViewById(R.id.btn_delete_row);
        }
    }

    // ---- Key cell ----

    private void bindKey(KeyViewHolder vh, int rowIndex) {
        GridModel.Row row = grid.getRow(rowIndex);

        // Prevent TextWatcher from firing during bind
        vh.suppressWatcher = true;
        vh.editText.setText(row.getKey() != null ? String.valueOf(row.getKey()) : "");
        vh.suppressWatcher = false;
        vh.currentRowIndex = rowIndex;
    }

    class KeyViewHolder extends RecyclerView.ViewHolder {
        EditText editText;
        boolean suppressWatcher = false;
        int currentRowIndex = -1;

        KeyViewHolder(View v) {
            super(v);
            editText = v.findViewById(R.id.edit_key);

            editText.addTextChangedListener(new TextWatcher() {
                @Override public void beforeTextChanged(CharSequence s, int start, int count, int after) {}
                @Override public void onTextChanged(CharSequence s, int start, int before, int count) {}

                @Override
                public void afterTextChanged(Editable s) {
                    if (suppressWatcher) return;
                    int ri = getAbsoluteAdapterPosition() / getVisualColCount();
                    if (ri < 0 || ri >= grid.getRowCount()) return;

                    String text = s.toString().trim();
                    Integer value = null;
                    if (!text.isEmpty()) {
                        try {
                            value = Integer.parseInt(text);
                        } catch (NumberFormatException e) {
                            return;
                        }
                    }
                    String error = grid.setKey(ri, value);
                    if (error != null) {
                        Toast.makeText(context, error, Toast.LENGTH_SHORT).show();
                        // Revert
                        suppressWatcher = true;
                        Integer oldKey = grid.getRow(ri).getKey();
                        editText.setText(oldKey != null ? String.valueOf(oldKey) : "");
                        suppressWatcher = false;
                    } else {
                        if (listener != null) listener.onDataChanged();
                    }
                }
            });
        }
    }

    // ---- Type cell ----

    private void bindType(TypeViewHolder vh, int rowIndex, int typeIndex) {
        GridModel.Row row = grid.getRow(rowIndex);
        String currentType = row.getType(typeIndex);

        vh.suppressListener = true;
        vh.currentRowIndex = rowIndex;
        vh.currentTypeIndex = typeIndex;

        // Find spinner position matching current value
        int selection = 0;
        for (int i = 0; i < GridModel.VALID_TYPES.length; i++) {
            if (GridModel.VALID_TYPES[i].equals(currentType)) {
                selection = i;
                break;
            }
        }
        vh.spinner.setSelection(selection);
        vh.suppressListener = false;
    }

    class TypeViewHolder extends RecyclerView.ViewHolder {
        Spinner spinner;
        boolean suppressListener = false;
        int currentRowIndex = -1;
        int currentTypeIndex = -1;

        TypeViewHolder(View v) {
            super(v);
            spinner = v.findViewById(R.id.spinner_type);

            // Build display labels: show "(blank)" for the empty string
            String[] labels = new String[GridModel.VALID_TYPES.length];
            for (int i = 0; i < GridModel.VALID_TYPES.length; i++) {
                labels[i] = GridModel.VALID_TYPES[i].isEmpty() ? "(blank)" : GridModel.VALID_TYPES[i];
            }
            ArrayAdapter<String> adapter = new ArrayAdapter<>(
                context, android.R.layout.simple_spinner_item, labels);
            adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
            spinner.setAdapter(adapter);

            spinner.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener() {
                @Override
                public void onItemSelected(AdapterView<?> parent, View view, int pos, long id) {
                    if (suppressListener) return;
                    int ri = getAbsoluteAdapterPosition() / getVisualColCount();
                    int ti = (getAbsoluteAdapterPosition() % getVisualColCount()) - 2;
                    if (ri < 0 || ri >= grid.getRowCount()) return;
                    if (ti < 0 || ti >= grid.getTypeCols()) return;

                    String newType = GridModel.VALID_TYPES[pos];
                    String error = grid.setType(ri, ti, newType);
                    if (error != null) {
                        Toast.makeText(context, error, Toast.LENGTH_SHORT).show();
                        // Revert spinner to previous value
                        suppressListener = true;
                        String current = grid.getRow(ri).getType(ti);
                        int revertPos = 0;
                        for (int i = 0; i < GridModel.VALID_TYPES.length; i++) {
                            if (GridModel.VALID_TYPES[i].equals(current)) {
                                revertPos = i;
                                break;
                            }
                        }
                        spinner.setSelection(revertPos);
                        suppressListener = false;
                    } else {
                        if (listener != null) listener.onDataChanged();
                    }
                }

                @Override
                public void onNothingSelected(AdapterView<?> parent) {}
            });
        }
    }
}
