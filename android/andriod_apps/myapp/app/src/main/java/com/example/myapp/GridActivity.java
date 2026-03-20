package com.example.myapp;

import android.app.AlertDialog;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.text.InputType;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import androidx.activity.result.ActivityResultLauncher;
import androidx.activity.result.contract.ActivityResultContracts;
import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.GridLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import java.util.List;

/**
 * Grid editor activity for serial message format definitions.
 * Provides row add/insert/delete, save/load to internal storage,
 * SAF import/export, and email attachment import.
 */
public class GridActivity extends AppCompatActivity implements GridAdapter.GridActionListener {

    private GridModel grid;
    private GridAdapter adapter;
    private RecyclerView recyclerView;
    private ConfigFileManager fileManager;
    private boolean hasUnsavedChanges = false;

    // SAF launchers
    private ActivityResultLauncher<String[]> importLauncher;
    private ActivityResultLauncher<String> exportLauncher;
    private GridModel pendingExportModel; // held between export launch and result

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_grid);

        fileManager = new ConfigFileManager(this);
        grid = GridModel.createDefault();
        setupRecyclerView();
        setupToolbar();
        setupSafLaunchers();
        handleIncomingIntent(getIntent());
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        handleIncomingIntent(intent);
    }

    // ---- RecyclerView setup ----

    private void setupRecyclerView() {
        recyclerView = findViewById(R.id.grid_recycler);
        adapter = new GridAdapter(this, grid);
        adapter.setListener(this);

        GridLayoutManager layoutManager = new GridLayoutManager(
            this, adapter.getVisualColCount());
        recyclerView.setLayoutManager(layoutManager);
        recyclerView.setAdapter(adapter);
    }

    /** Rebinds the adapter to a new grid model (after load/import). */
    private void rebindGrid() {
        adapter = new GridAdapter(this, grid);
        adapter.setListener(this);

        GridLayoutManager layoutManager = new GridLayoutManager(
            this, adapter.getVisualColCount());
        recyclerView.setLayoutManager(layoutManager);
        recyclerView.setAdapter(adapter);
        hasUnsavedChanges = false;
    }

    // ---- Toolbar buttons ----

    private void setupToolbar() {
        Button addRowBtn = findViewById(R.id.btn_add_row);
        Button saveBtn = findViewById(R.id.btn_save);
        Button loadBtn = findViewById(R.id.btn_load);
        Button importBtn = findViewById(R.id.btn_import);
        Button exportBtn = findViewById(R.id.btn_export);

        addRowBtn.setOnClickListener(v -> {
            grid.addRow();
            adapter.notifyDataSetChanged();
            hasUnsavedChanges = true;
            // Scroll to bottom
            recyclerView.scrollToPosition(adapter.getItemCount() - 1);
        });

        saveBtn.setOnClickListener(v -> showSaveDialog());
        loadBtn.setOnClickListener(v -> showLoadDialog());
        importBtn.setOnClickListener(v -> launchImport());
        exportBtn.setOnClickListener(v -> launchExport());
    }

    // ---- GridActionListener ----

    @Override
    public void onInsertRow(int rowIndex) {
        grid.insertRow(rowIndex);
        adapter.notifyDataSetChanged();
        hasUnsavedChanges = true;
    }

    @Override
    public void onDeleteRow(int rowIndex) {
        if (grid.getRowCount() <= 1) {
            Toast.makeText(this, "Cannot delete the last row", Toast.LENGTH_SHORT).show();
            return;
        }
        GridModel.Row row = grid.getRow(rowIndex);
        String keyStr = row.getKey() != null ? String.valueOf(row.getKey()) : "(none)";
        String typesStr = row.getTypeSummary();
        if (typesStr.isEmpty()) typesStr = "(empty)";

        new AlertDialog.Builder(this)
            .setTitle("Delete Row " + (rowIndex + 1) + "?")
            .setMessage("Key: " + keyStr + "\nTypes: " + typesStr + "\n\nThis cannot be undone.")
            .setPositiveButton("Delete", (d, w) -> {
                grid.removeRow(rowIndex);
                adapter.notifyDataSetChanged();
                hasUnsavedChanges = true;
            })
            .setNegativeButton("Cancel", null)
            .show();
    }

    @Override
    public void onDataChanged() {
        hasUnsavedChanges = true;
    }

    // ---- Save dialog ----

    private void showSaveDialog() {
        EditText input = new EditText(this);
        input.setInputType(InputType.TYPE_CLASS_TEXT);
        input.setHint("Configuration name");

        new AlertDialog.Builder(this)
            .setTitle("Save Configuration")
            .setView(input)
            .setPositiveButton("Save", (d, w) -> {
                String name = input.getText().toString().trim();
                if (name.isEmpty()) {
                    Toast.makeText(this, "Name cannot be empty", Toast.LENGTH_SHORT).show();
                    return;
                }
                if (fileManager.exists(name)) {
                    showOverwriteDialog(name);
                } else {
                    doSave(name);
                }
            })
            .setNegativeButton("Cancel", null)
            .show();
    }

    private void showOverwriteDialog(String name) {
        new AlertDialog.Builder(this)
            .setTitle("Overwrite?")
            .setMessage("'" + name + "' already exists. Overwrite it?")
            .setPositiveButton("Overwrite", (d, w) -> doSave(name))
            .setNegativeButton("Cancel", null)
            .show();
    }

    private void doSave(String name) {
        try {
            fileManager.save(name, grid);
            hasUnsavedChanges = false;
            Toast.makeText(this, "Saved: " + name, Toast.LENGTH_SHORT).show();
        } catch (Exception e) {
            Toast.makeText(this, "Save failed: " + e.getMessage(), Toast.LENGTH_LONG).show();
        }
    }

    // ---- Load dialog ----

    private void showLoadDialog() {
        List<String> names = fileManager.listConfigs();
        if (names.isEmpty()) {
            Toast.makeText(this, "No saved configurations", Toast.LENGTH_SHORT).show();
            return;
        }

        // Add "Delete..." option at the end
        String[] items = new String[names.size() + 1];
        for (int i = 0; i < names.size(); i++) {
            items[i] = names.get(i);
        }
        items[items.length - 1] = "Delete...";

        new AlertDialog.Builder(this)
            .setTitle("Load Configuration")
            .setItems(items, (d, which) -> {
                if (which == items.length - 1) {
                    showDeleteConfigDialog();
                } else {
                    String name = items[which];
                    if (hasUnsavedChanges) {
                        showDiscardDialog(() -> doLoad(name));
                    } else {
                        doLoad(name);
                    }
                }
            })
            .setNegativeButton("Cancel", null)
            .show();
    }

    private void doLoad(String name) {
        try {
            grid = fileManager.load(name);
            rebindGrid();
            Toast.makeText(this, "Loaded: " + name, Toast.LENGTH_SHORT).show();
        } catch (Exception e) {
            Toast.makeText(this, "Load failed: " + e.getMessage(), Toast.LENGTH_LONG).show();
        }
    }

    // ---- Delete saved config dialog ----

    private void showDeleteConfigDialog() {
        List<String> names = fileManager.listConfigs();
        if (names.isEmpty()) {
            Toast.makeText(this, "No saved configurations", Toast.LENGTH_SHORT).show();
            return;
        }

        String[] items = names.toArray(new String[0]);

        new AlertDialog.Builder(this)
            .setTitle("Delete Configuration")
            .setItems(items, (d, which) -> {
                String name = items[which];
                new AlertDialog.Builder(this)
                    .setTitle("Delete '" + name + "'?")
                    .setMessage("This cannot be undone.")
                    .setPositiveButton("Delete", (d2, w2) -> {
                        if (fileManager.delete(name)) {
                            Toast.makeText(this, "Deleted: " + name, Toast.LENGTH_SHORT).show();
                        }
                    })
                    .setNegativeButton("Cancel", null)
                    .show();
            })
            .setNegativeButton("Cancel", null)
            .show();
    }

    // ---- Discard unsaved changes dialog ----

    private void showDiscardDialog(Runnable onDiscard) {
        new AlertDialog.Builder(this)
            .setTitle("Unsaved Changes")
            .setMessage("You have unsaved changes. Discard them?")
            .setPositiveButton("Discard", (d, w) -> onDiscard.run())
            .setNegativeButton("Cancel", null)
            .show();
    }

    // ---- SAF import/export ----

    private void setupSafLaunchers() {
        importLauncher = registerForActivityResult(
            new ActivityResultContracts.OpenDocument(),
            uri -> {
                if (uri == null) return;
                if (hasUnsavedChanges) {
                    showDiscardDialog(() -> doImport(uri));
                } else {
                    doImport(uri);
                }
            });

        exportLauncher = registerForActivityResult(
            new ActivityResultContracts.CreateDocument("application/json"),
            uri -> {
                if (uri == null || pendingExportModel == null) return;
                doExport(uri);
            });
    }

    private void launchImport() {
        importLauncher.launch(new String[]{"application/json"});
    }

    private void launchExport() {
        pendingExportModel = grid;
        exportLauncher.launch("message_format.json");
    }

    private void doImport(Uri uri) {
        try {
            grid = fileManager.importFromUri(uri);
            rebindGrid();
            Toast.makeText(this, "Imported successfully", Toast.LENGTH_SHORT).show();
        } catch (Exception e) {
            Toast.makeText(this, "Import failed: " + e.getMessage(), Toast.LENGTH_LONG).show();
        }
    }

    private void doExport(Uri uri) {
        try {
            fileManager.exportToUri(uri, pendingExportModel);
            Toast.makeText(this, "Exported successfully", Toast.LENGTH_SHORT).show();
        } catch (Exception e) {
            Toast.makeText(this, "Export failed: " + e.getMessage(), Toast.LENGTH_LONG).show();
        } finally {
            pendingExportModel = null;
        }
    }

    // ---- Intent handling (email attachments, etc.) ----

    private void handleIncomingIntent(Intent intent) {
        if (intent == null) return;
        String action = intent.getAction();
        Uri uri = intent.getData();

        if (Intent.ACTION_VIEW.equals(action) && uri != null) {
            doImport(uri);
        } else if (Intent.ACTION_SEND.equals(action)) {
            Uri sharedUri = intent.getParcelableExtra(Intent.EXTRA_STREAM);
            if (sharedUri != null) {
                doImport(sharedUri);
            }
        }
    }
}
