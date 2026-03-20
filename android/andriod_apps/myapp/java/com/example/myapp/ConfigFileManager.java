package com.example.myapp;

import android.content.Context;
import android.net.Uri;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * File I/O for grid configurations.
 * 
 * Named configs are stored as JSON files under:
 *   <app_internal>/grid_configs/<sanitized_name>.json
 * 
 * Also handles SAF-based import from URI and export to URI.
 */
public class ConfigFileManager {

    private static final String CONFIG_DIR = "grid_configs";
    private static final String EXTENSION = ".json";

    private final Context context;
    private final File configDir;

    public ConfigFileManager(Context context) {
        this.context = context;
        this.configDir = new File(context.getFilesDir(), CONFIG_DIR);
        if (!configDir.exists()) {
            configDir.mkdirs();
        }
    }

    // --- Internal storage operations ---

    /** Returns sorted list of saved config names (without extension). */
    public List<String> listConfigs() {
        List<String> names = new ArrayList<>();
        File[] files = configDir.listFiles();
        if (files != null) {
            for (File f : files) {
                String name = f.getName();
                if (name.endsWith(EXTENSION)) {
                    names.add(name.substring(0, name.length() - EXTENSION.length()));
                }
            }
        }
        Collections.sort(names, String.CASE_INSENSITIVE_ORDER);
        return names;
    }

    /** Checks if a config with the given name exists. */
    public boolean exists(String name) {
        return getFile(name).exists();
    }

    /** Saves the grid model as a named config. */
    public void save(String name, GridModel model) throws Exception {
        String json = model.toJson().toString(2);
        File file = getFile(name);
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(json);
        }
    }

    /** Loads a named config. Returns a new GridModel. */
    public GridModel load(String name) throws Exception {
        File file = getFile(name);
        String json = readFileToString(file);
        return GridModel.fromJson(json);
    }

    /** Deletes a named config. Returns true if deleted. */
    public boolean delete(String name) {
        File file = getFile(name);
        return file.exists() && file.delete();
    }

    // --- SAF import/export ---

    /** Imports a grid config from a content URI (SAF or intent). */
    public GridModel importFromUri(Uri uri) throws Exception {
        String json = readUriToString(uri);
        return GridModel.fromJson(json);
    }

    /** Exports a grid model to a content URI (SAF). */
    public void exportToUri(Uri uri, GridModel model) throws Exception {
        String json = model.toJson().toString(2);
        try (OutputStream os = context.getContentResolver().openOutputStream(uri)) {
            if (os == null) throw new IOException("Cannot open output stream for URI");
            os.write(json.getBytes("UTF-8"));
        }
    }

    // --- Helpers ---

    private File getFile(String name) {
        return new File(configDir, sanitize(name) + EXTENSION);
    }

    /** Sanitize a filename: keep alphanumeric, dash, underscore, space. */
    private String sanitize(String name) {
        if (name == null || name.trim().isEmpty()) {
            return "untitled";
        }
        return name.trim().replaceAll("[^a-zA-Z0-9_\\- ]", "_");
    }

    private String readFileToString(File file) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }

    private String readUriToString(Uri uri) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (InputStream is = context.getContentResolver().openInputStream(uri)) {
            if (is == null) throw new IOException("Cannot open input stream for URI");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append('\n');
                }
            }
        }
        return sb.toString();
    }
}
