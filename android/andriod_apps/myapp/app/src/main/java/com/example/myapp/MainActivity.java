package com.example.myapp;

import android.content.Intent;
import android.os.Bundle;
import android.widget.Button;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;

/**
 * Main activity — app entry point with navigation to other activities.
 */
public class MainActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        Button gridEditorBtn = findViewById(R.id.btn_open_grid_editor);
        gridEditorBtn.setOnClickListener(v -> {
            Intent intent = new Intent(this, GridActivity.class);
            startActivity(intent);
        });
    }
}
