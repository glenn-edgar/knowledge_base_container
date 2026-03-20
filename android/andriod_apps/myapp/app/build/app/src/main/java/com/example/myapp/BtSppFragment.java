package com.example.myapp;

import android.annotation.SuppressLint;
import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothManager;
import android.bluetooth.BluetoothSocket;
import android.content.Context;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.view.inputmethod.EditorInfo;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ScrollView;
import android.widget.Spinner;
import android.widget.TextView;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@SuppressLint("MissingPermission")
public class BtSppFragment extends Fragment {

    // Standard SPP UUID
    private static final UUID SPP_UUID = UUID.fromString("00001101-0000-1000-8000-00805F9B34FB");

    private TextView tvOutput;
    private EditText etInput;
    private ScrollView scrollView;
    private Spinner spinnerDevices;
    private Button btnConnect;
    private final Handler mainHandler = new Handler(Looper.getMainLooper());

    private BluetoothAdapter btAdapter;
    private BluetoothSocket socket;
    private OutputStream outStream;
    private Thread readThread;
    private volatile boolean connected = false;
    private volatile boolean running = false;

    private final List<BluetoothDevice> deviceList = new ArrayList<>();
    private final List<String> deviceNames = new ArrayList<>();
    private ArrayAdapter<String> deviceAdapter;

    private final StringBuilder outputBuffer = new StringBuilder();
    private static final int MAX_OUTPUT_CHARS = 50000;

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container,
                             @Nullable Bundle savedInstanceState) {
        return inflater.inflate(R.layout.fragment_bt_spp, container, false);
    }

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);

        tvOutput = view.findViewById(R.id.tvOutput);
        etInput = view.findViewById(R.id.etInput);
        scrollView = view.findViewById(R.id.scrollView);
        spinnerDevices = view.findViewById(R.id.spinnerDevices);
        btnConnect = view.findViewById(R.id.btnConnect);
        Button btnRefresh = view.findViewById(R.id.btnRefresh);
        Button btnSend = view.findViewById(R.id.btnSend);
        Button btnClear = view.findViewById(R.id.btnClear);

        BluetoothManager btManager = (BluetoothManager) requireContext()
                .getSystemService(Context.BLUETOOTH_SERVICE);
        btAdapter = btManager.getAdapter();

        deviceAdapter = new ArrayAdapter<>(requireContext(),
                android.R.layout.simple_spinner_item, deviceNames);
        deviceAdapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        spinnerDevices.setAdapter(deviceAdapter);

        btnRefresh.setOnClickListener(v -> loadPairedDevices());
        btnConnect.setOnClickListener(v -> toggleConnection());
        btnSend.setOnClickListener(v -> sendData());
        btnClear.setOnClickListener(v -> {
            outputBuffer.setLength(0);
            tvOutput.setText("");
        });

        etInput.setOnEditorActionListener((v, actionId, event) -> {
            if (actionId == EditorInfo.IME_ACTION_SEND) {
                sendData();
                return true;
            }
            return false;
        });

        appendOutput(">>> Bluetooth SPP Terminal ready\n");

        if (btAdapter == null || !btAdapter.isEnabled()) {
            appendOutput(">>> Bluetooth not available or disabled\n");
        } else {
            loadPairedDevices();
        }
    }

    private void loadPairedDevices() {
        deviceList.clear();
        deviceNames.clear();

        if (btAdapter == null || !btAdapter.isEnabled()) {
            deviceNames.add("(Bluetooth disabled)");
            deviceAdapter.notifyDataSetChanged();
            return;
        }

        Set<BluetoothDevice> paired = btAdapter.getBondedDevices();
        if (paired == null || paired.isEmpty()) {
            deviceNames.add("(no paired devices)");
            appendOutput(">>> No paired devices. Pair via Android Settings first.\n");
        } else {
            for (BluetoothDevice device : paired) {
                deviceList.add(device);
                String name = device.getName() != null ? device.getName() : "Unknown";
                deviceNames.add(name + " [" + device.getAddress() + "]");
            }
            appendOutput(">>> " + paired.size() + " paired device(s)\n");
        }
        deviceAdapter.notifyDataSetChanged();
    }

    private void toggleConnection() {
        if (connected) {
            disconnect();
        } else {
            int pos = spinnerDevices.getSelectedItemPosition();
            if (pos < 0 || pos >= deviceList.size()) {
                appendOutput(">>> No device selected\n");
                return;
            }
            connectToDevice(deviceList.get(pos));
        }
    }

    private void connectToDevice(BluetoothDevice device) {
        appendOutput(">>> Connecting to " + device.getName() + "...\n");
        btnConnect.setEnabled(false);

        new Thread(() -> {
            try {
                // Cancel discovery to speed up connection
                btAdapter.cancelDiscovery();

                socket = device.createRfcommSocketToServiceRecord(SPP_UUID);
                socket.connect();

                outStream = socket.getOutputStream();
                InputStream inStream = socket.getInputStream();

                connected = true;
                running = true;

                mainHandler.post(() -> {
                    btnConnect.setText("Disconnect");
                    btnConnect.setEnabled(true);
                    appendOutput(">>> Connected via SPP\n");
                });

                // Read loop
                readThread = Thread.currentThread();
                byte[] buffer = new byte[1024];
                int bytes;
                while (running) {
                    try {
                        bytes = inStream.read(buffer);
                        if (bytes > 0) {
                            String text = new String(buffer, 0, bytes, StandardCharsets.UTF_8);
                            mainHandler.post(() -> appendOutput(text));
                        }
                    } catch (IOException e) {
                        if (running) {
                            mainHandler.post(() -> {
                                appendOutput(">>> Connection lost: " + e.getMessage() + "\n");
                                disconnect();
                            });
                        }
                        break;
                    }
                }
            } catch (IOException e) {
                mainHandler.post(() -> {
                    appendOutput(">>> Connection failed: " + e.getMessage() + "\n");
                    appendOutput(">>> Make sure the remote device is in SPP/serial mode\n");
                    btnConnect.setEnabled(true);
                });
                closeSocket();
            }
        }).start();
    }

    private void disconnect() {
        running = false;
        connected = false;
        closeSocket();
        btnConnect.setText("Connect");
        btnConnect.setEnabled(true);
        appendOutput(">>> Disconnected\n");
    }

    private void closeSocket() {
        outStream = null;
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {}
            socket = null;
        }
    }

    private void sendData() {
        if (!connected || outStream == null) {
            appendOutput(">>> Not connected\n");
            return;
        }
        String text = etInput.getText().toString();
        if (text.isEmpty()) return;

        new Thread(() -> {
            try {
                outStream.write((text + "\n").getBytes(StandardCharsets.UTF_8));
                outStream.flush();
                mainHandler.post(() -> {
                    appendOutput("TX> " + text + "\n");
                    etInput.setText("");
                });
            } catch (IOException e) {
                mainHandler.post(() -> appendOutput(">>> Send failed: " + e.getMessage() + "\n"));
            }
        }).start();
    }

    private void appendOutput(String text) {
        outputBuffer.append(text);
        if (outputBuffer.length() > MAX_OUTPUT_CHARS) {
            outputBuffer.delete(0, outputBuffer.length() - MAX_OUTPUT_CHARS);
        }
        tvOutput.setText(outputBuffer.toString());
        scrollView.post(() -> scrollView.fullScroll(View.FOCUS_DOWN));
    }

    @Override
    public void onDestroyView() {
        running = false;
        disconnect();
        super.onDestroyView();
    }
}
