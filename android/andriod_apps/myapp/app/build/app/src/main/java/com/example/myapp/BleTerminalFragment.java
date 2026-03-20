package com.example.myapp;

import android.annotation.SuppressLint;
import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothGatt;
import android.bluetooth.BluetoothGattCallback;
import android.bluetooth.BluetoothGattCharacteristic;
import android.bluetooth.BluetoothGattDescriptor;
import android.bluetooth.BluetoothGattService;
import android.bluetooth.BluetoothManager;
import android.bluetooth.le.BluetoothLeScanner;
import android.bluetooth.le.ScanCallback;
import android.bluetooth.le.ScanResult;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@SuppressLint("MissingPermission")
public class BleTerminalFragment extends Fragment {

    // Nordic UART Service UUIDs
    private static final UUID NUS_SERVICE = UUID.fromString("6e400001-b5a3-f393-e0a9-e50e24dcca9e");
    private static final UUID NUS_TX_CHAR = UUID.fromString("6e400002-b5a3-f393-e0a9-e50e24dcca9e"); // write to this
    private static final UUID NUS_RX_CHAR = UUID.fromString("6e400003-b5a3-f393-e0a9-e50e24dcca9e"); // notifications from this
    private static final UUID CCCD = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb");

    private TextView tvOutput, tvCharInfo;
    private EditText etInput;
    private ScrollView scrollView;
    private Spinner spinnerDevices;
    private Button btnConnect, btnScan;
    private final Handler mainHandler = new Handler(Looper.getMainLooper());

    private BluetoothAdapter btAdapter;
    private BluetoothLeScanner scanner;
    private BluetoothGatt gatt;
    private BluetoothGattCharacteristic txCharacteristic; // we write to this
    private BluetoothGattCharacteristic rxCharacteristic; // we get notifications from this
    private boolean connected = false;
    private boolean scanning = false;

    private final List<BluetoothDevice> deviceList = new ArrayList<>();
    private final List<String> deviceNames = new ArrayList<>();
    private ArrayAdapter<String> deviceAdapter;

    private final StringBuilder outputBuffer = new StringBuilder();
    private static final int MAX_OUTPUT_CHARS = 50000;

    private final ScanCallback scanCallback = new ScanCallback() {
        @Override
        public void onScanResult(int callbackType, ScanResult result) {
            BluetoothDevice device = result.getDevice();
            // Avoid duplicates
            for (BluetoothDevice d : deviceList) {
                if (d.getAddress().equals(device.getAddress())) return;
            }
            deviceList.add(device);
            String name = device.getName() != null ? device.getName() : "Unknown";
            int rssi = result.getRssi();
            deviceNames.add(String.format("%s [%s] %ddBm", name, device.getAddress(), rssi));
            mainHandler.post(() -> deviceAdapter.notifyDataSetChanged());
        }
    };

    private final BluetoothGattCallback gattCallback = new BluetoothGattCallback() {
        @Override
        public void onConnectionStateChange(BluetoothGatt g, int status, int newState) {
            if (newState == BluetoothGatt.STATE_CONNECTED) {
                mainHandler.post(() -> appendOutput(">>> GATT connected, discovering services...\n"));
                g.discoverServices();
            } else if (newState == BluetoothGatt.STATE_DISCONNECTED) {
                connected = false;
                mainHandler.post(() -> {
                    btnConnect.setText("Connect");
                    tvCharInfo.setText("Disconnected");
                    appendOutput(">>> GATT disconnected\n");
                });
            }
        }

        @Override
        public void onServicesDiscovered(BluetoothGatt g, int status) {
            if (status != BluetoothGatt.GATT_SUCCESS) {
                mainHandler.post(() -> appendOutput(">>> Service discovery failed\n"));
                return;
            }

            txCharacteristic = null;
            rxCharacteristic = null;

            // First try Nordic UART Service
            BluetoothGattService nus = g.getService(NUS_SERVICE);
            if (nus != null) {
                txCharacteristic = nus.getCharacteristic(NUS_TX_CHAR);
                rxCharacteristic = nus.getCharacteristic(NUS_RX_CHAR);
                mainHandler.post(() -> {
                    tvCharInfo.setText("Nordic UART Service found");
                    appendOutput(">>> Nordic UART Service detected\n");
                });
            }

            // Fallback: find any writable + notifiable characteristics
            if (txCharacteristic == null) {
                for (BluetoothGattService svc : g.getServices()) {
                    for (BluetoothGattCharacteristic c : svc.getCharacteristics()) {
                        int props = c.getProperties();
                        if (txCharacteristic == null &&
                                (props & BluetoothGattCharacteristic.PROPERTY_WRITE) != 0) {
                            txCharacteristic = c;
                        }
                        if (rxCharacteristic == null &&
                                (props & BluetoothGattCharacteristic.PROPERTY_NOTIFY) != 0) {
                            rxCharacteristic = c;
                        }
                    }
                }
                final String info = "TX: " +
                        (txCharacteristic != null ? txCharacteristic.getUuid().toString().substring(0, 8) : "none") +
                        " RX: " +
                        (rxCharacteristic != null ? rxCharacteristic.getUuid().toString().substring(0, 8) : "none");
                mainHandler.post(() -> tvCharInfo.setText(info));
            }

            // Enable notifications on RX characteristic
            if (rxCharacteristic != null) {
                g.setCharacteristicNotification(rxCharacteristic, true);
                BluetoothGattDescriptor desc = rxCharacteristic.getDescriptor(CCCD);
                if (desc != null) {
                    desc.setValue(BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE);
                    g.writeDescriptor(desc);
                }
            }

            connected = true;
            mainHandler.post(() -> {
                btnConnect.setText("Disconnect");
                appendOutput(">>> Ready - " + g.getServices().size() + " services found\n");
            });
        }

        @Override
        public void onCharacteristicChanged(BluetoothGatt g, BluetoothGattCharacteristic c) {
            byte[] data = c.getValue();
            if (data != null) {
                String text = new String(data, StandardCharsets.UTF_8);
                mainHandler.post(() -> appendOutput(text));
            }
        }

        @Override
        public void onCharacteristicWrite(BluetoothGatt g, BluetoothGattCharacteristic c, int status) {
            if (status != BluetoothGatt.GATT_SUCCESS) {
                mainHandler.post(() -> appendOutput(">>> Write failed (status " + status + ")\n"));
            }
        }
    };

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container,
                             @Nullable Bundle savedInstanceState) {
        return inflater.inflate(R.layout.fragment_ble_terminal, container, false);
    }

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);

        tvOutput = view.findViewById(R.id.tvOutput);
        tvCharInfo = view.findViewById(R.id.tvCharInfo);
        etInput = view.findViewById(R.id.etInput);
        scrollView = view.findViewById(R.id.scrollView);
        spinnerDevices = view.findViewById(R.id.spinnerDevices);
        btnConnect = view.findViewById(R.id.btnConnect);
        btnScan = view.findViewById(R.id.btnScan);
        Button btnSend = view.findViewById(R.id.btnSend);
        Button btnClear = view.findViewById(R.id.btnClear);

        BluetoothManager btManager = (BluetoothManager) requireContext()
                .getSystemService(Context.BLUETOOTH_SERVICE);
        btAdapter = btManager.getAdapter();

        deviceAdapter = new ArrayAdapter<>(requireContext(),
                android.R.layout.simple_spinner_item, deviceNames);
        deviceAdapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        spinnerDevices.setAdapter(deviceAdapter);

        btnScan.setOnClickListener(v -> toggleScan());
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

        appendOutput(">>> BLE Terminal ready\n");

        if (btAdapter == null || !btAdapter.isEnabled()) {
            appendOutput(">>> Bluetooth not available or disabled\n");
        }
    }

    private void toggleScan() {
        if (scanning) {
            stopScan();
        } else {
            startScan();
        }
    }

    private void startScan() {
        if (btAdapter == null || !btAdapter.isEnabled()) {
            appendOutput(">>> Enable Bluetooth first\n");
            return;
        }

        deviceList.clear();
        deviceNames.clear();
        deviceAdapter.notifyDataSetChanged();

        scanner = btAdapter.getBluetoothLeScanner();
        if (scanner == null) {
            appendOutput(">>> BLE scanner not available\n");
            return;
        }

        scanning = true;
        btnScan.setText("Stop");
        appendOutput(">>> Scanning...\n");
        scanner.startScan(scanCallback);

        // Auto-stop after 10 seconds
        mainHandler.postDelayed(() -> {
            if (scanning) stopScan();
        }, 10000);
    }

    private void stopScan() {
        if (scanner != null && scanning) {
            scanner.stopScan(scanCallback);
        }
        scanning = false;
        btnScan.setText("Scan");
        appendOutput(">>> Scan stopped, " + deviceList.size() + " device(s) found\n");
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
            if (scanning) stopScan();

            BluetoothDevice device = deviceList.get(pos);
            appendOutput(">>> Connecting to " + device.getAddress() + "...\n");
            gatt = device.connectGatt(requireContext(), false, gattCallback,
                    BluetoothDevice.TRANSPORT_LE);
        }
    }

    private void disconnect() {
        if (gatt != null) {
            gatt.disconnect();
            gatt.close();
            gatt = null;
        }
        connected = false;
        txCharacteristic = null;
        rxCharacteristic = null;
        btnConnect.setText("Connect");
        tvCharInfo.setText("Disconnected");
        appendOutput(">>> Disconnected\n");
    }

    private void sendData() {
        if (!connected || txCharacteristic == null) {
            appendOutput(">>> Not connected or no writable characteristic\n");
            return;
        }
        String text = etInput.getText().toString();
        if (text.isEmpty()) return;

        byte[] data = (text + "\n").getBytes(StandardCharsets.UTF_8);

        // BLE has a 20-byte MTU by default, chunk if needed
        int chunkSize = 20;
        for (int i = 0; i < data.length; i += chunkSize) {
            int end = Math.min(i + chunkSize, data.length);
            byte[] chunk = new byte[end - i];
            System.arraycopy(data, i, chunk, 0, chunk.length);
            txCharacteristic.setValue(chunk);
            txCharacteristic.setWriteType(BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT);
            gatt.writeCharacteristic(txCharacteristic);
        }

        appendOutput("TX> " + text + "\n");
        etInput.setText("");
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
        if (scanning) stopScan();
        disconnect();
        super.onDestroyView();
    }
}
