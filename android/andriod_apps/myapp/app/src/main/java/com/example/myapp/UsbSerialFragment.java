package com.example.myapp;

import android.app.PendingIntent;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.hardware.usb.UsbDevice;
import android.hardware.usb.UsbDeviceConnection;
import android.hardware.usb.UsbManager;
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
import com.hoho.android.usbserial.driver.UsbSerialDriver;
import com.hoho.android.usbserial.driver.UsbSerialPort;
import com.hoho.android.usbserial.driver.UsbSerialProber;
import com.hoho.android.usbserial.util.SerialInputOutputManager;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class UsbSerialFragment extends Fragment implements SerialInputOutputManager.Listener {

    private static final String ACTION_USB_PERMISSION = "com.example.myapp.USB_PERMISSION";
    private static final int[] BAUD_RATES = {9600, 19200, 38400, 57600, 115200, 230400, 460800, 921600};

    private TextView tvOutput;
    private EditText etInput;
    private ScrollView scrollView;
    private Spinner spinnerDevices, spinnerBaud;
    private Button btnConnect;
    private final Handler mainHandler = new Handler(Looper.getMainLooper());

    private UsbManager usbManager;
    private UsbSerialPort serialPort;
    private SerialInputOutputManager ioManager;
    private boolean connected = false;

    private final List<UsbSerialDriver> driverList = new ArrayList<>();
    private final List<String> deviceNames = new ArrayList<>();
    private ArrayAdapter<String> deviceAdapter;

    private final StringBuilder outputBuffer = new StringBuilder();
    private static final int MAX_OUTPUT_CHARS = 50000;

    private final BroadcastReceiver usbPermissionReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            if (ACTION_USB_PERMISSION.equals(intent.getAction())) {
                synchronized (this) {
                    if (intent.getBooleanExtra(UsbManager.EXTRA_PERMISSION_GRANTED, false)) {
                        connectToSelectedDevice();
                    } else {
                        appendOutput(">>> USB permission denied\n");
                    }
                }
            }
        }
    };

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container,
                             @Nullable Bundle savedInstanceState) {
        return inflater.inflate(R.layout.fragment_usb_serial, container, false);
    }

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);

        tvOutput = view.findViewById(R.id.tvOutput);
        etInput = view.findViewById(R.id.etInput);
        scrollView = view.findViewById(R.id.scrollView);
        spinnerDevices = view.findViewById(R.id.spinnerDevices);
        spinnerBaud = view.findViewById(R.id.spinnerBaud);
        btnConnect = view.findViewById(R.id.btnConnect);
        Button btnRefresh = view.findViewById(R.id.btnRefresh);
        Button btnSend = view.findViewById(R.id.btnSend);
        Button btnClear = view.findViewById(R.id.btnClear);

        usbManager = (UsbManager) requireContext().getSystemService(Context.USB_SERVICE);

        // Device spinner
        deviceAdapter = new ArrayAdapter<>(requireContext(),
                android.R.layout.simple_spinner_item, deviceNames);
        deviceAdapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        spinnerDevices.setAdapter(deviceAdapter);

        // Baud rate spinner
        List<String> baudStrings = new ArrayList<>();
        for (int b : BAUD_RATES) baudStrings.add(String.valueOf(b));
        ArrayAdapter<String> baudAdapter = new ArrayAdapter<>(requireContext(),
                android.R.layout.simple_spinner_item, baudStrings);
        baudAdapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        spinnerBaud.setAdapter(baudAdapter);
        spinnerBaud.setSelection(4); // default 115200

        // Register USB permission receiver
        IntentFilter filter = new IntentFilter(ACTION_USB_PERMISSION);
        requireContext().registerReceiver(usbPermissionReceiver, filter,
                Context.RECEIVER_NOT_EXPORTED);

        // Button handlers
        btnRefresh.setOnClickListener(v -> scanDevices());
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

        appendOutput(">>> USB Serial Terminal ready\n");
        scanDevices();
    }

    private void scanDevices() {
        driverList.clear();
        deviceNames.clear();

        List<UsbSerialDriver> drivers = UsbSerialProber.getDefaultProber().findAllDrivers(usbManager);
        if (drivers.isEmpty()) {
            deviceNames.add("(no devices)");
            appendOutput(">>> No USB serial devices found\n");
        } else {
            for (UsbSerialDriver driver : drivers) {
                driverList.add(driver);
                UsbDevice dev = driver.getDevice();
                String name = dev.getProductName() != null ? dev.getProductName() : "Unknown";
                deviceNames.add(String.format("%s [%04X:%04X]",
                        name, dev.getVendorId(), dev.getProductId()));
            }
            appendOutput(">>> Found " + drivers.size() + " device(s)\n");
        }
        deviceAdapter.notifyDataSetChanged();
    }

    private void toggleConnection() {
        if (connected) {
            disconnect();
        } else {
            int pos = spinnerDevices.getSelectedItemPosition();
            if (pos < 0 || pos >= driverList.size()) {
                appendOutput(">>> No device selected\n");
                return;
            }
            UsbSerialDriver driver = driverList.get(pos);
            UsbDevice device = driver.getDevice();

            if (!usbManager.hasPermission(device)) {
                int flags = PendingIntent.FLAG_MUTABLE;
                PendingIntent pi = PendingIntent.getBroadcast(requireContext(), 0,
                        new Intent(ACTION_USB_PERMISSION), flags);
                usbManager.requestPermission(device, pi);
            } else {
                connectToSelectedDevice();
            }
        }
    }

    private void connectToSelectedDevice() {
        int pos = spinnerDevices.getSelectedItemPosition();
        if (pos < 0 || pos >= driverList.size()) return;

        UsbSerialDriver driver = driverList.get(pos);
        UsbDeviceConnection connection = usbManager.openDevice(driver.getDevice());
        if (connection == null) {
            appendOutput(">>> Failed to open device\n");
            return;
        }

        serialPort = driver.getPorts().get(0);
        int baudRate = BAUD_RATES[spinnerBaud.getSelectedItemPosition()];

        try {
            serialPort.open(connection);
            serialPort.setParameters(baudRate, 8, UsbSerialPort.STOPBITS_1, UsbSerialPort.PARITY_NONE);
            serialPort.setDTR(true);
            serialPort.setRTS(true);

            ioManager = new SerialInputOutputManager(serialPort, this);
            ioManager.start();

            connected = true;
            btnConnect.setText("Disconnect");
            appendOutput(">>> Connected at " + baudRate + " baud\n");
        } catch (IOException e) {
            appendOutput(">>> Connect failed: " + e.getMessage() + "\n");
            disconnect();
        }
    }

    private void disconnect() {
        if (ioManager != null) {
            ioManager.setListener(null);
            ioManager.stop();
            ioManager = null;
        }
        if (serialPort != null) {
            try {
                serialPort.close();
            } catch (IOException ignored) {}
            serialPort = null;
        }
        connected = false;
        mainHandler.post(() -> {
            btnConnect.setText("Connect");
            appendOutput(">>> Disconnected\n");
        });
    }

    private void sendData() {
        if (!connected || serialPort == null) {
            appendOutput(">>> Not connected\n");
            return;
        }
        String text = etInput.getText().toString();
        if (text.isEmpty()) return;

        try {
            byte[] data = (text + "\n").getBytes(StandardCharsets.UTF_8);
            serialPort.write(data, 1000);
            appendOutput("TX> " + text + "\n");
            etInput.setText("");
        } catch (IOException e) {
            appendOutput(">>> Send failed: " + e.getMessage() + "\n");
        }
    }

    // SerialInputOutputManager.Listener callbacks
    @Override
    public void onNewData(byte[] data) {
        String text = new String(data, StandardCharsets.UTF_8);
        mainHandler.post(() -> appendOutput(text));
    }

    @Override
    public void onRunError(Exception e) {
        mainHandler.post(() -> {
            appendOutput(">>> Error: " + e.getMessage() + "\n");
            disconnect();
        });
    }

    private void appendOutput(String text) {
        outputBuffer.append(text);
        // Trim if too long
        if (outputBuffer.length() > MAX_OUTPUT_CHARS) {
            outputBuffer.delete(0, outputBuffer.length() - MAX_OUTPUT_CHARS);
        }
        tvOutput.setText(outputBuffer.toString());
        scrollView.post(() -> scrollView.fullScroll(View.FOCUS_DOWN));
    }

    @Override
    public void onDestroyView() {
        disconnect();
        try {
            requireContext().unregisterReceiver(usbPermissionReceiver);
        } catch (Exception ignored) {}
        super.onDestroyView();
    }
}
