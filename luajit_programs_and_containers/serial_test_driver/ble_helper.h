/* ble_helper.h
 * Minimal Windows BLE GATT write helper for LuaJIT FFI.
 *
 * Build (MSVC):
 *   cl /LD /O2 ble_helper.c /Fe:ble_helper.dll \
 *      ole32.lib windowsapp.lib
 *
 * Build (MinGW / llvm-mingw on ARM64):
 *   aarch64-w64-mingw32-gcc -shared -O2 ble_helper.c -o ble_helper.dll \
 *      -lole32 -lwindowsapp
 *
 * Approach: wraps the Windows.Devices.Bluetooth C WinRT projection.
 * Because the full WinRT C projection is heavy, this DLL provides
 * a thin synchronous interface:
 *
 *   ble_open(device_address)      -> handle
 *   ble_write(handle, svc, chr, data, len) -> 0 ok / -1 err
 *   ble_read(handle, svc, chr, buf, buf_len) -> bytes_read / -1
 *   ble_close(handle)
 *
 * ALTERNATIVE: If the WinRT C projection is too painful to build,
 * this can be implemented as a thin Python or C# bridge that the
 * DLL shells out to, or replaced with a named-pipe protocol to a
 * small .NET 6 BLE helper process.  See ble_bridge.lua for the
 * named-pipe fallback approach.
 */

 #ifndef BLE_HELPER_H
 #define BLE_HELPER_H
 
 #include <stdint.h>
 
 #ifdef BLE_HELPER_EXPORTS
   #define BLE_API __declspec(dllexport)
 #else
   #define BLE_API __declspec(dllimport)
 #endif
 
 #ifdef __cplusplus
 extern "C" {
 #endif
 
 /* Opaque handle to a BLE connection */
 typedef struct ble_conn* ble_handle_t;
 
 /* Status codes */
 #define BLE_OK           0
 #define BLE_ERR_NOTFOUND -1
 #define BLE_ERR_CONNECT  -2
 #define BLE_ERR_WRITE    -3
 #define BLE_ERR_READ     -4
 #define BLE_ERR_PARAM    -5
 #define BLE_ERR_BUSY     -6
 
 /**
  * Open a BLE connection to a device by its Bluetooth address.
  * @param address_u64  48-bit BT address as uint64 (e.g. 0xAABBCCDDEEFF)
  * @param out_handle   receives the connection handle
  * @return BLE_OK or error code
  */
 BLE_API int ble_open(uint64_t address_u64, ble_handle_t* out_handle);
 
 /**
  * Open a BLE connection by device name (first match).
  * Performs a short scan (up to timeout_ms) looking for the name.
  * @param device_name   UTF-8 device name to match
  * @param timeout_ms    scan timeout in milliseconds
  * @param out_handle    receives the connection handle
  * @return BLE_OK or error code
  */
 BLE_API int ble_open_by_name(const char* device_name,
                               uint32_t timeout_ms,
                               ble_handle_t* out_handle);
 
 /**
  * Write to a GATT characteristic.
  * @param h            connection handle
  * @param service_uuid UUID string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  * @param char_uuid    characteristic UUID string
  * @param data         bytes to write
  * @param data_len     number of bytes
  * @param write_type   0 = WriteWithResponse, 1 = WriteWithoutResponse
  * @return BLE_OK or error code
  */
 BLE_API int ble_write(ble_handle_t h,
                        const char* service_uuid,
                        const char* char_uuid,
                        const uint8_t* data,
                        uint32_t data_len,
                        int write_type);
 
 /**
  * Read from a GATT characteristic.
  * @param h            connection handle
  * @param service_uuid UUID string
  * @param char_uuid    characteristic UUID string
  * @param buf          output buffer
  * @param buf_len      buffer capacity
  * @return bytes read (>=0) or error code (<0)
  */
 BLE_API int ble_read(ble_handle_t h,
                       const char* service_uuid,
                       const char* char_uuid,
                       uint8_t* buf,
                       uint32_t buf_len);
 
 /**
  * Close a BLE connection and free resources.
  */
 BLE_API void ble_close(ble_handle_t h);
 
 /**
  * Get a human-readable error string for the last operation.
  */
 BLE_API const char* ble_last_error(void);
 
 #ifdef __cplusplus
 }
 #endif
 
 #endif /* BLE_HELPER_H */