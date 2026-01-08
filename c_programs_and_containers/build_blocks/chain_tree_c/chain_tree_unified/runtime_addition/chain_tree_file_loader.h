/* ChainTree File Loader Header
 * 
 * Loads ChainTree binary files from disk and initializes the runtime.
 */
 #ifndef CHAINTREE_FILE_LOADER_H
 #define CHAINTREE_FILE_LOADER_H
 
 #include "chaintree_binary_support.h"
 
 /* ===== File Load Result Codes ===== */
 typedef enum {
     CT_FILE_OK = 0,
     CT_FILE_ERR_NULL_PATH,
     CT_FILE_ERR_OPEN,
     CT_FILE_ERR_SEEK,
     CT_FILE_ERR_READ,
     CT_FILE_ERR_ALLOC,
     CT_FILE_ERR_EMPTY,
     CT_FILE_ERR_TOO_LARGE,
     /* Binary loader errors are passed through with offset */
     CT_FILE_ERR_BINARY_BASE = 100,
 } ct_file_result_t;
 
 /* ===== File Loader Handle ===== */
 typedef struct {
     uint8_t *binary_data;           /* Allocated buffer holding file contents */
     uint32_t binary_size;           /* Size of binary data */
     chaintree_runtime_t *runtime;   /* Runtime handle from loader */
     ct_allocator_t allocator;       /* Allocator used (stored for unload) */
 } ct_file_handle_t;
 
 /* ===== API Functions ===== */
 
 /*
  * Load binary file and create runtime.
  * 
  * @param filepath      Path to .bin file
  * @param allocator     Memory allocator (NULL to use malloc/free)
  * @param resolver      Function resolvers
  * @param handle_out    Output handle (caller must call ct_file_unload)
  * @return              CT_FILE_OK on success, error code otherwise
  * 
  * Usage:
  *   ct_file_handle_t handle;
  *   ct_file_result_t result = ct_file_load("tree.bin", NULL, &resolver, &handle);
  *   if (result == CT_FILE_OK) {
  *       // Use handle.runtime
  *       ct_file_unload(&handle);
  *   }
  */
 ct_file_result_t ct_file_load(
     const char *filepath,
     const ct_allocator_t *allocator,
     const ct_resolver_t *resolver,
     ct_file_handle_t *handle_out
 );
 
 /*
  * Load binary file without initializing runtime (verification only).
  * 
  * @param filepath      Path to .bin file
  * @param allocator     Memory allocator (NULL to use malloc/free)
  * @param handle_out    Output handle with binary_data and binary_size set
  * @return              CT_FILE_OK on success
  */
 ct_file_result_t ct_file_load_raw(
     const char *filepath,
     const ct_allocator_t *allocator,
     ct_file_handle_t *handle_out
 );
 
 /*
  * Unload file and free all resources.
  * 
  * @param handle    Handle from ct_file_load or ct_file_load_raw
  */
 void ct_file_unload(ct_file_handle_t *handle);
 
 /*
  * Get human-readable error string.
  * 
  * @param result    Result code from ct_file_load
  * @return          Static string describing the error
  */
 const char* ct_file_result_str(ct_file_result_t result);
 
 /*
  * Verify a binary file without loading it into memory permanently.
  * 
  * @param filepath  Path to .bin file
  * @return          CT_FILE_OK if valid, error code otherwise
  */
 ct_file_result_t ct_file_verify(const char *filepath);
 
 /*
  * Get the unique ID hash from a binary file.
  * 
  * @param filepath      Path to .bin file
  * @param hash_out      Output for unique ID hash
  * @return              CT_FILE_OK on success
  */
 ct_file_result_t ct_file_get_id(const char *filepath, uint32_t *hash_out);
 
 #endif /* CHAINTREE_FILE_LOADER_H */