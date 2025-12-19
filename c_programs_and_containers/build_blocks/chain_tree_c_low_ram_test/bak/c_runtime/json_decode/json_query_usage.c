/* example_query.c */
#include "json_decoder.h"
#include "json_query.h"
#include "json_records.h"

int main(void)
{
    json_decoder_t decoder;
    json_query_result_t result;

    // Initialize decoder
    json_decoder_init(&decoder,
                     json_records,
                     JSON_RECORDS_COUNT,
                     string_table,
                     STRING_TABLE_SIZE,
                     record_controls,
                     RECORD_CONTROLS_COUNT);

    // Example queries on object 0: {"temperature": 23.5, "sensors": [1, 2, 3]}
    
    printf("Querying object 0:\n");
    
    if (json_query_path(&decoder, 0, "temperature", &result) == 0) {
        printf("  temperature = ");
        json_print_query_result(&result);
        printf("\n");
    }
    
    if (json_query_path(&decoder, 0, "sensors[0]", &result) == 0) {
        printf("  sensors[0] = ");
        json_print_query_result(&result);
        printf("\n");
    }
    
    if (json_query_path(&decoder, 0, "sensors[2]", &result) == 0) {
        printf("  sensors[2] = ");
        json_print_query_result(&result);
        printf("\n");
    }

    // Example queries on object 1: {"config": {"timeout": 30, "retry": true}}
    
    printf("\nQuerying object 1:\n");
    
    if (json_query_path(&decoder, 1, "config.timeout", &result) == 0) {
        printf("  config.timeout = ");
        json_print_query_result(&result);
        printf("\n");
    }
    
    if (json_query_path(&decoder, 1, "config.retry", &result) == 0) {
        printf("  config.retry = ");
        json_print_query_result(&result);
        printf("\n");
    }

    // Example on object 2: {"data": [{"id": 1, "name": "sensor_a"}, ...]}
    
    printf("\nQuerying object 2:\n");
    
    if (json_query_path(&decoder, 2, "data[0].id", &result) == 0) {
        printf("  data[0].id = ");
        json_print_query_result(&result);
        printf("\n");
    }
    
    if (json_query_path(&decoder, 2, "data[1].name", &result) == 0) {
        printf("  data[1].name = ");
        json_print_query_result(&result);
        printf("\n");
    }

    // Test not found
    printf("\nTesting not found:\n");
    if (json_query_path(&decoder, 0, "nonexistent", &result) < 0 || !result.found) {
        printf("  'nonexistent' not found (expected)\n");
    }

    return 0;
}
```

## Example Output
```
Querying object 0:
  temperature = 23.5
  sensors[0] = 1
  sensors[2] = 3

Querying object 1:
  config.timeout = 30
  config.retry = true

Querying object 2:
  data[0].id = 1
  data[1].name = "sensor_b"

Testing not found:
  'nonexistent' not found (expected)


