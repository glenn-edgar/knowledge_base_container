/* Additional helper functions for json_decoder.c */

/* Get the type of a specific record */
json_type_t json_get_record_type(const json_decoder_t *decoder, uint32_t record_index)
{
    if (record_index >= decoder->num_records) {
        return -1;
    }
    return decoder->records[record_index].object_type;
}

/* Get the root type of an object (from control index) */
json_type_t json_get_object_root_type(const json_decoder_t *decoder, uint32_t control_index)
{
    if (control_index >= decoder->num_controls) {
        return -1;
    }
    uint32_t start = decoder->controls[control_index].start_position;
    return json_get_record_type(decoder, start);
}

/* Count records in an object tree (useful for validation) */
uint32_t json_count_records_recursive(const json_decoder_t *decoder, uint32_t *index)
{
    if (*index >= decoder->num_records) {
        return 0;
    }

    const json_record_t *rec = &decoder->records[*index];
    (*index)++;
    uint32_t count = 1;  // Count this record

    if (rec->object_type == JSON_TYPE_ARRAY) {
        uint32_t num_elements = rec->value.container_count;
        for (uint32_t i = 0; i < num_elements; i++) {
            count += json_count_records_recursive(decoder, index);
        }
    } else if (rec->object_type == JSON_TYPE_OBJECT) {
        uint32_t num_pairs = rec->value.container_count;
        for (uint32_t i = 0; i < num_pairs; i++) {
            count += json_count_records_recursive(decoder, index);  // key
            count += json_count_records_recursive(decoder, index);  // value
        }
    }

    return count;
}

/* Validate that record controls match actual record counts */
int json_validate_controls(const json_decoder_t *decoder)
{
    for (uint32_t i = 0; i < decoder->num_controls; i++) {
        uint32_t index = decoder->controls[i].start_position;
        uint32_t counted = json_count_records_recursive(decoder, &index);
        
        if (counted != decoder->controls[i].num_records) {
            fprintf(stderr, "Control %u: expected %u records, counted %u\n",
                    i, decoder->controls[i].num_records, counted);
            return -1;
        }
    }
    return 0;
}

