/**
 * @file robot_config.c
 * @brief Load robot configuration from JSON file using cJSON.
 */

#include "robot_config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cjson/cJSON.h>

int robot_config_load(const char *filename, robot_config_t *config) {
    memset(config, 0, sizeof(robot_config_t));

    /* Defaults */
    strncpy(config->mqtt_host, "localhost", sizeof(config->mqtt_host) - 1);
    config->mqtt_port = 1883;
    config->energy_max = 10000;
    strncpy(config->wire_format, "cbor", sizeof(config->wire_format) - 1);

    FILE *fp = fopen(filename, "r");
    if (!fp) {
        fprintf(stderr, "[config] cannot open %s\n", filename);
        return -1;
    }

    fseek(fp, 0, SEEK_END);
    long len = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *buf = malloc(len + 1);
    if (!buf) {
        fclose(fp);
        return -1;
    }
    size_t nread = fread(buf, 1, len, fp);
    (void)nread;
    buf[len] = '\0';
    fclose(fp);

    cJSON *root = cJSON_Parse(buf);
    free(buf);
    if (!root) {
        fprintf(stderr, "[config] JSON parse error\n");
        return -1;
    }

    cJSON *item;

    item = cJSON_GetObjectItem(root, "robot_id");
    if (item && item->valuestring)
        strncpy(config->robot_id, item->valuestring, ROBOT_ID_MAX - 1);

    item = cJSON_GetObjectItem(root, "site");
    if (item && item->valuestring)
        strncpy(config->site, item->valuestring, SITE_MAX - 1);

    item = cJSON_GetObjectItem(root, "mqtt_host");
    if (item && item->valuestring)
        strncpy(config->mqtt_host, item->valuestring, sizeof(config->mqtt_host) - 1);

    item = cJSON_GetObjectItem(root, "mqtt_port");
    if (item && cJSON_IsNumber(item))
        config->mqtt_port = item->valueint;

    item = cJSON_GetObjectItem(root, "robot_class");
    if (item && item->valuestring)
        strncpy(config->robot_class, item->valuestring, sizeof(config->robot_class) - 1);

    item = cJSON_GetObjectItem(root, "energy_max");
    if (item && cJSON_IsNumber(item))
        config->energy_max = item->valueint;

    item = cJSON_GetObjectItem(root, "energy_infinite");
    if (item && cJSON_IsBool(item))
        config->energy_infinite = cJSON_IsTrue(item);

    item = cJSON_GetObjectItem(root, "wire_format");
    if (item && item->valuestring)
        strncpy(config->wire_format, item->valuestring, WIRE_FORMAT_MAX - 1);

    cJSON_Delete(root);
    return 0;
}

void robot_config_print(const robot_config_t *config) {
    printf("[config] robot_id:       %s\n", config->robot_id);
    printf("[config] site:           %s\n", config->site);
    printf("[config] mqtt_host:      %s\n", config->mqtt_host);
    printf("[config] mqtt_port:      %d\n", config->mqtt_port);
    printf("[config] robot_class:    %s\n", config->robot_class);
    printf("[config] energy_max:     %d\n", config->energy_max);
    printf("[config] energy_infinite:%s\n", config->energy_infinite ? "true" : "false");
    printf("[config] wire_format:    %s\n", config->wire_format);
}
