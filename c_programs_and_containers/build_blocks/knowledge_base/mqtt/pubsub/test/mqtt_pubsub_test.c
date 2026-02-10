/**
 * mqtt_pubsub_test.c - Test driver for mqtt_pubsub library
 *
 * Registers add, multiply, get_time, echo methods on the server,
 * then the client makes sync and async calls.
 *
 * Requires: Mosquitto broker running on localhost:1883
 */

 #define _DEFAULT_SOURCE          /* usleep */
 #define _POSIX_C_SOURCE 200809L  /* strdup */
 
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <unistd.h>
 #include <time.h>
 
 #include "mqtt_pubsub.h"
 
 #define BROKER  "localhost"
 #define PORT    1883
 #define SERVICE "math_service"
 
 /* ================================================================== */
 /*  Server-side PubSub method implementations                             */
 /* ================================================================== */
 
 static cJSON *method_add(const cJSON *params, void *ud)
 {
     (void)ud;
     if (!params) return NULL;
     double a = 0, b = 0;
 
     if (cJSON_IsObject(params)) {
         cJSON *ja = cJSON_GetObjectItem(params, "a");
         cJSON *jb = cJSON_GetObjectItem(params, "b");
         if (ja) a = ja->valuedouble;
         if (jb) b = jb->valuedouble;
     } else if (cJSON_IsArray(params)) {
         cJSON *ja = cJSON_GetArrayItem(params, 0);
         cJSON *jb = cJSON_GetArrayItem(params, 1);
         if (ja) a = ja->valuedouble;
         if (jb) b = jb->valuedouble;
     }
     return cJSON_CreateNumber(a + b);
 }
 
 static cJSON *method_multiply(const cJSON *params, void *ud)
 {
     (void)ud;
     if (!params) return NULL;
     double a = 0, b = 0;
 
     if (cJSON_IsObject(params)) {
         cJSON *ja = cJSON_GetObjectItem(params, "a");
         cJSON *jb = cJSON_GetObjectItem(params, "b");
         if (ja) a = ja->valuedouble;
         if (jb) b = jb->valuedouble;
     } else if (cJSON_IsArray(params)) {
         cJSON *ja = cJSON_GetArrayItem(params, 0);
         cJSON *jb = cJSON_GetArrayItem(params, 1);
         if (ja) a = ja->valuedouble;
         if (jb) b = jb->valuedouble;
     }
     return cJSON_CreateNumber(a * b);
 }
 
 static cJSON *method_get_time(const cJSON *params, void *ud)
 {
     (void)params; (void)ud;
     time_t now = time(NULL);
     struct tm tm_buf;
     gmtime_r(&now, &tm_buf);
     char buf[64];
     strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm_buf);
     return cJSON_CreateString(buf);
 }
 
 static cJSON *method_echo(const cJSON *params, void *ud)
 {
     (void)ud;
     if (!params) return cJSON_CreateString("Echo: (null)");
 
     const char *msg = NULL;
     if (cJSON_IsObject(params)) {
         cJSON *jm = cJSON_GetObjectItem(params, "message");
         if (jm && cJSON_IsString(jm)) msg = jm->valuestring;
     } else if (cJSON_IsString(params)) {
         msg = params->valuestring;
     }
 
     char buf[256];
     snprintf(buf, sizeof(buf), "Echo: %s", msg ? msg : "(null)");
     return cJSON_CreateString(buf);
 }
 
 /* ================================================================== */
 /*  Async callback                                                     */
 /* ================================================================== */
 
 static void async_callback(const cJSON *error, const cJSON *result, void *ud)
 {
     (void)ud;
     if (error) {
         char *s = cJSON_PrintUnformatted(error);
         printf("[async] error: %s\n", s ? s : "(null)");
         free(s);
     } else if (result) {
         char *s = cJSON_PrintUnformatted(result);
         printf("[async] result: %s\n", s ? s : "(null)");
         free(s);
     }
 }
 
 /* ================================================================== */
 /*  Main                                                               */
 /* ================================================================== */
 
 int main(void)
 {
     printf("============================================\n");
     printf("   MQTT PubSub Library - Test Driver\n");
     printf("============================================\n");
     printf("Broker: %s:%d  Service: %s\n\n", BROKER, PORT, SERVICE);
 
     mqtt_pubsub_lib_init();
 
     int exit_code = 0;
 
     /* ---- start server -------------------------------------------- */
     mqtt_pubsub_config_t srv_cfg = {
         .host         = BROKER,
         .port         = PORT,
         .client_id    = "pubsub_server_test",
         .service_name = SERVICE,
         .keepalive    = 60,
         .username     = NULL,
         .password     = NULL,
         .qos          = 1,
     };
 
     pubsub_server_t server;
     if (pubsub_server_init(&server, &srv_cfg) != 0) {
         fprintf(stderr, "Server init failed\n");
         exit_code = 1; goto done;
     }
 
     pubsub_server_register(&server, "add",      method_add,      NULL);
     pubsub_server_register(&server, "multiply", method_multiply,  NULL);
     pubsub_server_register(&server, "get_time", method_get_time,  NULL);
     pubsub_server_register(&server, "echo",     method_echo,      NULL);
 
     if (pubsub_server_start(&server, true, 3000) != 0) {
         fprintf(stderr, "Server start failed. Is Mosquitto running?\n");
         pubsub_server_destroy(&server);
         exit_code = 1; goto done;
     }
 
     /* ---- start client -------------------------------------------- */
     mqtt_pubsub_config_t cli_cfg = {
         .host         = BROKER,
         .port         = PORT,
         .client_id    = "pubsub_client_test",
         .service_name = SERVICE,
         .keepalive    = 60,
         .username     = NULL,
         .password     = NULL,
         .qos          = 1,
     };
 
     pubsub_client_t client;
     if (pubsub_client_init(&client, &cli_cfg, 10.0f) != 0) {
         fprintf(stderr, "Client init failed\n");
         pubsub_server_stop(&server);
         pubsub_server_destroy(&server);
         exit_code = 1; goto done;
     }
 
     if (pubsub_client_connect(&client, 5000) != 0) {
         fprintf(stderr, "Client connect failed\n");
         pubsub_client_destroy(&client);
         pubsub_server_stop(&server);
         pubsub_server_destroy(&server);
         exit_code = 1; goto done;
     }
 
     /* Small settle time */
     usleep(500000);
 
     /* ---- synchronous calls --------------------------------------- */
     printf("\n--- Synchronous PubSub calls ---\n\n");
 
     cJSON *result = NULL, *error = NULL;
 
     /* add(5, 3) with named params */
     {
         cJSON *p = cJSON_CreateObject();
         cJSON_AddNumberToObject(p, "a", 5);
         cJSON_AddNumberToObject(p, "b", 3);
         int rc = pubsub_client_call(&client, "add", p, 0, &result, &error);
         if (rc == 0 && result) {
             printf("5 + 3 = %.0f\n", result->valuedouble);
             cJSON_Delete(result); result = NULL;
         } else {
             printf("add call failed (rc=%d)\n", rc);
             if (error) { cJSON_Delete(error); error = NULL; }
         }
         cJSON_Delete(p);
     }
 
     /* multiply(4, 7) with positional params */
     {
         cJSON *p = cJSON_CreateArray();
         cJSON_AddItemToArray(p, cJSON_CreateNumber(4));
         cJSON_AddItemToArray(p, cJSON_CreateNumber(7));
         int rc = pubsub_client_call(&client, "multiply", p, 0, &result, &error);
         if (rc == 0 && result) {
             printf("4 * 7 = %.0f\n", result->valuedouble);
             cJSON_Delete(result); result = NULL;
         } else {
             printf("multiply call failed (rc=%d)\n", rc);
             if (error) { cJSON_Delete(error); error = NULL; }
         }
         cJSON_Delete(p);
     }
 
     /* get_time() — no params */
     {
         int rc = pubsub_client_call(&client, "get_time", NULL, 0, &result, &error);
         if (rc == 0 && result) {
             printf("Server time: %s\n", result->valuestring ? result->valuestring : "(null)");
             cJSON_Delete(result); result = NULL;
         } else {
             printf("get_time call failed (rc=%d)\n", rc);
             if (error) { cJSON_Delete(error); error = NULL; }
         }
     }
 
     /* echo("Hello PubSub!") */
     {
         cJSON *p = cJSON_CreateObject();
         cJSON_AddStringToObject(p, "message", "Hello PubSub!");
         int rc = pubsub_client_call(&client, "echo", p, 0, &result, &error);
         if (rc == 0 && result) {
             printf("%s\n", result->valuestring ? result->valuestring : "(null)");
             cJSON_Delete(result); result = NULL;
         } else {
             printf("echo call failed (rc=%d)\n", rc);
             if (error) { cJSON_Delete(error); error = NULL; }
         }
         cJSON_Delete(p);
     }
 
     /* ---- error case: unknown method ------------------------------ */
     {
         int rc = pubsub_client_call(&client, "nonexistent", NULL, 0, &result, &error);
         if (rc == -2 && error) {
             cJSON *msg_j = cJSON_GetObjectItem(error, "message");
             printf("\nExpected error for unknown method: %s\n",
                    msg_j && cJSON_IsString(msg_j) ? msg_j->valuestring : "(?)");
             cJSON_Delete(error); error = NULL;
         } else {
             printf("\nUnexpected result for unknown method (rc=%d)\n", rc);
             if (result) { cJSON_Delete(result); result = NULL; }
         }
     }
 
     /* ---- asynchronous call --------------------------------------- */
     printf("\n--- Asynchronous PubSub call ---\n\n");
     {
         cJSON *p = cJSON_CreateObject();
         cJSON_AddNumberToObject(p, "a", 10);
         cJSON_AddNumberToObject(p, "b", 20);
         char *async_id = pubsub_client_call_async(&client, "add", p, 0,
                                                async_callback, NULL);
         if (async_id) {
             printf("Async call fired (id hint: %s)\n", async_id);
             free(async_id);
         }
         cJSON_Delete(p);
     }
 
     /* Give async response time to arrive */
     usleep(1500000);
 
     /* ---- teardown ------------------------------------------------ */
     printf("\n--- Teardown ---\n\n");
     pubsub_client_disconnect(&client);
     pubsub_client_destroy(&client);
     pubsub_server_stop(&server);
     pubsub_server_destroy(&server);
 
     printf("\n============================================\n");
     printf("   All PubSub tests passed!\n");
     printf("============================================\n");
 
 done:
     mqtt_pubsub_lib_cleanup();
     return exit_code;
 }