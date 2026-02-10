/**
 * @file test_nats_rpc.c
 * @brief Test driver for nats_rpc library.
 *
 * Requires a running NATS server at 127.0.0.1:4222:
 *   docker run -p 4222:4222 nats:latest
 *
 * Usage:
 *   ./test_nats_rpc              # run all tests
 *   ./test_nats_rpc tests        # tests only
 *   ./test_nats_rpc demo         # interactive demo
 */

#define _GNU_SOURCE

#include <inttypes.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "nats_rpc.h"
#include <cjson/cJSON.h>

/* ------------------------------------------------------------------ */
/*  Minimal test framework                                             */
/* ------------------------------------------------------------------ */

static int tests_run    = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST_SERVER "nats://127.0.0.1:4222"

#define RUN_TEST(fn)                                               \
    do {                                                           \
        tests_run++;                                               \
        printf("  %-50s ", #fn);                                   \
        fflush(stdout);                                            \
        if (fn()) {                                                \
            tests_passed++;                                        \
            printf("[PASS]\n");                                    \
        } else {                                                   \
            tests_failed++;                                        \
            printf("[FAIL]\n");                                    \
        }                                                          \
    } while (0)

#define EXPECT(cond)                                               \
    do {                                                           \
        if (!(cond)) {                                             \
            fprintf(stderr, "    FAIL at %s:%d: %s\n",             \
                    __FILE__, __LINE__, #cond);                    \
            return false;                                          \
        }                                                          \
    } while (0)

#define EXPECT_OK(st) EXPECT((st) == RPC_OK)

/* ------------------------------------------------------------------ */
/*  Test handler functions                                             */
/* ------------------------------------------------------------------ */

static rpc_status_t handler_add(const char *params_json,
                                void *user_data, char **result_json)
{
    (void)user_data;
    cJSON *p = cJSON_Parse(params_json);
    if (!p) return RPC_ERR_DECODE;

    cJSON *a = cJSON_GetObjectItem(p, "a");
    cJSON *b = cJSON_GetObjectItem(p, "b");

    if (!a || !b || !cJSON_IsNumber(a) || !cJSON_IsNumber(b)) {
        cJSON_Delete(p);
        *result_json = strdup("Invalid params: expected a and b numbers");
        return RPC_ERR_HANDLER;
    }

    double sum = a->valuedouble + b->valuedouble;
    cJSON_Delete(p);

    char buf[64];
    snprintf(buf, sizeof(buf), "%.10g", sum);
    *result_json = strdup(buf);
    return RPC_OK;
}

static rpc_status_t handler_multiply(const char *params_json,
                                     void *user_data, char **result_json)
{
    (void)user_data;
    cJSON *p = cJSON_Parse(params_json);
    if (!p) return RPC_ERR_DECODE;

    cJSON *x = cJSON_GetObjectItem(p, "x");
    cJSON *y = cJSON_GetObjectItem(p, "y");

    if (!x || !y || !cJSON_IsNumber(x) || !cJSON_IsNumber(y)) {
        cJSON_Delete(p);
        *result_json = strdup("Invalid params: expected x and y numbers");
        return RPC_ERR_HANDLER;
    }

    double product = x->valuedouble * y->valuedouble;
    cJSON_Delete(p);

    char buf[64];
    snprintf(buf, sizeof(buf), "%.10g", product);
    *result_json = strdup(buf);
    return RPC_OK;
}

static rpc_status_t handler_echo(const char *params_json,
                                 void *user_data, char **result_json)
{
    (void)user_data;
    *result_json = strdup(params_json);
    return RPC_OK;
}

static rpc_status_t handler_greet(const char *params_json,
                                  void *user_data, char **result_json)
{
    (void)user_data;
    cJSON *p = cJSON_Parse(params_json);
    if (!p) {
        *result_json = strdup("{\"greeting\":\"Hello, World!\"}");
        return RPC_OK;
    }

    cJSON *name = cJSON_GetObjectItem(p, "name");
    const char *n = (name && cJSON_IsString(name)) ? name->valuestring : "World";

    cJSON *r = cJSON_CreateObject();
    char greeting[256];
    snprintf(greeting, sizeof(greeting), "Hello, %s!", n);
    cJSON_AddStringToObject(r, "greeting", greeting);

    *result_json = cJSON_PrintUnformatted(r);
    cJSON_Delete(r);
    cJSON_Delete(p);
    return RPC_OK;
}

static rpc_status_t handler_fail(const char *params_json,
                                 void *user_data, char **result_json)
{
    (void)params_json;
    (void)user_data;
    *result_json = strdup("Intentional failure for testing");
    return RPC_ERR_HANDLER;
}

static rpc_status_t handler_slow(const char *params_json,
                                 void *user_data, char **result_json)
{
    (void)params_json;
    (void)user_data;
    usleep(2000000);  /* 2 seconds */
    *result_json = strdup("\"done\"");
    return RPC_OK;
}

/* context pointer test */
typedef struct { int offset; } MathCtx;

static rpc_status_t handler_add_offset(const char *params_json,
                                       void *user_data, char **result_json)
{
    MathCtx *ctx = user_data;
    cJSON *p = cJSON_Parse(params_json);
    if (!p) return RPC_ERR_DECODE;

    cJSON *val = cJSON_GetObjectItem(p, "value");
    if (!val || !cJSON_IsNumber(val)) {
        cJSON_Delete(p);
        *result_json = strdup("Expected 'value'");
        return RPC_ERR_HANDLER;
    }

    double r = val->valuedouble + ctx->offset;
    cJSON_Delete(p);

    char buf[64];
    snprintf(buf, sizeof(buf), "%.10g", r);
    *result_json = strdup(buf);
    return RPC_OK;
}

/* ------------------------------------------------------------------ */
/*  Helper: create server+client pair, run test, clean up              */
/* ------------------------------------------------------------------ */

typedef struct {
    RpcServer *srv;
    RpcClient *cli;
} TestPair;

static bool setup_pair(TestPair *tp, const char *srv_id, const char *cli_id,
                       const char *ns)
{
    RpcConfig cfg;
    rpc_config_defaults(&cfg);
    cfg.server      = TEST_SERVER;
    cfg.namespace_  = ns;
    cfg.instance_id = srv_id;
    cfg.enable_health = true;

    if (rpc_server_create(&tp->srv, &cfg) != RPC_OK) return false;

    cfg.instance_id = cli_id;
    cfg.enable_health = false;
    if (rpc_client_create(&tp->cli, &cfg) != RPC_OK) return false;

    return true;
}

static void teardown_pair(TestPair *tp)
{
    if (tp->srv) { rpc_server_stop(tp->srv); rpc_server_destroy(tp->srv); }
    if (tp->cli) { rpc_client_disconnect(tp->cli); rpc_client_destroy(tp->cli); }
}

/* ================================================================== */
/*  Tests                                                              */
/* ================================================================== */

static bool test_status_strings(void)
{
    EXPECT(strcmp(rpc_status_str(RPC_OK), "ok") == 0);
    EXPECT(strcmp(rpc_status_str(RPC_ERR_TIMEOUT), "timeout") == 0);
    EXPECT(strcmp(rpc_status_str(RPC_ERR_HANDLER), "handler_error") == 0);
    EXPECT(strcmp(rpc_status_str(RPC_ERR_NATS), "nats_error") == 0);
    return true;
}

static bool test_config_defaults(void)
{
    RpcConfig cfg;
    rpc_config_defaults(&cfg);
    EXPECT(cfg.server != NULL);
    EXPECT(cfg.namespace_ != NULL);
    EXPECT(strcmp(cfg.namespace_, "default") == 0);
    EXPECT(cfg.enable_health == true);
    EXPECT(cfg.instance_id == NULL);
    return true;
}

static bool test_basic_call(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_basic", "test_cli_basic", "test_basic"));

    rpc_server_register(tp.srv, "math.add", handler_add, NULL, false);
    EXPECT_OK(rpc_server_start(tp.srv, "rpc"));
    EXPECT_OK(rpc_client_connect(tp.cli));

    /* Small delay for subscriptions to propagate */
    usleep(100000);

    char *result = NULL;
    EXPECT_OK(rpc_client_call(tp.cli, "rpc.math.add",
                              "{\"a\":5,\"b\":3}", 5.0, &result));
    EXPECT(result != NULL);

    double val = atof(result);
    EXPECT(fabs(val - 8.0) < 0.001);
    free(result);

    teardown_pair(&tp);
    return true;
}

static bool test_multiply(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_mul", "test_cli_mul", "test_mul"));

    rpc_server_register(tp.srv, "math.multiply", handler_multiply, NULL, false);
    EXPECT_OK(rpc_server_start(tp.srv, "rpc"));
    EXPECT_OK(rpc_client_connect(tp.cli));
    usleep(100000);

    char *result = NULL;
    EXPECT_OK(rpc_client_call(tp.cli, "rpc.math.multiply",
                              "{\"x\":4,\"y\":6}", 5.0, &result));
    EXPECT(result != NULL);

    double val = atof(result);
    EXPECT(fabs(val - 24.0) < 0.001);
    free(result);

    teardown_pair(&tp);
    return true;
}

static bool test_echo(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_echo", "test_cli_echo", "test_echo"));

    rpc_server_register(tp.srv, "echo", handler_echo, NULL, false);
    EXPECT_OK(rpc_server_start(tp.srv, ""));
    EXPECT_OK(rpc_client_connect(tp.cli));
    usleep(100000);

    char *result = NULL;
    EXPECT_OK(rpc_client_call(tp.cli, "echo",
                              "{\"hello\":\"world\"}", 5.0, &result));
    EXPECT(result != NULL);
    EXPECT(strstr(result, "hello") != NULL);
    EXPECT(strstr(result, "world") != NULL);
    free(result);

    teardown_pair(&tp);
    return true;
}

static bool test_greet(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_greet", "test_cli_greet", "test_greet"));

    rpc_server_register(tp.srv, "greet", handler_greet, NULL, false);
    EXPECT_OK(rpc_server_start(tp.srv, ""));
    EXPECT_OK(rpc_client_connect(tp.cli));
    usleep(100000);

    char *result = NULL;
    EXPECT_OK(rpc_client_call(tp.cli, "greet",
                              "{\"name\":\"Glenn\"}", 5.0, &result));
    EXPECT(result != NULL);
    EXPECT(strstr(result, "Glenn") != NULL);
    free(result);

    teardown_pair(&tp);
    return true;
}

static bool test_user_data(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_ud", "test_cli_ud", "test_ud"));

    MathCtx ctx = { .offset = 100 };
    rpc_server_register(tp.srv, "add_offset", handler_add_offset,
                        &ctx, false);
    EXPECT_OK(rpc_server_start(tp.srv, ""));
    EXPECT_OK(rpc_client_connect(tp.cli));
    usleep(100000);

    char *result = NULL;
    EXPECT_OK(rpc_client_call(tp.cli, "add_offset",
                              "{\"value\":42}", 5.0, &result));
    EXPECT(result != NULL);
    double val = atof(result);
    EXPECT(fabs(val - 142.0) < 0.001);
    free(result);

    teardown_pair(&tp);
    return true;
}

static bool test_handler_error(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_err", "test_cli_err", "test_err"));

    rpc_server_register(tp.srv, "fail", handler_fail, NULL, false);
    EXPECT_OK(rpc_server_start(tp.srv, ""));
    EXPECT_OK(rpc_client_connect(tp.cli));
    usleep(100000);

    char *result = NULL;
    rpc_status_t st = rpc_client_call(tp.cli, "fail", "{}", 5.0, &result);
    EXPECT(st == RPC_ERR_HANDLER);
    EXPECT(result != NULL);
    EXPECT(strstr(result, "Intentional") != NULL);
    free(result);

    teardown_pair(&tp);
    return true;
}

static bool test_timeout(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_to", "test_cli_to", "test_to"));

    rpc_server_register(tp.srv, "slow", handler_slow, NULL, false);
    EXPECT_OK(rpc_server_start(tp.srv, ""));
    EXPECT_OK(rpc_client_connect(tp.cli));
    usleep(100000);

    char *result = NULL;
    rpc_status_t st = rpc_client_call(tp.cli, "slow", "{}", 0.5, &result);
    EXPECT(st == RPC_ERR_TIMEOUT);
    free(result);

    teardown_pair(&tp);
    return true;
}

static bool test_no_server(void)
{
    RpcConfig cfg;
    rpc_config_defaults(&cfg);
    cfg.server      = TEST_SERVER;
    cfg.namespace_  = "test_nosrv";
    cfg.instance_id = "cli_nosrv";

    RpcClient *cli = NULL;
    EXPECT_OK(rpc_client_create(&cli, &cfg));
    EXPECT_OK(rpc_client_connect(cli));

    char *result = NULL;
    rpc_status_t st = rpc_client_call(cli, "nonexistent", "{}", 0.5, &result);
    EXPECT(st == RPC_ERR_TIMEOUT);
    free(result);

    rpc_client_destroy(cli);
    return true;
}

static bool test_instance_specific(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_inst", "test_cli_inst", "test_inst"));

    rpc_server_register(tp.srv, "private_method", handler_echo, NULL, true);
    EXPECT_OK(rpc_server_start(tp.srv, "rpc"));
    EXPECT_OK(rpc_client_connect(tp.cli));
    usleep(100000);

    /* Call targeting the specific instance */
    char *result = NULL;
    EXPECT_OK(rpc_client_call_instance(tp.cli, "rpc.private_method",
                                       "{\"msg\":\"targeted\"}",
                                       5.0, "test_srv_inst", &result));
    EXPECT(result != NULL);
    EXPECT(strstr(result, "targeted") != NULL);
    free(result);

    teardown_pair(&tp);
    return true;
}

static bool test_health_check(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_health", "test_cli_health", "test_health"));

    rpc_server_register(tp.srv, "math.add", handler_add, NULL, false);
    EXPECT_OK(rpc_server_start(tp.srv, "rpc"));
    EXPECT_OK(rpc_client_connect(tp.cli));
    usleep(100000);

    /* Make a normal call first */
    char *r1 = NULL;
    EXPECT_OK(rpc_client_call(tp.cli, "rpc.math.add",
                              "{\"a\":1,\"b\":2}", 5.0, &r1));
    free(r1);

    /* Call health check (instance-specific) */
    char *result = NULL;
    EXPECT_OK(rpc_client_call_instance(tp.cli, "rpc._health", "{}",
                                       5.0, "test_srv_health", &result));
    EXPECT(result != NULL);
    EXPECT(strstr(result, "healthy") != NULL);
    EXPECT(strstr(result, "test_srv_health") != NULL);
    EXPECT(strstr(result, "math.add") != NULL);
    free(result);

    teardown_pair(&tp);
    return true;
}

static bool test_server_stats(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_stats", "test_cli_stats", "test_stats"));

    rpc_server_register(tp.srv, "math.add", handler_add, NULL, false);
    rpc_server_register(tp.srv, "fail", handler_fail, NULL, false);
    EXPECT_OK(rpc_server_start(tp.srv, "rpc"));
    EXPECT_OK(rpc_client_connect(tp.cli));
    usleep(100000);

    /* Make some calls */
    char *r = NULL;
    rpc_client_call(tp.cli, "rpc.math.add", "{\"a\":1,\"b\":2}", 5.0, &r);
    free(r); r = NULL;
    rpc_client_call(tp.cli, "rpc.math.add", "{\"a\":3,\"b\":4}", 5.0, &r);
    free(r); r = NULL;
    rpc_client_call(tp.cli, "rpc.fail", "{}", 5.0, &r);
    free(r); r = NULL;

    usleep(100000);

    RpcHandlerStats *stats = NULL;
    size_t count = 0;
    EXPECT_OK(rpc_server_get_stats(tp.srv, &stats, &count));
    EXPECT(count >= 2);

    /* Find math.add stats */
    bool found_add = false;
    for (size_t i = 0; i < count; i++) {
        if (strcmp(stats[i].method, "math.add") == 0) {
            EXPECT(stats[i].call_count == 2);
            EXPECT(stats[i].error_count == 0);
            found_add = true;
        }
    }
    EXPECT(found_add);

    free(stats);
    teardown_pair(&tp);
    return true;
}

static bool test_batch_calls(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_batch", "test_cli_batch", "test_batch"));

    rpc_server_register(tp.srv, "math.add", handler_add, NULL, false);
    rpc_server_register(tp.srv, "math.multiply", handler_multiply, NULL, false);
    EXPECT_OK(rpc_server_start(tp.srv, "rpc"));
    EXPECT_OK(rpc_client_connect(tp.cli));
    usleep(100000);

    RpcBatchEntry entries[] = {
        { "rpc.math.add",      "{\"a\":1,\"b\":2}",   NULL },
        { "rpc.math.multiply", "{\"x\":3,\"y\":4}",   NULL },
        { "rpc.math.add",      "{\"a\":10,\"b\":20}", NULL },
    };

    RpcBatchResult results[3] = {0};
    EXPECT_OK(rpc_client_call_batch(tp.cli, entries, 3, 5.0, results));

    EXPECT(results[0].status == RPC_OK);
    EXPECT(fabs(atof(results[0].result_json) - 3.0) < 0.001);

    EXPECT(results[1].status == RPC_OK);
    EXPECT(fabs(atof(results[1].result_json) - 12.0) < 0.001);

    EXPECT(results[2].status == RPC_OK);
    EXPECT(fabs(atof(results[2].result_json) - 30.0) < 0.001);

    for (int i = 0; i < 3; i++) free(results[i].result_json);

    teardown_pair(&tp);
    return true;
}

static bool test_multiple_methods(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_multi", "test_cli_multi", "test_multi"));

    rpc_server_register(tp.srv, "math.add", handler_add, NULL, false);
    rpc_server_register(tp.srv, "math.multiply", handler_multiply, NULL, false);
    rpc_server_register(tp.srv, "echo", handler_echo, NULL, false);
    rpc_server_register(tp.srv, "greet", handler_greet, NULL, false);
    EXPECT_OK(rpc_server_start(tp.srv, ""));
    EXPECT_OK(rpc_client_connect(tp.cli));
    usleep(100000);

    char *r = NULL;

    EXPECT_OK(rpc_client_call(tp.cli, "math.add",
                              "{\"a\":100,\"b\":200}", 5.0, &r));
    EXPECT(fabs(atof(r) - 300.0) < 0.001);
    free(r); r = NULL;

    EXPECT_OK(rpc_client_call(tp.cli, "math.multiply",
                              "{\"x\":7,\"y\":8}", 5.0, &r));
    EXPECT(fabs(atof(r) - 56.0) < 0.001);
    free(r); r = NULL;

    EXPECT_OK(rpc_client_call(tp.cli, "echo",
                              "{\"test\":true}", 5.0, &r));
    EXPECT(strstr(r, "test") != NULL);
    free(r); r = NULL;

    EXPECT_OK(rpc_client_call(tp.cli, "greet",
                              "{\"name\":\"NATS\"}", 5.0, &r));
    EXPECT(strstr(r, "NATS") != NULL);
    free(r);

    teardown_pair(&tp);
    return true;
}

static double monotonic_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
}

static bool test_performance(void)
{
    TestPair tp = {0};
    EXPECT(setup_pair(&tp, "test_srv_perf", "test_cli_perf", "test_perf"));

    rpc_server_register(tp.srv, "echo", handler_echo, NULL, false);
    EXPECT_OK(rpc_server_start(tp.srv, ""));
    EXPECT_OK(rpc_client_connect(tp.cli));
    usleep(100000);

    int n = 100;
    double t0 = monotonic_ms();

    for (int i = 0; i < n; i++) {
        char *r = NULL;
        rpc_status_t st = rpc_client_call(tp.cli, "echo",
                                          "{\"i\":1}", 5.0, &r);
        if (st != RPC_OK) { free(r); EXPECT(false); }
        free(r);
    }

    double elapsed = monotonic_ms() - t0;
    printf("\n    %d calls in %.1f ms (%.1f calls/sec) ",
           n, elapsed, n / (elapsed / 1000.0));

    teardown_pair(&tp);
    return true;
}

/* ================================================================== */
/*  Demo                                                               */
/* ================================================================== */

static void run_demo(void)
{
    printf("\n");
    printf("====================================================\n");
    printf("  NATS RPC Demo (C)\n");
    printf("====================================================\n");

    RpcConfig cfg;
    rpc_config_defaults(&cfg);
    cfg.server      = TEST_SERVER;
    cfg.namespace_  = "demo";
    cfg.instance_id = "demo_server";

    RpcServer *srv = NULL;
    rpc_server_create(&srv, &cfg);

    rpc_server_register(srv, "math.add", handler_add, NULL, false);
    rpc_server_register(srv, "math.multiply", handler_multiply, NULL, false);
    rpc_server_register(srv, "echo", handler_echo, NULL, false);
    rpc_server_register(srv, "greet", handler_greet, NULL, false);

    if (rpc_server_start(srv, "rpc") != RPC_OK) {
        fprintf(stderr, "Failed to start server\n");
        rpc_server_destroy(srv);
        return;
    }

    cfg.instance_id = "demo_client";
    RpcClient *cli = NULL;
    rpc_client_create(&cli, &cfg);

    if (rpc_client_connect(cli) != RPC_OK) {
        fprintf(stderr, "Failed to connect client\n");
        rpc_server_destroy(srv);
        return;
    }

    usleep(200000);

    /* 1. Basic calls */
    printf("\n1. Basic RPC calls:\n");
    char *r = NULL;

    rpc_client_call(cli, "rpc.math.add", "{\"a\":5,\"b\":3}", 5.0, &r);
    printf("   add(5, 3) = %s\n", r);
    free(r); r = NULL;

    rpc_client_call(cli, "rpc.math.multiply", "{\"x\":4,\"y\":6}", 5.0, &r);
    printf("   multiply(4, 6) = %s\n", r);
    free(r); r = NULL;

    rpc_client_call(cli, "rpc.greet", "{\"name\":\"Glenn\"}", 5.0, &r);
    printf("   greet(Glenn) = %s\n", r);
    free(r); r = NULL;

    /* 2. Batch calls */
    printf("\n2. Batch RPC calls:\n");

    RpcBatchEntry entries[] = {
        { "rpc.math.add",      "{\"a\":10,\"b\":20}", NULL },
        { "rpc.math.multiply", "{\"x\":7,\"y\":8}",   NULL },
        { "rpc.math.add",      "{\"a\":100,\"b\":200}", NULL },
    };
    RpcBatchResult results[3] = {0};

    rpc_client_call_batch(cli, entries, 3, 5.0, results);
    for (int i = 0; i < 3; i++) {
        printf("   Batch %d: %s = %s\n", i + 1,
               entries[i].method, results[i].result_json);
        free(results[i].result_json);
    }

    /* 3. Health check */
    printf("\n3. Health check:\n");
    rpc_client_call_instance(cli, "rpc._health", "{}",
                             5.0, "demo_server", &r);
    /* Pretty-print */
    cJSON *health = cJSON_Parse(r);
    if (health) {
        char *pretty = cJSON_Print(health);
        printf("   %s\n", pretty);
        free(pretty);
        cJSON_Delete(health);
    }
    free(r); r = NULL;

    /* 4. Error handling */
    printf("\n4. Error handling:\n");
    rpc_status_t st = rpc_client_call(cli, "rpc.nonexistent", "{}",
                                      0.5, &r);
    printf("   Call to nonexistent: %s\n", rpc_status_str(st));
    free(r); r = NULL;

    /* 5. Server stats */
    printf("\n5. Server statistics:\n");
    RpcHandlerStats *stats = NULL;
    size_t count = 0;
    rpc_server_get_stats(srv, &stats, &count);
    for (size_t i = 0; i < count; i++) {
        printf("   %-20s calls=%" PRId64 "  errors=%" PRId64 "%s\n",
               stats[i].method, stats[i].call_count, stats[i].error_count,
               stats[i].instance_specific ? " (instance)" : "");
    }
    free(stats);

    /* 6. Performance */
    printf("\n6. Performance test (100 echo calls):\n");
    double t0 = monotonic_ms();
    for (int i = 0; i < 100; i++) {
        rpc_client_call(cli, "rpc.echo", "{\"i\":1}", 5.0, &r);
        free(r); r = NULL;
    }
    double elapsed = monotonic_ms() - t0;
    printf("   100 calls in %.1f ms (%.0f calls/sec)\n",
           elapsed, 100.0 / (elapsed / 1000.0));

    /* Cleanup */
    printf("\n");
    rpc_client_destroy(cli);
    rpc_server_destroy(srv);

    printf("====================================================\n");
}

/* ================================================================== */
/*  Main                                                               */
/* ================================================================== */

int main(int argc, char **argv)
{
    const char *mode = (argc > 1) ? argv[1] : "all";

    printf("\n======================================================================\n");
    printf("  NATS RPC Test Suite (C)\n");
    printf("  Server: %s\n", TEST_SERVER);
    printf("======================================================================\n");

    if (strcmp(mode, "demo") == 0) {
        run_demo();
        return 0;
    }

    if (strcmp(mode, "all") == 0 || strcmp(mode, "tests") == 0) {
        printf("\n--- Basic ---\n");
        RUN_TEST(test_status_strings);
        RUN_TEST(test_config_defaults);

        printf("\n--- RPC Calls ---\n");
        RUN_TEST(test_basic_call);
        RUN_TEST(test_multiply);
        RUN_TEST(test_echo);
        RUN_TEST(test_greet);
        RUN_TEST(test_user_data);
        RUN_TEST(test_multiple_methods);

        printf("\n--- Error Handling ---\n");
        RUN_TEST(test_handler_error);
        RUN_TEST(test_timeout);
        RUN_TEST(test_no_server);

        printf("\n--- Advanced ---\n");
        RUN_TEST(test_instance_specific);
        RUN_TEST(test_health_check);
        RUN_TEST(test_server_stats);
        RUN_TEST(test_batch_calls);

        printf("\n--- Performance ---\n");
        RUN_TEST(test_performance);
    }

    printf("\n======================================================================\n");
    printf("  Results: %d run, %d passed, %d failed (%.1f%%)\n",
           tests_run, tests_passed, tests_failed,
           tests_run > 0 ? (100.0 * tests_passed / tests_run) : 0.0);
    printf("======================================================================\n\n");

    return tests_failed > 0 ? 1 : 0;
}
