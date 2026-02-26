/*
 * test_kb_query_support.c
 * Knowledge Base C Port — KB_Search unit tests
 *
 * Creates an in-memory test database with sample KB data,
 * then exercises the CTE filter chain.
 */

#include <stdio.h>
#include <stdlib.h>
#include "kb_common.h"
#include "kb_query_support.h"
#include "test_common.h"

static sqlite3 *create_test_db(void)
{
    sqlite3 *db = NULL;
    int rc = sqlite3_open(":memory:", &db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to open in-memory DB\n");
        return NULL;
    }

    /* Create a minimal knowledge_base table */
    const char *ddl =
        "CREATE TABLE test_kb ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  knowledge_base TEXT,"
        "  label TEXT,"
        "  name TEXT,"
        "  path TEXT,"
        "  properties TEXT,"
        "  data TEXT,"
        "  has_link INTEGER DEFAULT 0,"
        "  has_link_mount INTEGER DEFAULT 0"
        ");"

        "INSERT INTO test_kb (knowledge_base, label, name, path, properties, data, has_link) VALUES"
        "('kb1', 'article', 'intro', 'kb1.docs.intro', "
        " '{\"difficulty\":\"beginner\",\"description\":\"Introduction\"}', '{\"content\":\"hello\"}', 0),"
        "('kb1', 'article', 'advanced', 'kb1.docs.advanced', "
        " '{\"difficulty\":\"advanced\",\"description\":\"Deep dive\"}', '{\"content\":\"deep\"}', 1),"
        "('kb1', 'KB_STATUS_FIELD', 'temperature', 'kb1.sensors.temperature', "
        " '{\"unit\":\"celsius\",\"description\":\"Temp sensor\"}', '{\"value\":22.5}', 0),"
        "('kb2', 'article', 'overview', 'kb2.docs.overview', "
        " '{\"difficulty\":\"beginner\",\"description\":\"Overview\"}', '{\"content\":\"summary\"}', 0),"
        "('kb1', 'KB_JOB_FIELD', 'processor', 'kb1.jobs.processor', "
        " '{\"description\":\"Job processor\"}', '{}', 0);";

    char *errmsg = NULL;
    rc = sqlite3_exec(db, ddl, NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "DDL error: %s\n", errmsg ? errmsg : "unknown");
        if (errmsg) sqlite3_free(errmsg);
        sqlite3_close(db);
        return NULL;
    }

    return db;
}

static void test_no_filters(sqlite3 *db)
{
    TEST_BEGIN("no filters (select all)");

    kb_search_t *ks = kb_search_create_from_db(db, "test_kb");
    ASSERT_NOT_NULL(ks, "create KB_Search");

    ASSERT_OK(kb_search_execute(ks), "execute with no filters");

    const kb_result_t *r = kb_search_results(ks);
    ASSERT_EQ_INT(r->count, 5, "should return all 5 rows");

    kb_search_destroy(ks);
}

static void test_label_filter(sqlite3 *db)
{
    TEST_BEGIN("label filter");

    kb_search_t *ks = kb_search_create_from_db(db, "test_kb");
    kb_search_label(ks, "article");
    ASSERT_OK(kb_search_execute(ks), "execute label=article");

    const kb_result_t *r = kb_search_results(ks);
    ASSERT_EQ_INT(r->count, 3, "3 articles");

    kb_search_clear_filters(ks);
    kb_search_label(ks, "KB_STATUS_FIELD");
    ASSERT_OK(kb_search_execute(ks), "execute label=KB_STATUS_FIELD");
    r = kb_search_results(ks);
    ASSERT_EQ_INT(r->count, 1, "1 status field");

    kb_search_destroy(ks);
}

static void test_combined_filters(sqlite3 *db)
{
    TEST_BEGIN("combined filters (kb + label)");

    kb_search_t *ks = kb_search_create_from_db(db, "test_kb");
    kb_search_kb(ks, "kb1");
    kb_search_label(ks, "article");
    ASSERT_OK(kb_search_execute(ks), "execute kb=kb1 + label=article");

    const kb_result_t *r = kb_search_results(ks);
    ASSERT_EQ_INT(r->count, 2, "2 kb1 articles");

    kb_search_destroy(ks);
}

static void test_name_filter(sqlite3 *db)
{
    TEST_BEGIN("name filter");

    kb_search_t *ks = kb_search_create_from_db(db, "test_kb");
    kb_search_name(ks, "intro");
    ASSERT_OK(kb_search_execute(ks), "execute name=intro");

    const kb_result_t *r = kb_search_results(ks);
    ASSERT_EQ_INT(r->count, 1, "1 row named intro");

    const char *path = kb_row_get(r, 0, "path");
    ASSERT_EQ_STR(path, "kb1.docs.intro", "path is kb1.docs.intro");

    kb_search_destroy(ks);
}

static void test_has_link_filter(sqlite3 *db)
{
    TEST_BEGIN("has_link filter");

    kb_search_t *ks = kb_search_create_from_db(db, "test_kb");
    kb_search_has_link(ks);
    ASSERT_OK(kb_search_execute(ks), "execute has_link");

    const kb_result_t *r = kb_search_results(ks);
    ASSERT_EQ_INT(r->count, 1, "1 row with has_link=1");

    kb_search_destroy(ks);
}

static void test_find_path_values(sqlite3 *db)
{
    TEST_BEGIN("find_path_values");

    kb_search_t *ks = kb_search_create_from_db(db, "test_kb");
    kb_search_label(ks, "article");
    ASSERT_OK(kb_search_execute(ks), "execute");

    const kb_result_t *r = kb_search_results(ks);
    char **paths = NULL;
    int count = 0;

    ASSERT_OK(kb_search_find_path_values(r, &paths, &count),
              "find_path_values");
    ASSERT_EQ_INT(count, 3, "3 paths");

    kb_path_values_free(paths, count);
    kb_search_destroy(ks);
}

static void test_decode_link_nodes(void)
{
    TEST_BEGIN("decode_link_nodes");

    char *kb_name = NULL;
    kb_link_pair_t *pairs = NULL;
    int pair_count = 0;

    ASSERT_OK(kb_search_decode_link_nodes(
        "kb_main.uuid1.parent.uuid2.child",
        &kb_name, &pairs, &pair_count),
        "decode link path");

    ASSERT_EQ_STR(kb_name, "kb_main", "kb_name == kb_main");
    ASSERT_EQ_INT(pair_count, 2, "2 link pairs");

    if (pair_count >= 2) {
        ASSERT_EQ_STR(pairs[0].link, "uuid1", "pair[0].link");
        ASSERT_EQ_STR(pairs[0].name, "parent", "pair[0].name");
        ASSERT_EQ_STR(pairs[1].link, "uuid2", "pair[1].link");
        ASSERT_EQ_STR(pairs[1].name, "child", "pair[1].name");
    }

    free(kb_name);
    kb_link_pairs_free(pairs, pair_count);
}

int main(void)
{
    printf("=== KB_Search Unit Tests ===\n");

    sqlite3 *db = create_test_db();
    if (!db) {
        printf("FATAL: Cannot create test database\n");
        return 1;
    }

    test_no_filters(db);
    test_label_filter(db);
    test_combined_filters(db);
    test_name_filter(db);
    test_has_link_filter(db);
    test_find_path_values(db);
    test_decode_link_nodes();

    sqlite3_close(db);

    TEST_END();
}
