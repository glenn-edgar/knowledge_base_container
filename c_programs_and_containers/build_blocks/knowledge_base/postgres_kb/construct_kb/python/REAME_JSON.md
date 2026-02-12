# PostgreSQL JSONB Operators - Complete Reference

## Overview

This guide covers all mainstream PostgreSQL JSONB operators and their implementation in the `LTreeJsonDB` class.

---

## Core JSONB Operators

### 1. Field Access Operators

#### `->` - Get JSON Field (returns JSON)
```python
# Get value as JSON
name = db.jsonb_get(ltree_path, "name", as_text=False)
# Returns: JSON value (could be object, array, string, etc.)
```

**SQL Equivalent:**
```sql
SELECT data->'name' FROM table;
```

#### `->>` - Get Text Value (returns text)
```python
# Get value as text string
name = db.jsonb_get(ltree_path, "name", as_text=True)
# Returns: string value
```

**SQL Equivalent:**
```sql
SELECT data->>'name' FROM table;
```

#### `#>` - Get Nested JSON Path (returns JSON)
```python
# Access nested path
city = db.jsonb_get(ltree_path, "address.city", as_text=False)
```

**SQL Equivalent:**
```sql
SELECT data#>'{address,city}' FROM table;
```

#### `#>>` - Get Nested Text Value (returns text)
```python
# Access nested path as text
city = db.jsonb_get(ltree_path, "address.city", as_text=True)
```

**SQL Equivalent:**
```sql
SELECT data#>>'{address,city}' FROM table;
```

---

## 2. Modification Operators

### `-` - Remove Key
```python
# Remove a top-level key
db.jsonb_delete_key(ltree_path, "password")
```

**SQL Equivalent:**
```sql
UPDATE table SET data = data - 'password';
```

### `#-` - Remove Nested Path
```python
# Remove nested path
db.jsonb_delete_path(ltree_path, "address.city")
```

**SQL Equivalent:**
```sql
UPDATE table SET data = data #- '{address,city}';
```

### `jsonb_set()` - Replace/Insert at Path
```python
# Set or create value at path
db.jsonb_set(ltree_path, "address.city", "LA")
```

**SQL Equivalent:**
```sql
UPDATE table SET data = jsonb_set(data, '{address,city}', '"LA"');
```

---

## 3. Existence & Search Operators

### `?` - Has Key
```python
# Check if key exists
has_role = db.jsonb_has_key(ltree_path, "role")
# Returns: True/False
```

**SQL Equivalent:**
```sql
SELECT data ? 'role' FROM table;
```

### `?|` - Has Any Keys
```python
# Check if any of the keys exist
has_any = db.jsonb_has_any_keys(ltree_path, ["role", "admin", "user"])
# Returns: True if ANY key exists
```

**SQL Equivalent:**
```sql
SELECT data ?| ARRAY['role', 'admin', 'user'] FROM table;
```

### `?&` - Has All Keys
```python
# Check if all keys exist
has_all = db.jsonb_has_all_keys(ltree_path, ["name", "role", "email"])
# Returns: True only if ALL keys exist
```

**SQL Equivalent:**
```sql
SELECT data ?& ARRAY['name', 'role', 'email'] FROM table;
```

---

## 4. Containment Operators

### `@>` - Contains Object
```python
# Check if data contains the specified object
contains = db.jsonb_contains(ltree_path, {"role": "admin"})
# Returns: True if data contains {"role": "admin"}
```

**SQL Equivalent:**
```sql
SELECT * FROM table WHERE data @> '{"role": "admin"}';
```

**Use Case:** Finding documents with specific properties
```python
# Find all admin users
is_admin = db.jsonb_contains("root.users.john", {"role": "admin"})

# Find documents with nested properties
has_la_address = db.jsonb_contains("root.users.john", {
    "address": {"city": "LA"}
})
```

### `<@` - Is Contained By
```python
# Check if data is contained by larger object
contained = db.jsonb_contained_by(ltree_path, {
    "name": "Test",
    "role": "admin",
    "extra": "fields"
})
# Returns: True if ALL data fields are in the specified object
```

**SQL Equivalent:**
```sql
SELECT * FROM table WHERE data <@ '{"name": "Test", "role": "admin", "extra": "fields"}';
```

---

## 5. Array Operations

### Array Contains (`@>`)
```python
# Check if array contains specific element
has_tag = db.jsonb_array_contains(ltree_path, "tags", "python")
# Returns: True if "python" is in the tags array
```

**SQL Equivalent:**
```sql
SELECT * FROM table WHERE data->'tags' @> '["python"]';
```

### `jsonb_array_elements()` - Expand Array
```python
# Expand array to list of elements
elements = db.jsonb_array_elements(ltree_path, "tags")
# Returns: ["python", "postgres", "redis"]
```

**SQL Equivalent:**
```sql
SELECT jsonb_array_elements(data->'tags') FROM table;
```

### Array Manipulation
```python
# Append to array
db.jsonb_array_append(ltree_path, "tags", "redis")

# Prepend to array
db.jsonb_array_prepend(ltree_path, "tags", "docker")

# Remove by index
removed = db.jsonb_array_remove_index(ltree_path, "tags", 0)
```

**SQL Equivalent:**
```sql
-- Append
UPDATE table SET data = jsonb_set(
    data, 
    '{tags}', 
    (data->'tags') || '["redis"]'
);

-- Remove element at index
UPDATE table SET data = jsonb_set(
    data,
    '{tags}',
    (data->'tags') - 0
);
```

---

## 6. JSON Path Queries (SQL/JSON Standard)

### `jsonb_path_exists()` - Check Path
```python
# Check if path matches condition
exists = db.jsonb_path_exists(ltree_path, '$.age ? (@ > 30)')
# Returns: True if age > 30
```

**SQL Equivalent:**
```sql
SELECT jsonb_path_exists(data, '$.age ? (@ > 30)') FROM table;
```

**Common Path Patterns:**
```python
# Simple existence
'$.role'

# Comparison
'$.age ? (@ > 30)'
'$.price ? (@ < 100)'

# String matching
'$.name ? (@ like_regex "John.*")'

# Array indexing
'$.items[0].price'

# Wildcard
'$.items[*].name'
```

### `jsonb_path_query_array()` - Extract Values
```python
# Extract all matching values as array
results = db.jsonb_path_query(ltree_path, '$.items[*].price')
# Returns: [10.99, 20.50, 15.75]
```

**SQL Equivalent:**
```sql
SELECT jsonb_path_query_array(data, '$.items[*].price') FROM table;
```

---

## Complete Usage Examples

### Example 1: User Management

```python
db = LTreeJsonDB(conn)

# Create user structure (assume document exists)
user_path = "root.users.john_doe"

# Check if admin
is_admin = db.jsonb_contains(user_path, {"role": "admin"})

# Get email as text
email = db.jsonb_get(user_path, "email", as_text=True)

# Check required fields exist
has_required = db.jsonb_has_all_keys(user_path, ["name", "email", "role"])

# Add a tag
db.jsonb_array_append(user_path, "tags", "premium")

# Check if user has tag
has_premium = db.jsonb_array_contains(user_path, "tags", "premium")

# Update nested address
db.jsonb_set(user_path, "address.city", "San Francisco")

# Remove sensitive data
db.jsonb_delete_key(user_path, "password_hash")
```

### Example 2: Product Search

```python
product_path = "root.products.laptop_001"

# Check if in stock
in_stock = db.jsonb_path_exists(product_path, '$.inventory ? (@ > 0)')

# Get all prices from variants
prices = db.jsonb_path_query(product_path, '$.variants[*].price')

# Check if product has specific features
has_features = db.jsonb_contains(product_path, {
    "features": {
        "wifi": True,
        "bluetooth": True
    }
})

# Get all variant names
variants = db.jsonb_array_elements(product_path, "variants")
```

### Example 3: Event Processing Queue

```python
queue_path = "root.queues.events"

# Add event to queue
db.enqueue(queue_path, {
    "event_type": "user.signup",
    "user_id": "12345",
    "timestamp": "2025-01-15T10:30:00Z"
})

# Check queue size
size = db.size(queue_path)

# Process next event
event = db.dequeue(queue_path)

# Check if specific event type in queue
has_signup = db.jsonb_array_contains(
    queue_path, 
    "items",
    {"event_type": "user.signup"}
)

# Get metadata
meta = db.get_metadata(queue_path)
```

### Example 4: Complex Filtering

```python
doc_path = "root.documents.report_2025"

# Multiple containment checks
is_published = db.jsonb_contains(doc_path, {"status": "published"})
has_author = db.jsonb_has_key(doc_path, "author")

# Complex path query
high_priority = db.jsonb_path_exists(
    doc_path, 
    '$.metadata.priority ? (@ == "high")'
)

# Get all section titles
titles = db.jsonb_path_query(doc_path, '$.sections[*].title')

# Check nested structure
has_valid_metadata = db.jsonb_has_all_keys(doc_path, [
    "metadata.created_at",
    "metadata.updated_at",
    "metadata.version"
])
```

---

## Performance Tips

### 1. Indexing Strategies

**GIN Index (General):**
```sql
CREATE INDEX idx_data ON table USING gin (data);
```

**GIN + jsonb_path_ops (Containment Only):**
```sql
CREATE INDEX idx_data_path ON table USING gin (data jsonb_path_ops);
```

**BTREE on Extracted Field:**
```sql
CREATE INDEX idx_email ON table((data->>'email'));
```

### 2. Query Optimization

**Good:**
```python
# Use containment for exact matches
is_admin = db.jsonb_contains(path, {"role": "admin"})
```

**Better for text search:**
```python
# Use path queries for complex conditions
matches = db.jsonb_path_exists(path, '$.email ? (@ like_regex ".*@company.com")')
```

### 3. Array Operations

- Use `@>` for containment checks (fast with GIN index)
- Use `jsonb_array_elements()` sparingly (expands to rows)
- Consider `jsonb_path_query()` for complex array filtering

---

## Comparison: Operators vs Methods

| Operation | Raw SQL | LTreeJsonDB Method |
|-----------|---------|-------------------|
| Get field | `data->'name'` | `jsonb_get(path, "name")` |
| Get text | `data->>'email'` | `jsonb_get(path, "email", as_text=True)` |
| Has key | `data ? 'role'` | `jsonb_has_key(path, "role")` |
| Contains | `data @> '{"role":"admin"}'` | `jsonb_contains(path, {"role":"admin"})` |
| Array contains | `data->'tags' @> '["python"]'` | `jsonb_array_contains(path, "tags", "python")` |
| Path exists | `jsonb_path_exists(data, '$.age?(@ > 30)')` | `jsonb_path_exists(path, '$.age?(@ > 30)')` |
| Delete key | `data - 'password'` | `jsonb_delete_key(path, "password")` |

---

## Common Patterns

### 1. Safe Navigation
```python
# Check existence before accessing
if db.jsonb_has_key(path, "user"):
    user_name = db.jsonb_get(path, "user.name", as_text=True)
```

### 2. Conditional Updates
```python
# Only update if exists
if db.jsonb_contains(path, {"status": "pending"}):
    db.jsonb_set(path, "status", "processing")
```

### 3. Array Management
```python
# Add only if not present
if not db.jsonb_array_contains(path, "tags", "featured"):
    db.jsonb_array_append(path, "tags", "featured")
```

### 4. Bulk Checks
```python
# Validate document structure
required_fields = ["id", "name", "email", "created_at"]
is_valid = db.jsonb_has_all_keys(path, required_fields)
```

---

## Migration Guide

### From Old `LTreeQueryDB` to New `LTreeJsonDB`

**Old (broken):**
```python
queue_db = LTreeQueryDB(conn)
value = queue_db._jsonb_get(path, "field")  # Private method
```

**New (fixed):**
```python
db = LTreeJsonDB(conn)
value = db.jsonb_get(path, "field")  # Public method with proper operators
```

**Key Improvements:**
1. ✅ Fixed `_jsonb_array_remove_index` bug (separate SELECT then UPDATE)
2. ✅ Added all mainstream JSONB operators (?, ?|, ?&, @>, <@)
3. ✅ Added JSON path queries (jsonb_path_exists, jsonb_path_query_array)
4. ✅ Renamed to `LTreeJsonDB` (more accurate)
5. ✅ Made JSONB operations public API
6. ✅ Better separation: low-level JSONB ops + high-level queue abstractions

---

## Testing

Run the comprehensive test suite:

```bash
python ltree_jsonb_queue.py
```

Tests cover:
- Basic JSONB get operations (-> and ->>)
- Key existence checks (?, ?|, ?&)
- Containment operators (@>, <@)
- Array operations
- JSON path queries
- Queue/Stack operations (FIFO/LIFO)
- Edge cases

---

## References

- [PostgreSQL JSONB Documentation](https://www.postgresql.org/docs/current/datatype-json.html)
- [JSONB Operators](https://www.postgresql.org/docs/current/functions-json.html)
- [JSON Path Queries](https://www.postgresql.org/docs/current/functions-json.html#FUNCTIONS-SQLJSON-PATH)

