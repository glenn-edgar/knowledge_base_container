Here’s a solid **rule set for YAML** — think of it like a practical cheat sheet / style guide.

---

# 📘 YAML Rules

## 1. **General Structure**

* YAML is **indentation-based** (like Python).
* Use **spaces only** — **no tabs**.
* Standard indentation: **2 spaces** (sometimes 4).
* Root level has no indentation.

✅ Example:

```yaml
person:
  name: Glenn
  age: 71
```

---

## 2. **Scalars (Basic Values)**

* Strings can be written:

  * Without quotes (barewords)
  * With single quotes `'` (no escape processing)
  * With double quotes `"` (supports escaping like `\n`)

✅ Examples:

```yaml
city: Paris
quote1: 'It''s YAML, not JSON'   # single quotes double up ''
quote2: "New\nLine"              # double quotes allow escapes
```

---

## 3. **Lists (Sequences)**

* Use `-` for list items (indented under the key).

✅ Example:

```yaml
hobbies:
  - reading
  - coding
  - hiking
```

---

## 4. **Dictionaries (Mappings)**

* Use `key: value` pairs.
* Keys must be unique at the same level.

✅ Example:

```yaml
server:
  host: localhost
  port: 8080
```

---

## 5. **Booleans, Nulls, and Numbers**

* YAML supports flexible boolean values: `true/false`, `yes/no`, `on/off`.
* Null values can be written as `null`, `~`, or empty.
* Numbers can be written as plain digits, hex, octal, or floats.

✅ Example:

```yaml
active: yes
disabled: no
nothing: null
nothing2: ~
count: 42
pi: 3.14159
```

---

## 6. **Comments**

* Start with `#`.
* Can appear alone or at end of a line.

✅ Example:

```yaml
# A comment line
name: Glenn  # inline comment
```

---

## 7. **Multi-line Strings**

* Use `|` for **literal block** (preserve newlines).
* Use `>` for **folded block** (folds lines into spaces).

✅ Example:

```yaml
bio_literal: |
  Line 1
  Line 2
  Line 3

bio_folded: >
  Line 1
  Line 2
  Line 3
```

---

## 8. **Anchors and Aliases**

* `&anchor` defines a block once.
* `*alias` reuses it.
* `<<: *alias` merges mapping content.

✅ Example:

```yaml
defaults: &defaults
  retries: 3
  timeout: 30

service1:
  <<: *defaults
  url: https://example.com
```

---

## 9. **Multiple Documents**

* Separate multiple YAML documents with `---`.
* Optionally end with `...`.

✅ Example:

```yaml
---
name: Glenn
role: Developer
---
name: Alice
role: Analyst
...
```

---

## 10. **Quoting Rules**

* Quote strings if they contain:

  * Special characters (`:`, `{}`, `[]`, `,`, `#`, `&`, `*`)
  * Leading/trailing spaces
  * Reserved words (`yes`, `no`, `on`, `off`) if you don’t want them parsed as booleans.

✅ Example:

```yaml
literal_colon: "http://example.com:80"
```

---

⚖️ **Summary of YAML Rules**

* Indentation defines structure (spaces only).
* `key: value` for mappings.
* `-` for lists.
* `#` for comments.
* `|` and `>` for multi-line text.
* `&` and `*` for reusing data.
* `---` separates documents.
* Quote strings when needed.

---

Would you like me to also make a **“wrong vs right” YAML rules table** (bad syntax vs good syntax), so you have a quick error-prevention guide?
