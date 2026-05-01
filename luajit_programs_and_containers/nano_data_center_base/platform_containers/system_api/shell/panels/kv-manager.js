// kv-manager.js — Shared KV bucket cache + watcher lifecycle.
//
// Caches bucket handles to avoid duplicate NATS connections.
// Tracks watchers with tokens so panels can release them on destroy().

var _decoder = new TextDecoder();

export class KvManager {
  constructor(js) {
    this._js = js;
    this._buckets = {};
    this._watchers = {};
    this._tokenCounter = 0;
  }

  // Get or create a KV bucket handle (cached).
  async bucket(name) {
    if (!this._buckets[name]) {
      this._buckets[name] = await this._js.views.kv(name);
    }
    return this._buckets[name];
  }

  // Get a single key's value (parsed JSON), or null.
  async get(bucketName, key) {
    var kv = await this.bucket(bucketName);
    var entry = await kv.get(key);
    if (!entry || !entry.value || entry.value.length === 0) return null;
    return JSON.parse(_decoder.decode(entry.value));
  }

  // Get first key matching a prefix. Returns key string or null.
  async firstKeyWithPrefix(bucketName, prefix) {
    var kv = await this.bucket(bucketName);
    var iter = await kv.keys();
    for await (var key of iter) {
      if (key.indexOf(prefix) === 0) return key;
    }
    return null;
  }

  // Get all keys in a bucket. Returns array of strings.
  async keys(bucketName) {
    var kv = await this.bucket(bucketName);
    var result = [];
    var iter = await kv.keys();
    for await (var key of iter) {
      result.push(key);
    }
    return result;
  }

  // Load all current values from a bucket, calling handler(key, value) for each.
  // Collects keys first, then fetches values — avoids iterator/get interleaving issues.
  async loadAll(bucketName, handler) {
    var kv = await this.bucket(bucketName);
    var allKeys = [];
    var iter = await kv.keys();
    for await (var key of iter) {
      allKeys.push(key);
    }
    for (var i = 0; i < allKeys.length; i++) {
      var entry = await kv.get(allKeys[i]);
      if (entry && entry.value && entry.value.length > 0) {
        try {
          var val = JSON.parse(_decoder.decode(entry.value));
          handler(allKeys[i], val);
        } catch (e) { /* skip non-JSON */ }
      }
    }
  }

  // Get history for a single key. Returns array of parsed values (newest first).
  async history(bucketName, key) {
    var kv = await this.bucket(bucketName);
    var result = [];
    var iter = await kv.history({ key: key });
    for await (var entry of iter) {
      if (entry.value && entry.value.length > 0) {
        try {
          result.push(JSON.parse(_decoder.decode(entry.value)));
        } catch (e) { /* skip */ }
      }
    }
    return result;
  }

  // Start a KV watch. Returns a token for later cleanup.
  async watch(bucketName, handler, opts) {
    var kv = await this.bucket(bucketName);
    var token = ++this._tokenCounter;
    var cancelled = false;
    var watch = await kv.watch(opts || { include: "updates" });

    (async function () {
      for await (var entry of watch) {
        if (cancelled) break;
        if (entry.value && entry.value.length > 0) {
          try {
            var val = JSON.parse(_decoder.decode(entry.value));
            handler(entry.key, val);
          } catch (e) { /* skip non-JSON */ }
        }
      }
    })();

    this._watchers[token] = { cancel: function () { cancelled = true; } };
    return token;
  }

  // Stop a specific watcher.
  release(token) {
    var w = this._watchers[token];
    if (w) {
      w.cancel();
      delete this._watchers[token];
    }
  }

  // Stop all watchers.
  releaseAll() {
    for (var t in this._watchers) {
      this._watchers[t].cancel();
    }
    this._watchers = {};
  }
}
