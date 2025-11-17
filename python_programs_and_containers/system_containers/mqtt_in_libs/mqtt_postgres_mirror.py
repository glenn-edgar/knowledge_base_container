import re
from typing import Optional, Dict, Any

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection


class MqttPostgresMirror:
    """
    Mirror MQTT publishes into Postgres using:
      <base>_state   : latest key-value per topic
      <base>_history : fixed-size circular log (ring buffer)

    Usage:
        mirror = MqttPostgresMirror(conn, base="mqtt", log_size=100000)
        mirror.ensure_schema()         # creates/adjusts schema and ring size
        mirror.handle_message(
            topic=msg.topic,
            payload=msg.payload,       # bytes
            qos=msg.qos,
            retain=bool(msg.retain),
            props=None,                # dict if you want MQTT5 props
            client_id="sensor01",
            packet_id=getattr(msg, "mid", None),
            content_type="application/json"
        )
    """

    _BASE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def __init__(self, conn: PGConnection, base: str, log_size: int):
        if not isinstance(conn, PGConnection.__mro__[0]):  # duck-type-ish guard
            # We only require it looks like a psycopg2 connection (has cursor()).
            # If you pass a wrapper, it must expose .cursor() and .commit().
            pass
        if not self._BASE_NAME_RE.match(base):
            raise ValueError("Invalid base name. Use letters, numbers, and underscores; cannot start with a number.")
        if log_size is None or int(log_size) < 1:
            raise ValueError("log_size must be >= 1")

        self.conn: PGConnection = conn
        self.base = base
        self.log_size = int(log_size)

        # Pre-compose identifiers
        self._ident = {
            "base": sql.SQL(self.base),
            "state": sql.Identifier(f"{self.base}_state"),
            "history": sql.Identifier(f"{self.base}_history"),
            "meta": sql.Identifier(f"{self.base}_meta"),
            "cfg": sql.Identifier(f"{self.base}_cfg"),
            "seq": sql.Identifier(f"{self.base}_history_seq"),
            "to_ltree": sql.Identifier(f"{self.base}_to_ltree"),
            "set_log_size": sql.Identifier(f"{self.base}_set_log_size"),
            "history_put": sql.Identifier(f"{self.base}_history_put"),
            "state_upsert_from_ring": sql.Identifier(f"{self.base}_state_upsert_from_ring"),
            "log_and_state": sql.Identifier(f"{self.base}_log_and_state"),
        }

    # ---------------------------
    # Public API
    # ---------------------------

    def ensure_schema(self) -> None:
        """Create/upgrade schema and preallocate ring to self.log_size."""
        with self.conn, self.conn.cursor() as cur:
            # 0) ltree
            cur.execute("CREATE EXTENSION IF NOT EXISTS ltree;")

            # 1) meta + cfg view
            cur.execute(self._ddl_meta_cfg())

            # 2) state
            cur.execute(self._ddl_state())

            # 3) sequence + history (ring)
            cur.execute(self._ddl_history_seq())
            cur.execute(self._ddl_history())

            # 4) helper: topic -> ltree
            cur.execute(self._ddl_to_ltree_fn())

            # 5) ring management
            cur.execute(self._ddl_set_log_size_fn())

            # 6) ring insert + state upsert
            cur.execute(self._ddl_history_put_fn())
            cur.execute(self._ddl_state_upsert_from_ring_fn())
            cur.execute(self._ddl_log_and_state_fn())

            # 7) (re)size ring & preallocate
            cur.execute(
                sql.SQL("SELECT {}(%s);").format(self._ident["set_log_size"]),
                (self.log_size,),
            )

    def handle_message(
        self,
        *,
        topic: str,
        payload: bytes,
        qos: int = 0,
        retain: bool = False,
        props: Optional[Dict[str, Any]] = None,
        client_id: Optional[str] = None,
        packet_id: Optional[int] = None,
        content_type: Optional[str] = None,
    ) -> int:
        """
        Store one MQTT publish into the ring and update latest state.
        Returns the ring slot_id used.

        - If payload is valid UTF-8 -> stored as TEXT, else as BYTEA.
        - props: pass a dict; will be stored as JSONB if provided.
        """
        if topic is None or topic == "":
            raise ValueError("topic must be a non-empty string")

        payload_text: Optional[str] = None
        payload_bytes: Optional[bytes] = None

        # auto-detect UTF-8
        if payload is None:
            # allow empty payload: TEXT empty string is a valid 'delete' semantic on your side if desired
            payload_text = ""
        else:
            try:
                payload_text = payload.decode("utf-8")
            except Exception:
                payload_bytes = payload

        with self.conn, self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT {}(%s,%s,%s,%s,%s,%s,%s,%s,%s);").format(
                    self._ident["log_and_state"]
                ),
                (
                    topic,
                    payload_text,
                    payload_bytes,
                    content_type,
                    qos,
                    retain,
                    psycopg2.extras.Json(props) if props is not None else None,
                    client_id,
                    packet_id,
                ),
            )
            slot_id = cur.fetchone()[0]
            return slot_id

    def set_log_size(self, new_size: int) -> None:
        """Resize the ring and preallocate to exactly new_size slots."""
        if new_size is None or int(new_size) < 1:
            raise ValueError("new_size must be >= 1")
        self.log_size = int(new_size)
        with self.conn, self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT {}(%s);").format(self._ident["set_log_size"]),
                (self.log_size,),
            )

    # ---------------------------
    # DDL builders (private)
    # ---------------------------

    def _ddl_meta_cfg(self) -> sql.SQL:
        return sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {meta} (
              k TEXT PRIMARY KEY,
              v JSONB NOT NULL
            );
            CREATE OR REPLACE VIEW {cfg} AS
            SELECT (v->>'log_size')::INT AS log_size
            FROM {meta} WHERE k = 'ring';
            """
        ).format(meta=self._ident["meta"], cfg=self._ident["cfg"])

    def _ddl_state(self) -> sql.SQL:
        return sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {state} (
              topic           TEXT PRIMARY KEY,
              topic_path      LTREE,
              payload_text    TEXT,
              payload_bytes   BYTEA,
              content_type    TEXT DEFAULT 'text/plain',
              qos             SMALLINT NOT NULL DEFAULT 0,
              retain          BOOLEAN  NOT NULL DEFAULT TRUE,
              props           JSONB,
              updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
              last_slot_id    INTEGER
            );
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = '{base}_state_payload_ck'
              ) THEN
                ALTER TABLE {state}
                  ADD CONSTRAINT {base}_state_payload_ck
                  CHECK (payload_text IS NOT NULL OR payload_bytes IS NOT NULL);
              END IF;
            END$$;
            CREATE INDEX IF NOT EXISTS {base}_state_path_gist
              ON {state} USING GIST (topic_path);
            CREATE INDEX IF NOT EXISTS {base}_state_updated_at_idx
              ON {state} (updated_at DESC);
            """
        ).format(
            state=self._ident["state"],
            base=sql.Identifier(self.base),
        )

    def _ddl_history_seq(self) -> sql.SQL:
        return sql.SQL(
            "CREATE SEQUENCE IF NOT EXISTS {seq};"
        ).format(seq=self._ident["seq"])

    def _ddl_history(self) -> sql.SQL:
        return sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {history} (
              slot_id        INTEGER PRIMARY KEY,
              received_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
              topic          TEXT NOT NULL,
              topic_path     LTREE,
              payload_text   TEXT,
              payload_bytes  BYTEA,
              content_type   TEXT DEFAULT 'text/plain',
              qos            SMALLINT NOT NULL DEFAULT 0,
              retain         BOOLEAN  NOT NULL,
              props          JSONB,
              client_id      TEXT,
              packet_id      INTEGER
            );
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = '{base}_history_payload_ck'
              ) THEN
                ALTER TABLE {history}
                  ADD CONSTRAINT {base}_history_payload_ck
                  CHECK (payload_text IS NOT NULL OR payload_bytes IS NOT NULL);
              END IF;
            END$$;
            CREATE INDEX IF NOT EXISTS {base}_history_path_gist
              ON {history} USING GIST (topic_path);
            CREATE INDEX IF NOT EXISTS {base}_history_received_at_idx
              ON {history} (received_at DESC);
            """
        ).format(
            history=self._ident["history"],
            base=sql.Identifier(self.base),
        )

    def _ddl_to_ltree_fn(self) -> sql.SQL:
        return sql.SQL(
            """
            CREATE OR REPLACE FUNCTION {to_ltree}(p_topic text)
            RETURNS ltree LANGUAGE SQL IMMUTABLE AS $$
              SELECT replace(regexp_replace(p_topic, '[^A-Za-z0-9/]+', '_', 'g'), '/', '.')::ltree;
            $$;
            """
        ).format(to_ltree=self._ident["to_ltree"])

    def _ddl_set_log_size_fn(self) -> sql.SQL:
        return sql.SQL(
            """
            CREATE OR REPLACE FUNCTION {fn}(p_size int)
            RETURNS void
            LANGUAGE plpgsql AS $$
            BEGIN
              IF p_size IS NULL OR p_size < 1 THEN
                RAISE EXCEPTION 'log_size must be >= 1';
              END IF;

              INSERT INTO {meta}(k, v)
              VALUES ('ring', jsonb_build_object('log_size', p_size))
              ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v;

              DELETE FROM {history} h WHERE h.slot_id >= p_size;

              INSERT INTO {history}(slot_id, received_at, topic, topic_path, retain)
              SELECT s, now(), '<empty>'::text, NULL::ltree, false
              FROM generate_series(0, p_size-1) AS g(s)
              WHERE NOT EXISTS (SELECT 1 FROM {history} h2 WHERE h2.slot_id = g.s);

              PERFORM setval({seq}::regclass, 0, false);
            END$$;
            """
        ).format(
            fn=self._ident["set_log_size"],
            meta=self._ident["meta"],
            history=self._ident["history"],
            seq=self._ident["seq"],
        )

    def _ddl_history_put_fn(self) -> sql.SQL:
        return sql.SQL(
            """
            CREATE OR REPLACE FUNCTION {fn}(
              p_topic        text,
              p_payload_text text,
              p_payload_bytes bytea,
              p_content_type text,
              p_qos          smallint,
              p_retain       boolean,
              p_props        jsonb,
              p_client_id    text,
              p_packet_id    integer,
              OUT o_slot_id  integer
            ) RETURNS integer
            LANGUAGE plpgsql AS $$
            DECLARE
              v_size int;
              v_slot int;
            BEGIN
              SELECT (v->>'log_size')::int INTO v_size FROM {meta} WHERE k = 'ring';
              IF v_size IS NULL OR v_size < 1 THEN
                RAISE EXCEPTION 'Ring size not initialized. Call {set_log_size}(int).';
              END IF;

              v_slot := (nextval({seq})::bigint % v_size)::int;

              UPDATE {history}
                 SET received_at   = now(),
                     topic         = p_topic,
                     topic_path    = {to_ltree}(p_topic),
                     payload_text  = p_payload_text,
                     payload_bytes = p_payload_bytes,
                     content_type  = COALESCE(p_content_type, 'text/plain'),
                     qos           = COALESCE(p_qos, 0),
                     retain        = COALESCE(p_retain, false),
                     props         = p_props,
                     client_id     = p_client_id,
                     packet_id     = p_packet_id
               WHERE slot_id = v_slot;

              o_slot_id := v_slot;
              RETURN;
            END$$;
            """
        ).format(
            fn=self._ident["history_put"],
            meta=self._ident["meta"],
            history=self._ident["history"],
            seq=self._ident["seq"],
            to_ltree=self._ident["to_ltree"],
            set_log_size=self._ident["set_log_size"],
        )

    def _ddl_state_upsert_from_ring_fn(self) -> sql.SQL:
        return sql.SQL(
            """
            CREATE OR REPLACE FUNCTION {fn}(
              p_slot_id int,
              p_topic   text,
              p_payload_text text,
              p_payload_bytes bytea,
              p_content_type text,
              p_qos smallint,
              p_retain boolean,
              p_props jsonb
            ) RETURNS void
            LANGUAGE SQL AS $$
              INSERT INTO {state}(topic, topic_path, payload_text, payload_bytes,
                                  content_type, qos, retain, props, updated_at, last_slot_id)
              VALUES (
                p_topic,
                {to_ltree}(p_topic),
                p_payload_text,
                p_payload_bytes,
                COALESCE(p_content_type, 'text/plain'),
                COALESCE(p_qos, 0),
                COALESCE(p_retain, false),
                p_props,
                now(),
                p_slot_id
              )
              ON CONFLICT (topic) DO UPDATE
              SET topic_path   = EXCLUDED.topic_path,
                  payload_text = EXCLUDED.payload_text,
                  payload_bytes= EXCLUDED.payload_bytes,
                  content_type = EXCLUDED.content_type,
                  qos          = EXCLUDED.qos,
                  retain       = EXCLUDED.retain,
                  props        = EXCLUDED.props,
                  updated_at   = EXCLUDED.updated_at,
                  last_slot_id = EXCLUDED.last_slot_id;
            $$;
            """
        ).format(
            fn=self._ident["state_upsert_from_ring"],
            state=self._ident["state"],
            to_ltree=self._ident["to_ltree"],
        )

    def _ddl_log_and_state_fn(self) -> sql.SQL:
        return sql.SQL(
            """
            CREATE OR REPLACE FUNCTION {fn}(
              p_topic        text,
              p_payload_text text,
              p_payload_bytes bytea,
              p_content_type text,
              p_qos          smallint,
              p_retain       boolean,
              p_props        jsonb,
              p_client_id    text,
              p_packet_id    integer
            ) RETURNS integer
            LANGUAGE plpgsql AS $$
            DECLARE
              v_slot int;
            BEGIN
              v_slot := {history_put}(p_topic, p_payload_text, p_payload_bytes,
                                      p_content_type, p_qos, p_retain, p_props,
                                      p_client_id, p_packet_id);
              PERFORM {state_upsert}(v_slot, p_topic, p_payload_text, p_payload_bytes,
                                     p_content_type, p_qos, p_retain, p_props);
              RETURN v_slot;
            END$$;
            """
        ).format(
            fn=self._ident["log_and_state"],
            history_put=self._ident["history_put"],
            state_upsert=self._ident["state_upsert_from_ring"],
        )
