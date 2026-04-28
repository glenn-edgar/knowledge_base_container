// libcomm/manifest.c
// See manifest.h for the 8 invariants. No copy: out_data aliases the
// caller's blob, so the blob must remain valid for libcomm's lifetime
// (the rover_*_manifest_bin arrays in comm_manifest_bin.h are static
// const, satisfying that).

#include "manifest.h"

int manifest_dongle_is_host(const uint8_t uuid[16])
{
    for (int i = 0; i < 16; i++) {
        if (uuid[i] != 0) return 0;
    }
    return 1;
}

int manifest_dongle_owns_bus(const manifest_dongle_t *d, uint8_t bus_id)
{
    uint8_t n = d->bus_count;
    if (n > 8) n = 8;
    for (uint8_t i = 0; i < n; i++) {
        if (d->bus_local_ids[i] == bus_id) return 1;
    }
    return 0;
}

comm_result_t manifest_validate(const uint8_t *blob,
                                size_t blob_len,
                                const comm_manifest_v1_wire_t **out_data)
{
    if (!blob || !out_data) return COMM_ERR_BAD_ARG;
    if (blob_len < sizeof(comm_manifest_v1_packet_t)) return COMM_ERR_BAD_MANIFEST;

    const comm_manifest_v1_packet_t *pkt = (const comm_manifest_v1_packet_t *)blob;
    const comm_manifest_v1_wire_t   *m   = comm_manifest_v1_packet_verify(pkt);
    if (!m) return COMM_ERR_BAD_MANIFEST;

    // Invariant 1+3: counts in range.
    if (m->dongle_count < 1 || m->dongle_count > COMM_DONGLES_MAX) return COMM_ERR_BAD_MANIFEST;
    if (m->bus_count   > COMM_BUSES_MAX)                            return COMM_ERR_BAD_MANIFEST;
    if (m->slave_count > COMM_SLAVES_MAX)                           return COMM_ERR_BAD_MANIFEST;

    // Invariant 2 used to require dongles[0] to be HOST_INTERNAL_DONGLE
    // (all-zero uuid). Phase B drops that requirement: every dongle now
    // carries a real (type, instance) identity. The all-zero sentinel
    // is still a valid value (legacy inproc-mode tests depend on that)
    // — it's just no longer mandatory in slot 0.

    // Each dongle's bus_count must fit and bus_local_ids must be in-range.
    for (uint8_t di = 0; di < m->dongle_count; di++) {
        const manifest_dongle_t *d = &m->dongles[di];
        if (d->bus_count > 8) return COMM_ERR_BAD_MANIFEST;
        for (uint8_t bi = 0; bi < d->bus_count; bi++) {
            if (d->bus_local_ids[bi] >= m->bus_count) return COMM_ERR_BAD_MANIFEST;
        }
    }

    // Invariant 4: per-bus tunables sane.
    for (uint8_t i = 0; i < m->bus_count; i++) {
        const manifest_bus_t *b = &m->buses[i];
        if (b->tunables.tick_period_ms < CT_COMM_RX_PERIOD_MS) return COMM_ERR_BAD_MANIFEST;
        if (b->tunables.max_miss == 0)                          return COMM_ERR_BAD_MANIFEST;
        if (b->tunables.join_timeout_ms == 0)                   return COMM_ERR_BAD_MANIFEST;
    }

    // Invariants 5–8: per-slave.
    for (uint8_t i = 0; i < m->slave_count; i++) {
        const manifest_slave_t *s = &m->slaves[i];

        if (s->mcu == 0)                            return COMM_ERR_BAD_MANIFEST;
        if (s->dongle_idx >= m->dongle_count)       return COMM_ERR_BAD_MANIFEST;
        if (!manifest_dongle_owns_bus(&m->dongles[s->dongle_idx], s->bus_id))
                                                    return COMM_ERR_BAD_MANIFEST;

        int addr_ok = (s->addr >= COMM_ADDR_SLAVE_MIN && s->addr <= COMM_ADDR_SLAVE_MAX)
                   || (s->addr == COMM_ADDR_DONGLE_SELF);
        if (!addr_ok)                               return COMM_ERR_BAD_MANIFEST;

        for (uint8_t j = 0; j < i; j++) {
            if (m->slaves[j].mcu == s->mcu) return COMM_ERR_BAD_MANIFEST;
        }
    }

    *out_data = m;
    return COMM_OK;
}
