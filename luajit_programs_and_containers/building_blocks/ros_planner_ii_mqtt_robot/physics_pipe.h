/*
 * physics_pipe.h -- Bidirectional FIFO pipe between the soft real-time
 * ChainTree and the hard real-time physics inner loop.
 *
 * Two SPSC ring buffers, fixed-size memory blocks, lock-free with
 * head/tail counters. Storage lives inside phys_t (allocated by
 * phys_create); no singletons.
 *
 * Fault model: fail-stop. Every error is FAULT_* and halts the system
 * until CMD_RESET is received.
 *
 * Transport portability: in-process today (FFI), shared-memory next,
 * SLIP-framed serial later. The C side never knows which transport
 * carries the bytes.
 *
 * See docs/architecture/pipe.md for the full spec.
 */

#ifndef PHYSICS_PIPE_H
#define PHYSICS_PIPE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ====================================================================== */
/* Sizes                                                                  */
/* ====================================================================== */

#define CMD_BLOCK_BYTES   64
#define UP_BLOCK_BYTES    256
#define CMD_FIFO_DEPTH    32       /* segment-ahead buffer */
#define UP_FIFO_DEPTH     64       /* events + telemetry frames */

/* ====================================================================== */
/* Cmd block (ChainTree -> physics)                                       */
/* ====================================================================== */

enum {
    CMD_PUSH_LINE        = 1,
    CMD_PUSH_SPLINE      = 2,
    CMD_PUSH_ROTATE      = 3,

    CMD_REQUEST_STOP     = 10,
    CMD_RELEASE_STOP     = 11,
    CMD_ABORT_PATH       = 12,

    CMD_BEGIN_TOOL_MOVE  = 20,
    CMD_BEGIN_GRIP       = 21,
    CMD_BEGIN_RELEASE    = 22,
    CMD_BEGIN_DOCK       = 23,
    CMD_BEGIN_CHARGE     = 24,

    CMD_SET_PARAM        = 30,
    CMD_SET_TELEM_RATE   = 31,
    CMD_SET_TELEM_FIELDS = 32,

    CMD_HEARTBEAT        = 99,
    CMD_RESET            = 200
};

typedef struct {
    float    fx, fy, tx, ty;
    float    h_from, h_to, speed;
    uint32_t seg_id;
} cmd_push_seg_t;       /* 32 bytes */

typedef struct {
    float    from_h, to_h, rate;
    uint32_t seg_id;
} cmd_push_rotate_t;    /* 16 bytes */

typedef struct {
    int32_t  slot;
    float    target;
    float    speed;
} cmd_tool_move_t;      /* 12 bytes */

typedef struct {
    int32_t slot;
} cmd_tool_slot_t;      /* 4 bytes */

typedef struct {
    int32_t slot;
    float   target_j;
} cmd_charge_t;         /* 8 bytes */

typedef struct {
    uint32_t param_id;
    float    value;
} cmd_param_t;          /* 8 bytes */

typedef struct {
    uint32_t telem_period_inner_steps;
} cmd_telem_rate_t;     /* 4 bytes */

typedef struct {
    uint64_t field_mask;
} cmd_telem_fields_t;   /* 8 bytes */

typedef struct {
    uint16_t kind;          /* CMD_*                                  */
    uint16_t reserved;
    uint32_t seq;           /* monotonic, ChainTree-assigned          */
    uint8_t  payload[CMD_BLOCK_BYTES - 8];
} cmd_block_t;

#ifdef __STDC_VERSION__
#if __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(cmd_block_t) == CMD_BLOCK_BYTES,
               "cmd_block_t must be exactly CMD_BLOCK_BYTES");
#endif
#endif

/* ====================================================================== */
/* Up block (physics -> ChainTree)                                        */
/* ====================================================================== */

enum {
    UP_TELEM        = 1,
    EVT_CMD_ACK     = 100,
    EVT_SEG_DONE    = 101,
    EVT_TOOL_DONE   = 102,
    EVT_FAULT       = 200
};

/* Telemetry field-group bits */
enum {
    TF_TIME       = 1u << 0,
    TF_POSE_TRUTH = 1u << 1,
    TF_POSE_NOISY = 1u << 2,
    TF_VELOCITY   = 1u << 3,
    TF_WHEELS     = 1u << 4,
    TF_ACTUATORS  = 1u << 5,
    TF_PATH       = 1u << 6,
    TF_ENERGY     = 1u << 7,
    TF_TOOLS      = 1u << 8,
    TF_FAULT      = 1u << 9
};

#define TF_DEFAULT_MASK (TF_TIME | TF_POSE_NOISY | TF_VELOCITY | \
                         TF_PATH | TF_ENERGY | TF_TOOLS | TF_FAULT)

/* Fault codes */
enum {
    FAULT_NONE                 = 0,
    FAULT_PATH_CROSS_TRACK     = 1,
    FAULT_TOOL                 = 2,
    FAULT_CYCLE_OVERRUN        = 3,
    FAULT_CONTROLLER_SILENT    = 4,
    FAULT_UP_OVERFLOW          = 5,
    FAULT_CMD_OVERFLOW         = 6,
    FAULT_PHYSICS_SILENT       = 7,    /* raised by ChainTree side       */
    FAULT_INVALID_CMD          = 8
};

typedef struct {
    uint32_t cycle_us;
    uint32_t cycles_since;

    /* truth pose */
    float    x, y, heading;
    /* noisy pose */
    float    x_n, y_n, heading_n;
    /* body velocity */
    float    v, omega;

    /* wheels */
    float    v_l, v_r;
    float    v_l_target, v_r_target;

    /* actuators */
    float    torque_l, torque_r;
    float    current_l, current_r;

    /* path */
    uint32_t flags;
    uint32_t active_seg_id;
    uint32_t last_done_seg_id;
    int32_t  queue_depth;
    float    active_progress;
    float    cross_track_err;
    float    heading_err;
    float    energy_used_total;
    float    v_cmd;

    /* power */
    float    battery_j;
    float    battery_v;

    /* tools */
    struct {
        uint32_t flags;
        int32_t  kind;
        float    value;
        float    target;
    } tools[8];

    /* health */
    uint32_t fault_word;
} telem_payload_t;

typedef struct {
    uint64_t        field_mask;     /* echoes the active mask           */
    telem_payload_t p;
} up_telem_t;

typedef struct {
    uint32_t seq;                   /* echoes cmd_block_t.seq           */
} up_cmd_ack_t;

typedef struct {
    uint32_t seg_id;
    float    energy_at_complete;
} up_seg_done_t;

typedef struct {
    int32_t  slot;
    uint32_t flags_final;
    float    value_final;
} up_tool_done_t;

typedef struct {
    uint32_t fault_code;
    uint32_t sim_t_us;
    uint32_t ctx0;
    uint32_t ctx1;
} up_fault_t;

typedef struct {
    uint16_t kind;          /* UP_TELEM | EVT_*                         */
    uint16_t reserved;
    uint32_t sim_t_us;
    uint8_t  payload[UP_BLOCK_BYTES - 8];
} up_block_t;

#ifdef __STDC_VERSION__
#if __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(up_block_t) == UP_BLOCK_BYTES,
               "up_block_t must be exactly UP_BLOCK_BYTES");
_Static_assert(sizeof(up_telem_t) <= UP_BLOCK_BYTES - 8,
               "up_telem_t must fit in up_block_t.payload");
#endif
#endif

/* ====================================================================== */
/* SPSC FIFO                                                              */
/* ====================================================================== */

typedef struct {
    cmd_block_t blocks[CMD_FIFO_DEPTH];
    /* Head: written by physics (consumer). Tail: written by ChainTree.  */
    /* Counter wrap is safe: index = counter % depth.                    */
    volatile uint32_t head;
    volatile uint32_t tail;
} cmd_fifo_t;

typedef struct {
    up_block_t blocks[UP_FIFO_DEPTH];
    /* Head: written by ChainTree (consumer). Tail: written by physics.  */
    volatile uint32_t head;
    volatile uint32_t tail;
} up_fifo_t;

/* ====================================================================== */
/* SPSC primitives. All return 1 on success, 0 on full / empty.          */
/* ====================================================================== */

/* Producer: ChainTree side */
int  pipe_cmd_push (cmd_fifo_t *f, const cmd_block_t *in);
/* Consumer: physics side */
int  pipe_cmd_pop  (cmd_fifo_t *f, cmd_block_t *out);
/* Producer: physics side */
int  pipe_up_push  (up_fifo_t *f, const up_block_t *in);
/* Consumer: ChainTree side */
int  pipe_up_pop   (up_fifo_t *f, up_block_t *out);

/* Inspect without dequeue */
uint32_t pipe_cmd_depth(const cmd_fifo_t *f);
uint32_t pipe_up_depth (const up_fifo_t  *f);

/* Drain — used on RESET */
void pipe_cmd_clear(cmd_fifo_t *f);
void pipe_up_clear (up_fifo_t  *f);

/* ====================================================================== */
/* Block builders (host-side helpers; safe-by-construction)               */
/* ====================================================================== */

void cmd_build_push_line  (cmd_block_t *b, uint32_t seq,
                           float fx, float fy, float tx, float ty,
                           float h_from, float h_to, float speed,
                           uint32_t seg_id);
void cmd_build_push_spline(cmd_block_t *b, uint32_t seq,
                           float fx, float fy, float tx, float ty,
                           float h_from, float h_to, float speed,
                           uint32_t seg_id);
void cmd_build_push_rotate(cmd_block_t *b, uint32_t seq,
                           float from_h, float to_h, float rate,
                           uint32_t seg_id);
void cmd_build_simple     (cmd_block_t *b, uint32_t seq, uint16_t kind);
void cmd_build_tool_move  (cmd_block_t *b, uint32_t seq,
                           int slot, float target, float speed);
void cmd_build_tool_slot  (cmd_block_t *b, uint32_t seq,
                           uint16_t kind, int slot);
void cmd_build_charge     (cmd_block_t *b, uint32_t seq,
                           int slot, float target_j);
void cmd_build_set_param  (cmd_block_t *b, uint32_t seq,
                           uint32_t param_id, float value);
void cmd_build_telem_rate (cmd_block_t *b, uint32_t seq,
                           uint32_t period_inner_steps);
void cmd_build_telem_field(cmd_block_t *b, uint32_t seq,
                           uint64_t field_mask);

#ifdef __cplusplus
}
#endif

#endif  /* PHYSICS_PIPE_H */
