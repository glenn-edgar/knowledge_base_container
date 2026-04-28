CC      = gcc
CFLAGS  = -O2 -fPIC -Wall -Wextra -Wno-unused-parameter -std=c99
LDFLAGS = -shared -lm

TARGET  = libphysics.so
SRC     = physics_core.c

# libcomm — multi-bus, multi-dongle link layer between chain-tree and
# physics slaves. Phase 1a is a stub returning COMM_ERR_INIT for every
# entry; frame/link/router/transport land in slices 1b–1d.
LIBCOMM_TARGET = libcomm.so
LIBCOMM_SRCS   = libcomm/comm.c libcomm/frame.c libcomm/transport_inproc.c \
                 libcomm/transport_uart.c \
                 libcomm/manifest.c libcomm/router.c libcomm/link.c
LIBCOMM_HDRS   = libcomm/comm.h libcomm/frame.h libcomm/transport_inproc.h \
                 libcomm/transport_uart.h \
                 libcomm/manifest.h libcomm/router.h libcomm/link.h \
                 comm_manifest.h

# Generated avro_dsl artifacts. Regenerate by running `make manifest` after
# editing comm_manifest.lua. Generated files MUST NOT be hand-edited.
MANIFEST_SRC      = comm_manifest.lua
MANIFEST_OUTPUTS  = comm_manifest.h comm_manifest_bin.h comm_manifest_ffi.lua comm_manifest.bin

all: $(TARGET) $(LIBCOMM_TARGET) $(ROBOT_SIM_TARGET) manifest

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET) $(LDFLAGS)

$(LIBCOMM_TARGET): $(LIBCOMM_SRCS) $(LIBCOMM_HDRS)
	$(CC) $(CFLAGS) -Ilibcomm -I. $(LIBCOMM_SRCS) -o $(LIBCOMM_TARGET) $(LDFLAGS)

# robot_sim: Phase A standalone process holding the slave end of a pty.
# Mirrors what a USB dongle + its attached MCUs jointly do; in production
# this binary is replaced by real hardware and not packed into the image.
# Reuses libcomm/frame.c for SLIP/CRC and libcomm/comm.h for cmd codes.
ROBOT_SIM_TARGET = robot_sim/robot_sim
ROBOT_SIM_SRCS   = robot_sim/main.c libcomm/frame.c
ROBOT_SIM_HDRS   = libcomm/frame.h libcomm/comm.h

$(ROBOT_SIM_TARGET): $(ROBOT_SIM_SRCS) $(ROBOT_SIM_HDRS)
	$(CC) -O2 -Wall -Wextra -std=c99 -Ilibcomm -I. \
	    $(ROBOT_SIM_SRCS) -o $(ROBOT_SIM_TARGET) -lpthread

# libcomm depends on the generated comm_manifest.h, so make sure the
# manifest target runs first.
$(LIBCOMM_TARGET): $(MANIFEST_OUTPUTS)

manifest: $(MANIFEST_OUTPUTS)

$(MANIFEST_OUTPUTS): $(MANIFEST_SRC)
	luajit $(MANIFEST_SRC)

clean:
	rm -f $(TARGET) $(LIBCOMM_TARGET) $(ROBOT_SIM_TARGET) $(MANIFEST_OUTPUTS)

.PHONY: all clean manifest
