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
                 libcomm/manifest.c libcomm/router.c libcomm/link.c
LIBCOMM_HDRS   = libcomm/comm.h libcomm/frame.h libcomm/transport_inproc.h \
                 libcomm/manifest.h libcomm/router.h libcomm/link.h \
                 comm_manifest.h

# Generated avro_dsl artifacts. Regenerate by running `make manifest` after
# editing comm_manifest.lua. Generated files MUST NOT be hand-edited.
MANIFEST_SRC      = comm_manifest.lua
MANIFEST_OUTPUTS  = comm_manifest.h comm_manifest_bin.h comm_manifest_ffi.lua comm_manifest.bin

all: $(TARGET) $(LIBCOMM_TARGET) manifest

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET) $(LDFLAGS)

$(LIBCOMM_TARGET): $(LIBCOMM_SRCS) $(LIBCOMM_HDRS)
	$(CC) $(CFLAGS) -Ilibcomm -I. $(LIBCOMM_SRCS) -o $(LIBCOMM_TARGET) $(LDFLAGS)

# libcomm depends on the generated comm_manifest.h, so make sure the
# manifest target runs first.
$(LIBCOMM_TARGET): $(MANIFEST_OUTPUTS)

manifest: $(MANIFEST_OUTPUTS)

$(MANIFEST_OUTPUTS): $(MANIFEST_SRC)
	luajit $(MANIFEST_SRC)

clean:
	rm -f $(TARGET) $(LIBCOMM_TARGET) $(MANIFEST_OUTPUTS)

.PHONY: all clean manifest
