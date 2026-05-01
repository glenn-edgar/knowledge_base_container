CC      = gcc
CFLAGS  = -O2 -fPIC -Wall -Wextra -Wno-unused-parameter -std=c99
LDFLAGS = -shared -lm

TARGET  = libphysics.so
SRC     = physics_core.c

# robot_sim target name — defined here so the `all:` target below can
# reference it without forward-reference foot-gun (was missing the
# rebuild path on `make clean && make`).
ROBOT_SIM_TARGET = robot_sim/robot_sim

# libcomm — multi-bus, multi-dongle link layer between chain-tree and
# physics slaves. Phase 1a is a stub returning COMM_ERR_INIT for every
# entry; frame/link/router/transport land in slices 1b–1d.
LIBCOMM_TARGET = libcomm.so
LIBCOMM_SRCS   = libcomm/comm.c libcomm/frame.c libcomm/transport_inproc.c \
                 libcomm/transport_uart.c libcomm/ext_bus_linux_pty.c \
                 libcomm/manifest.c libcomm/router.c libcomm/link.c \
                 libcomm/bus_kernel_linux.c libcomm/logical_robot.c
LIBCOMM_HDRS   = libcomm/comm.h libcomm/frame.h libcomm/transport_inproc.h \
                 libcomm/transport_uart.h libcomm/ext_bus.h \
                 libcomm/manifest.h libcomm/router.h libcomm/link.h \
                 libcomm/bus_config.h libcomm/bus_kernel.h libcomm/bus_msg.h \
                 libcomm/logical_robot.h \
                 comm_manifest.h

# Generated avro_dsl artifacts. Regenerate by running `make manifest` after
# editing comm_manifest.lua. Generated files MUST NOT be hand-edited.
MANIFEST_SRC      = comm_manifest.lua
MANIFEST_OUTPUTS  = comm_manifest.h comm_manifest_bin.h comm_manifest_ffi.lua comm_manifest.bin

TEST_BUS_KERNEL_TARGET = test_bus_kernel
TEST_BUS_KERNEL_SRCS   = libcomm/test_bus_kernel.c libcomm/bus_kernel_linux.c

TEST_EXT_BUS_TARGET = test_ext_bus_contract
TEST_EXT_BUS_SRCS   = libcomm/test_ext_bus_contract.c \
                     libcomm/ext_bus_linux_pty.c \
                     libcomm/bus_kernel_linux.c

TEST_BUS_MSG_TARGET = test_bus_msg
TEST_BUS_MSG_SRCS   = libcomm/test_bus_msg.c libcomm/bus_kernel_linux.c

TEST_LOGICAL_ROBOT_TARGET = test_logical_robot
TEST_LOGICAL_ROBOT_SRCS   = libcomm/test_logical_robot.c \
                            libcomm/logical_robot.c \
                            libcomm/bus_kernel_linux.c

all: $(TARGET) $(LIBCOMM_TARGET) $(ROBOT_SIM_TARGET) \
     $(TEST_BUS_KERNEL_TARGET) $(TEST_EXT_BUS_TARGET) $(TEST_BUS_MSG_TARGET) \
     $(TEST_LOGICAL_ROBOT_TARGET) manifest

$(TEST_BUS_KERNEL_TARGET): $(TEST_BUS_KERNEL_SRCS) libcomm/bus_kernel.h libcomm/bus_config.h
	$(CC) -O2 -Wall -Wextra -std=c99 -Ilibcomm -I. \
	    $(TEST_BUS_KERNEL_SRCS) -o $(TEST_BUS_KERNEL_TARGET) -lpthread

$(TEST_EXT_BUS_TARGET): $(TEST_EXT_BUS_SRCS) libcomm/ext_bus.h libcomm/bus_kernel.h libcomm/bus_config.h
	$(CC) -O2 -Wall -Wextra -std=c99 -Ilibcomm -I. \
	    $(TEST_EXT_BUS_SRCS) -o $(TEST_EXT_BUS_TARGET) -lpthread

$(TEST_BUS_MSG_TARGET): $(TEST_BUS_MSG_SRCS) libcomm/bus_msg.h libcomm/bus_kernel.h libcomm/bus_config.h
	$(CC) -O2 -Wall -Wextra -std=c99 -Ilibcomm -I. \
	    $(TEST_BUS_MSG_SRCS) -o $(TEST_BUS_MSG_TARGET) -lpthread

$(TEST_LOGICAL_ROBOT_TARGET): $(TEST_LOGICAL_ROBOT_SRCS) libcomm/logical_robot.h libcomm/bus_msg.h libcomm/bus_kernel.h libcomm/bus_config.h
	$(CC) -O2 -Wall -Wextra -std=c99 -Ilibcomm -I. \
	    $(TEST_LOGICAL_ROBOT_SRCS) -o $(TEST_LOGICAL_ROBOT_TARGET) -lpthread

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET) $(LDFLAGS)

$(LIBCOMM_TARGET): $(LIBCOMM_SRCS) $(LIBCOMM_HDRS)
	$(CC) $(CFLAGS) -Ilibcomm -I. $(LIBCOMM_SRCS) -o $(LIBCOMM_TARGET) $(LDFLAGS) -lpthread

# robot_sim: Phase A standalone process holding the slave end of a pty.
# Mirrors what a USB dongle + its attached MCUs jointly do; in production
# this binary is replaced by real hardware and not packed into the image.
# Reuses libcomm/frame.c for SLIP/CRC and libcomm/comm.h for cmd codes.
# (ROBOT_SIM_TARGET is defined above so the `all:` line resolves.)
ROBOT_SIM_SRCS   = robot_sim/main.c libcomm/frame.c
ROBOT_SIM_HDRS   = libcomm/frame.h libcomm/comm.h libcomm/bus_config.h

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
	rm -f $(TARGET) $(LIBCOMM_TARGET) $(ROBOT_SIM_TARGET) \
	      $(TEST_BUS_KERNEL_TARGET) $(TEST_EXT_BUS_TARGET) $(TEST_BUS_MSG_TARGET) \
	      $(TEST_LOGICAL_ROBOT_TARGET) \
	      $(MANIFEST_OUTPUTS)

.PHONY: all clean manifest
