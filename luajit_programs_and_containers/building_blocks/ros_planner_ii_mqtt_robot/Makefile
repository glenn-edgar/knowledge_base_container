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
# drive_base_robot lives ABOVE libcomm: it's a logical_robot class, not
# transport infrastructure. Built into test binaries (and eventually
# robot_sim) directly, not linked into libcomm.so.
DRIVE_BASE_SRCS = libcomm/drive_base_robot.c
DRIVE_BASE_HDRS = libcomm/drive_base_robot.h

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

TEST_DRIVE_BASE_TARGET = test_drive_base
TEST_DRIVE_BASE_SRCS   = libcomm/test_drive_base.c \
                         libcomm/drive_base_robot.c \
                         libcomm/logical_robot.c \
                         libcomm/bus_kernel_linux.c

TEST_DONGLE_CAT_TARGET = test_dongle_catalogue
TEST_DONGLE_CAT_SRCS   = libcomm/test_dongle_catalogue.c \
                         robot_sim/dongle_threads.c \
                         libcomm/drive_base_robot.c \
                         libcomm/logical_robot.c \
                         libcomm/bus_kernel_linux.c \
                         libcomm/frame.c

all: $(TARGET) $(LIBCOMM_TARGET) $(ROBOT_SIM_TARGET) \
     $(TEST_BUS_KERNEL_TARGET) $(TEST_EXT_BUS_TARGET) $(TEST_BUS_MSG_TARGET) \
     $(TEST_LOGICAL_ROBOT_TARGET) $(TEST_DRIVE_BASE_TARGET) \
     $(TEST_DONGLE_CAT_TARGET) manifest

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

# test_drive_base links against libphysics.so for the phys_* symbols.
# -Wl,-rpath,'$$ORIGIN' makes the binary search for libphysics.so in
# its own directory at runtime, so test runs without setting
# LD_LIBRARY_PATH.
$(TEST_DRIVE_BASE_TARGET): $(TEST_DRIVE_BASE_SRCS) $(DRIVE_BASE_HDRS) libcomm/logical_robot.h libcomm/bus_msg.h libcomm/bus_kernel.h libcomm/bus_config.h $(TARGET)
	$(CC) -O2 -Wall -Wextra -std=c99 -Ilibcomm -I. \
	    $(TEST_DRIVE_BASE_SRCS) -o $(TEST_DRIVE_BASE_TARGET) \
	    -L. -lphysics -lm -lpthread -Wl,-rpath,'$$ORIGIN'

$(TEST_DONGLE_CAT_TARGET): $(TEST_DONGLE_CAT_SRCS) $(DRIVE_BASE_HDRS) libcomm/logical_robot.h libcomm/bus_msg.h libcomm/bus_kernel.h libcomm/bus_config.h libcomm/frame.h libcomm/comm.h robot_sim/dongle_skeleton.h $(TARGET)
	$(CC) -O2 -Wall -Wextra -std=c99 -Ilibcomm -Irobot_sim -I. \
	    $(TEST_DONGLE_CAT_SRCS) -o $(TEST_DONGLE_CAT_TARGET) \
	    -L. -lphysics -lm -lpthread -Wl,-rpath,'$$ORIGIN'

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET) $(LDFLAGS)

$(LIBCOMM_TARGET): $(LIBCOMM_SRCS) $(LIBCOMM_HDRS)
	$(CC) $(CFLAGS) -Ilibcomm -I. $(LIBCOMM_SRCS) -o $(LIBCOMM_TARGET) $(LDFLAGS) -lpthread

# robot_sim: Phase A/B standalone process holding the slave end of a
# pty. Slice L4b adds drive_base hosted inside the four-thread shape
# (ext_bus + dongle_manager + internal_bus + drive_base logical_robot),
# so we now link libphysics.so for the phys_* symbols. -Wl,-rpath,
# '$$ORIGIN' lets the binary find libphysics.so next to itself at
# runtime without LD_LIBRARY_PATH.
# (ROBOT_SIM_TARGET is defined above so the `all:` line resolves.)
ROBOT_SIM_SRCS   = robot_sim/main.c robot_sim/dongle_threads.c \
                   libcomm/frame.c libcomm/bus_kernel_linux.c \
                   libcomm/logical_robot.c libcomm/drive_base_robot.c
ROBOT_SIM_HDRS   = libcomm/frame.h libcomm/comm.h libcomm/bus_config.h \
                   libcomm/bus_kernel.h libcomm/bus_msg.h \
                   libcomm/logical_robot.h libcomm/drive_base_robot.h \
                   robot_sim/dongle_skeleton.h

$(ROBOT_SIM_TARGET): $(ROBOT_SIM_SRCS) $(ROBOT_SIM_HDRS) $(TARGET)
	$(CC) -O2 -Wall -Wextra -std=c99 -Ilibcomm -I. \
	    $(ROBOT_SIM_SRCS) -o $(ROBOT_SIM_TARGET) \
	    -L. -lphysics -lm -lpthread -Wl,-rpath,'$$ORIGIN/..'

# libcomm depends on the generated comm_manifest.h, so make sure the
# manifest target runs first.
$(LIBCOMM_TARGET): $(MANIFEST_OUTPUTS)

manifest: $(MANIFEST_OUTPUTS)

$(MANIFEST_OUTPUTS): $(MANIFEST_SRC)
	luajit $(MANIFEST_SRC)

clean:
	rm -f $(TARGET) $(LIBCOMM_TARGET) $(ROBOT_SIM_TARGET) \
	      $(TEST_BUS_KERNEL_TARGET) $(TEST_EXT_BUS_TARGET) $(TEST_BUS_MSG_TARGET) \
	      $(TEST_LOGICAL_ROBOT_TARGET) $(TEST_DRIVE_BASE_TARGET) \
	      $(TEST_DONGLE_CAT_TARGET) \
	      $(MANIFEST_OUTPUTS)

.PHONY: all clean manifest
