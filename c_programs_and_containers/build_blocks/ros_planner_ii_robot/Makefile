# ===========================================================================
# ROS Planner II — C Robot Makefile (ChainTree integrated)
#
# MQTT/CBOR robot with ChainTree C runtime for event-driven processing.
# Dependencies: libmosquitto-dev, libcjson-dev
# ===========================================================================

CC      ?= gcc
CFLAGS  ?= -Wall -Wextra -O2 -std=c11 -D_POSIX_C_SOURCE=200809L
LDFLAGS ?=

DEPFLAGS = -MMD -MP

# --- Directories ---
BUILD_DIR = build
SRC_DIR   = src
LIBS_DIR  = libs
DSL_DIR   = dsl

# --- ChainTree C directories ---
CT_ROOT     = ../chain_tree_c
CT_CORE_DIR = $(CT_ROOT)/runtime_binary
CT_FUNC_DIR = $(CT_ROOT)/runtime_functions
CT_JSON_DIR = $(CT_ROOT)/json_packets
CT_CBOR_DIR = $(CT_ROOT)/cbor_packets
CT_SE_DIR   = $(CT_ROOT)/s_expression

# --- Include paths ---
CFLAGS += -I$(SRC_DIR) -I$(LIBS_DIR) \
          -I$(CT_CORE_DIR)/include \
          -I$(CT_FUNC_DIR)/include \
          -I$(CT_JSON_DIR)/include \
          -I$(CT_CBOR_DIR)/include

# --- Output binary ---
TARGET = robot_main

# --- ChainTree static libraries ---
CT_CORE_LIB = $(CT_CORE_DIR)/libcfl_binarycore.a
CT_FUNC_LIB = $(CT_FUNC_DIR)/libcfl_core_functions.a
CT_JSON_LIB = $(CT_JSON_DIR)/lib/libcfl_json_packets.a
CT_CBOR_LIB = $(CT_CBOR_DIR)/lib/libcfl_cbor_packets.a
CT_SE_LIB   = $(CT_SE_DIR)/lib/libs_s_engine.a

# --- Source files ---
LIB_SRCS = $(LIBS_DIR)/mqtt_pubsub.c \
           $(LIBS_DIR)/cbor_codec.c

SRC_SRCS = $(SRC_DIR)/main.c \
           $(SRC_DIR)/robot_config.c \
           $(SRC_DIR)/robot_state.c \
           $(SRC_DIR)/robot_mqtt.c \
           $(SRC_DIR)/json_extract.c \
           $(SRC_DIR)/rob_publish.c \
           $(SRC_DIR)/ct_user_functions.c

ALL_SRCS = $(LIB_SRCS) $(SRC_SRCS)

# --- Object files ---
LIB_OBJS = $(patsubst $(LIBS_DIR)/%.c,$(BUILD_DIR)/libs/%.o,$(LIB_SRCS))
SRC_OBJS = $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/src/%.o,$(SRC_SRCS))
ALL_OBJS = $(LIB_OBJS) $(SRC_OBJS)
ALL_DEPS = $(ALL_OBJS:.o=.d)

# --- System libraries ---
LIBS = -lmosquitto -lcjson -lm -lpthread

# ===========================================================================

.PHONY: all clean rebuild run libs dsl

all: libs $(TARGET)

# --- Build ChainTree libraries if needed ---
libs:
	@$(MAKE) -s -C $(CT_CORE_DIR)
	@$(MAKE) -s -C $(CT_FUNC_DIR)
	@$(MAKE) -s -C $(CT_JSON_DIR)
	@$(MAKE) -s -C $(CT_CBOR_DIR)

$(TARGET): $(ALL_OBJS) $(CT_FUNC_LIB) $(CT_CBOR_LIB) $(CT_JSON_LIB) $(CT_CORE_LIB) $(CT_SE_LIB)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(ALL_OBJS) \
		$(CT_FUNC_LIB) $(CT_CBOR_LIB) $(CT_JSON_LIB) $(CT_CORE_LIB) $(CT_SE_LIB) $(LIBS)
	@echo "Built: $(TARGET)"

# --- Library objects ---
$(BUILD_DIR)/libs/%.o: $(LIBS_DIR)/%.c | $(BUILD_DIR)/libs
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

# --- Source objects ---
$(BUILD_DIR)/src/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)/src
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@

# --- Build directories ---
$(BUILD_DIR)/libs:
	mkdir -p $(BUILD_DIR)/libs

$(BUILD_DIR)/src:
	mkdir -p $(BUILD_DIR)/src

-include $(ALL_DEPS)

# --- DSL compilation (run from chain_tree_c directory) ---
dsl:
	cd $(CT_ROOT) && ./s_build_json.sh ../ros_planner_ii_robot/dsl/robot_mqtt_dsl.lua ../ros_planner_ii_robot/dsl
	cd $(CT_ROOT) && ./s_build_headers_binary.sh ../ros_planner_ii_robot/dsl/robot_mqtt_dsl.json ../ros_planner_ii_robot/dsl robot_handle

# --- Run ---
run: $(TARGET)
	./$(TARGET) config/robot_config.json

# --- Clean ---
clean:
	rm -rf $(BUILD_DIR) $(TARGET)

rebuild: clean all
