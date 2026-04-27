#!/bin/bash
# run_tests.sh -- Orchestrate sim-robot tests.
#
#   1. Rebuild libphysics.so + remote.json as needed
#   2. Run standalone physics unit tests (no broker)
#   3. Launch mock planner + robot in background
#   4. Wait for robot link_state = live
#   5. Run random-path scenarios over MQTT
#   6. Clean up background processes on any exit path
#
# Options:
#   --skip-build     Don't rebuild libphysics.so or remote.json
#   --skip-unit      Skip standalone physics tests
#   --skip-e2e       Skip end-to-end MQTT tests
#   --speed N        Physics speed factor (default 10)
#   --host HOST      MQTT broker host (default localhost)
#   --port N         MQTT broker port (default 1883)
#   --count N        Commands per e2e scenario (default 10)
#   --verbose        Tail logs on failure
#
# Exit 0 = all pass, non-zero = something failed.

set -u
set -o pipefail

# ---------- arg parsing ----------
SKIP_BUILD=0; SKIP_UNIT=0; SKIP_E2E=0
SPEED=10
MQTT_HOST="localhost"; MQTT_PORT=1883
COUNT=10
VERBOSE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-build) SKIP_BUILD=1; shift ;;
        --skip-unit)  SKIP_UNIT=1;  shift ;;
        --skip-e2e)   SKIP_E2E=1;   shift ;;
        --speed)      SPEED="$2";     shift 2 ;;
        --host)       MQTT_HOST="$2"; shift 2 ;;
        --port)       MQTT_PORT="$2"; shift 2 ;;
        --count)      COUNT="$2";     shift 2 ;;
        --verbose)    VERBOSE=1;    shift ;;
        -h|--help)
            sed -n '2,24p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1"; exit 2 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BB_DIR="$SCRIPT_DIR/.."
CT_DSL="$BB_DIR/chain_tree_luajit/lua_dsl"
JSON_LIB="$CT_DSL/luajit_pipeline"
MQTT_BASE="$BB_DIR/knowledge_base/mqtt"
LOG_DIR="/tmp/rover_test_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

# Colors (only if stdout is a TTY)
if [[ -t 1 ]]; then
    R='\033[31m'; G='\033[32m'; Y='\033[33m'; B='\033[34m'; N='\033[0m'
else
    R=''; G=''; Y=''; B=''; N=''
fi

log()  { echo -e "${B}[run_tests]${N} $*"; }
pass() { echo -e "${G}[PASS]${N} $*"; }
fail() { echo -e "${R}[FAIL]${N} $*"; }
warn() { echo -e "${Y}[WARN]${N} $*"; }

FAILURES=0

# ---------- cleanup trap ----------
MOCK_PID=""; ROBOT_PID=""

cleanup() {
    log "cleaning up background processes..."
    if [[ -n "${ROBOT_PID:-}" ]]; then
        kill "$ROBOT_PID" 2>/dev/null || true
        # The shell script may spawn a subprocess; kill by pattern too
        pkill -P "$ROBOT_PID" 2>/dev/null || true
    fi
    if [[ -n "${MOCK_PID:-}" ]]; then
        kill "$MOCK_PID" 2>/dev/null || true
    fi
    sleep 1
    # Force-kill anything still around
    pkill -9 -f "mqtt_robot_main.lua"   2>/dev/null || true
    pkill -9 -f "test_mock_planner.lua" 2>/dev/null || true
    log "logs kept in $LOG_DIR"
}
trap cleanup EXIT INT TERM

# ---------- step 1: build ----------
cd "$SCRIPT_DIR"

if [[ $SKIP_BUILD -eq 0 ]]; then
    log "building libphysics.so"
    if ! make -B >"$LOG_DIR/make.log" 2>&1; then
        fail "libphysics.so build failed"
        cat "$LOG_DIR/make.log"
        exit 1
    fi
    pass "libphysics.so built"

    log "building remote.json (ChainTree DSL compile)"
    if ! bash build.sh >"$LOG_DIR/build.log" 2>&1; then
        fail "remote.json build failed"
        [[ $VERBOSE -eq 1 ]] && cat "$LOG_DIR/build.log"
        exit 1
    fi
    pass "remote.json built"
fi

# ---------- Lua path (shared by all luajit invocations) ----------
export LUA_PATH="$JSON_LIB/?.lua;$MQTT_BASE/?.lua;$MQTT_BASE/lib/?.lua;$SCRIPT_DIR/?.lua;;"
export LD_LIBRARY_PATH="$MQTT_BASE:${LD_LIBRARY_PATH:-}"

# ---------- step 2: standalone unit tests ----------
if [[ $SKIP_UNIT -eq 0 ]]; then
    log "running standalone physics tests"
    if luajit "$SCRIPT_DIR/test_physics.lua" >"$LOG_DIR/test_physics.log" 2>&1; then
        grep -E "^  (PASS|FAIL)" "$LOG_DIR/test_physics.log" | tail -3
        local_pass=$(grep -c "^  PASS" "$LOG_DIR/test_physics.log" || true)
        local_fail=$(grep -c "^  FAIL" "$LOG_DIR/test_physics.log" || true)
        pass "standalone physics: $local_pass passed, $local_fail failed"
    else
        fail "standalone physics tests failed"
        [[ $VERBOSE -eq 1 ]] && cat "$LOG_DIR/test_physics.log"
        FAILURES=$((FAILURES+1))
    fi

    log "running libcomm frame unit tests (slice 1b)"
    if luajit "$SCRIPT_DIR/test_frame_unit.lua" >"$LOG_DIR/test_frame_unit.log" 2>&1; then
        local_pass=$(grep -c "^  PASS" "$LOG_DIR/test_frame_unit.log" || true)
        local_fail=$(grep -c "^  FAIL" "$LOG_DIR/test_frame_unit.log" || true)
        pass "libcomm frame: $local_pass passed, $local_fail failed"
    else
        fail "libcomm frame unit tests failed"
        [[ $VERBOSE -eq 1 ]] && cat "$LOG_DIR/test_frame_unit.log"
        FAILURES=$((FAILURES+1))
    fi

    log "running libcomm transport_inproc unit tests (slice 1c)"
    if luajit "$SCRIPT_DIR/test_transport_inproc.lua" >"$LOG_DIR/test_transport_inproc.log" 2>&1; then
        local_pass=$(grep -c "^  PASS" "$LOG_DIR/test_transport_inproc.log" || true)
        local_fail=$(grep -c "^  FAIL" "$LOG_DIR/test_transport_inproc.log" || true)
        pass "libcomm transport_inproc: $local_pass passed, $local_fail failed"
    else
        fail "libcomm transport_inproc unit tests failed"
        [[ $VERBOSE -eq 1 ]] && cat "$LOG_DIR/test_transport_inproc.log"
        FAILURES=$((FAILURES+1))
    fi

    log "running libcomm link/router unit tests (slice 1d)"
    if luajit "$SCRIPT_DIR/test_link_router.lua" >"$LOG_DIR/test_link_router.log" 2>&1; then
        local_pass=$(grep -c "^  PASS" "$LOG_DIR/test_link_router.log" || true)
        local_fail=$(grep -c "^  FAIL" "$LOG_DIR/test_link_router.log" || true)
        pass "libcomm link/router: $local_pass passed, $local_fail failed"
    else
        fail "libcomm link/router unit tests failed"
        [[ $VERBOSE -eq 1 ]] && cat "$LOG_DIR/test_link_router.log"
        FAILURES=$((FAILURES+1))
    fi

    log "running libcomm end-to-end loopback tests (slice 1e)"
    if luajit "$SCRIPT_DIR/test_comm_loopback.lua" >"$LOG_DIR/test_comm_loopback.log" 2>&1; then
        local_pass=$(grep -c "^  PASS" "$LOG_DIR/test_comm_loopback.log" || true)
        local_fail=$(grep -c "^  FAIL" "$LOG_DIR/test_comm_loopback.log" || true)
        pass "libcomm loopback: $local_pass passed, $local_fail failed"
    else
        fail "libcomm loopback tests failed"
        [[ $VERBOSE -eq 1 ]] && cat "$LOG_DIR/test_comm_loopback.log"
        FAILURES=$((FAILURES+1))
    fi
fi

# ---------- step 3+: e2e ----------
if [[ $SKIP_E2E -eq 0 ]]; then

    # check broker (retry up to 20s -- broker containers sometimes bounce)
    broker_ok=0
    for i in $(seq 1 20); do
        if mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" -t test/ping -m hello -q 0 >/dev/null 2>&1; then
            broker_ok=1; break
        fi
        [[ $i -eq 1 ]] && log "broker not ready, retrying..."
        sleep 1
    done
    if [[ $broker_ok -ne 1 ]]; then
        fail "MQTT broker unreachable at $MQTT_HOST:$MQTT_PORT after 20s"
        exit 1
    fi
    pass "broker reachable"

    # launch mock planner
    log "launching mock planner"
    nohup luajit "$SCRIPT_DIR/test_mock_planner.lua" \
        --host "$MQTT_HOST" --port "$MQTT_PORT" --duration 600 \
        >"$LOG_DIR/mock_planner.log" 2>&1 &
    MOCK_PID=$!
    sleep 1
    if ! kill -0 "$MOCK_PID" 2>/dev/null; then
        fail "mock planner died at launch"
        cat "$LOG_DIR/mock_planner.log"
        exit 1
    fi

    # launch robot
    log "launching robot (speed_factor=${SPEED}x)"
    SPEED_FACTOR="$SPEED" MQTT_HOST="$MQTT_HOST" MQTT_PORT="$MQTT_PORT" \
        nohup bash "$BB_DIR/ros_scripts/start_robot.sh" \
            "$SCRIPT_DIR/rover_1_config.json" \
            >"$LOG_DIR/rover_1.log" 2>&1 &
    ROBOT_PID=$!
    sleep 1
    if ! kill -0 "$ROBOT_PID" 2>/dev/null; then
        fail "robot start.sh died immediately"
        cat "$LOG_DIR/rover_1.log"
        exit 1
    fi

    # wait for robot to go live
    log "waiting up to 15s for robot link_state=live"
    live=0
    for i in $(seq 1 30); do
        if grep -q "LINK_CLIENT.*→ live" "$LOG_DIR/rover_1.log" 2>/dev/null; then
            live=1; break
        fi
        sleep 0.5
    done
    if [[ $live -ne 1 ]]; then
        fail "robot did not become live"
        tail -30 "$LOG_DIR/rover_1.log"
        FAILURES=$((FAILURES+1))
    else
        pass "robot is live"
    fi

    if [[ $live -eq 1 ]]; then
        # Wait for broker to be reachable. Allow up to 90s; the broker
        # container can take a while to come back.
        wait_for_broker() {
            for i in $(seq 1 90); do
                if mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" -t test/ping -m p -q 0 >/dev/null 2>&1; then
                    return 0
                fi
                sleep 1
            done
            return 1
        }

        # Tear down the mock planner + robot (their sockets are dead
        # when the broker bounced) and launch a fresh pair.
        relaunch_stack() {
            log "relaunching mock planner + robot"
            pkill -9 -f "test_mock_planner.lua" 2>/dev/null || true
            pkill -9 -f "mqtt_robot_main.lua"   2>/dev/null || true
            sleep 1

            # Rotate log filenames so prior logs aren't clobbered
            local ts=$(date +%H%M%S)
            [[ -f "$LOG_DIR/mock_planner.log" ]] && \
                mv "$LOG_DIR/mock_planner.log" "$LOG_DIR/mock_planner.$ts.log"
            [[ -f "$LOG_DIR/rover_1.log" ]] && \
                mv "$LOG_DIR/rover_1.log" "$LOG_DIR/rover_1.$ts.log"

            nohup luajit "$SCRIPT_DIR/test_mock_planner.lua" \
                --host "$MQTT_HOST" --port "$MQTT_PORT" --duration 600 \
                >"$LOG_DIR/mock_planner.log" 2>&1 &
            MOCK_PID=$!
            sleep 1

            SPEED_FACTOR="$SPEED" MQTT_HOST="$MQTT_HOST" MQTT_PORT="$MQTT_PORT" \
                nohup bash "$BB_DIR/ros_scripts/start_robot.sh" \
                    "$SCRIPT_DIR/rover_1_config.json" \
                    >"$LOG_DIR/rover_1.log" 2>&1 &
            ROBOT_PID=$!

            for i in $(seq 1 30); do
                if grep -q "LINK_CLIENT.*→ live" "$LOG_DIR/rover_1.log" 2>/dev/null; then
                    return 0
                fi
                sleep 0.5
            done
            return 1
        }

        # Ensure the stack is healthy (broker up, robot live).
        # Relaunches mock+robot if broker has flapped.
        ensure_robot_live() {
            if ! wait_for_broker; then return 1; fi
            # Check if robot is still responsive (recent "live" in log)
            if tail -20 "$LOG_DIR/rover_1.log" 2>/dev/null | grep -q "→ live" && \
               ! tail -10 "$LOG_DIR/rover_1.log" 2>/dev/null | grep -q "planner lost" ; then
                return 0
            fi
            relaunch_stack
        }

        # Run one scenario. On broker disconnect during the run, retry up to
        # $max_retry times. Returns 0 on success, non-zero on real failure.
        run_scenario() {
            local name="$1"; shift
            local outfile="$LOG_DIR/$name.log"
            local max_retry=2

            for attempt in $(seq 1 $max_retry); do
                if [[ $attempt -gt 1 ]]; then
                    log "e2e: $name (retry $attempt/$max_retry -- broker flap)"
                    if ! ensure_robot_live; then
                        fail "$name  broker never recovered"
                        return 1
                    fi
                    mv "$outfile" "${outfile}.attempt$((attempt-1))" 2>/dev/null || true
                else
                    log "e2e: $name"
                fi

                if luajit "$SCRIPT_DIR/test_random_paths.lua" \
                    --host "$MQTT_HOST" --port "$MQTT_PORT" "$@" >"$outfile" 2>&1
                then
                    local sum=$(grep "^Summary:" "$outfile" | tail -1)
                    pass "$name  $sum"
                    return 0
                fi

                # Non-zero exit. Distinguish broker flap from real failure.
                if grep -q "unexpected disconnect" "$outfile" ; then
                    warn "$name  broker disconnect mid-run, will retry"
                    continue
                fi

                local sum=$(grep "^Summary:" "$outfile" | tail -1)
                fail "$name  $sum"
                [[ $VERBOSE -eq 1 ]] && tail -30 "$outfile"
                return 1
            done

            # All retries exhausted
            local sum=$(grep "^Summary:" "$outfile" | tail -1)
            fail "$name  $sum  (broker flapped on every attempt)"
            [[ $VERBOSE -eq 1 ]] && tail -30 "$outfile"
            return 1
        }

        run_scenario "paths_only_seed42" \
            --seed 42 --count "$COUNT" --mode paths_only --workspace 2.0 --wait 120 \
            || FAILURES=$((FAILURES+1))
        ensure_robot_live >/dev/null

        run_scenario "paths_only_seed1" \
            --seed 1  --count "$COUNT" --mode paths_only --workspace 2.0 --wait 120 \
            || FAILURES=$((FAILURES+1))
        ensure_robot_live >/dev/null

        run_scenario "mixed_seed17" \
            --seed 17 --count "$COUNT" --mode mixed      --workspace 2.0 --wait 180 \
            || FAILURES=$((FAILURES+1))
        ensure_robot_live >/dev/null

        run_scenario "mixed_seed99" \
            --seed 99 --count "$COUNT" --mode mixed      --workspace 2.0 --wait 180 \
            || FAILURES=$((FAILURES+1))
    fi
fi

# ---------- summary ----------
echo
if [[ $FAILURES -eq 0 ]]; then
    pass "ALL TESTS GREEN"
    exit 0
else
    fail "$FAILURES test group(s) failed -- see $LOG_DIR"
    exit 1
fi
