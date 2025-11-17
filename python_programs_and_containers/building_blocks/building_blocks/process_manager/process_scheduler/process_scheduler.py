#!/usr/bin/env python3
import asyncio
import os
import signal
import time
import base64
import json
from pathlib import Path
from typing import Dict, List, Optional, Callable
from datetime import datetime, timezone


class ChildSpec:
    def __init__(self, name: str, cmd: List[str], err_dir: Path):
        self.name = name
        self.cmd = cmd
        self.err_file = open(err_dir / f"{name}.stderr.log", "wb", buffering=0)
        self.process: Optional[asyncio.subprocess.Process] = None
        self.backoff = 1
        self.restarts = 0
        self.last_start_time = 0.0
        self.err_path = str(self.err_file.name)

    async def start(self):
        now = time.time()
        if (now - self.last_start_time) > 30:
            self.backoff = 1
        self.last_start_time = now

        self.process = await asyncio.create_subprocess_exec(
            *self.cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=self.err_file,
            start_new_session=True,
        )
        print(f"[{self.name}] started pid={self.process.pid}")

    async def wait(self) -> int:
        return await self.process.wait()

    async def stop(self, sig=signal.SIGTERM):
        if self.process and (self.process.returncode is None):
            try:
                os.killpg(self.process.pid, sig)
            except ProcessLookupError:
                pass

    def close(self):
        try:
            self.err_file.flush()
            self.err_file.close()
        except Exception:
            pass


class ProcessScheduler:
    def __init__(
        self,
        base_dir: Path,
        programs: Dict[str, List[str]],
        err_dir: Path = Path("/tmp"),
        error_handler: Optional[Callable[[str, str], None]] = None,
    ):
        """
        error_handler: function(process_name: str, json_string: str)
        json_string will be of the form:
        {
            "process": "<name>",
            "timestamp": <utc seconds>,
            "utc_iso": "<UTC ISO string>",
            "data": "<base64 stderr contents>"
        }
        """
        self.base_dir = base_dir
        self.programs = [ChildSpec(name, cmd, err_dir) for name, cmd in programs.items()]
        self.err_dir = err_dir
        self.stop_event = asyncio.Event()
        self.tasks: List[asyncio.Task] = []
        self.error_handler = error_handler or self._default_error_handler

    def _default_error_handler(self, name: str, json_payload: str):
        print(f"[{name}] error JSON:\n{json_payload}")

    async def supervise(self, child: ChildSpec):
        await child.start()
        while not self.stop_event.is_set():
            rc = await child.wait()

            # Read stderr contents
            try:
                with open(child.err_path, "rb") as f:
                    raw = f.read()
                encoded = base64.b64encode(raw).decode("ascii")
            except Exception as e:
                encoded = base64.b64encode(str(e).encode()).decode("ascii")

            ts = time.time()
            iso = datetime.now(timezone.utc).isoformat()

            payload = {
                "process": child.name,
                "timestamp": ts,
                "utc_iso": iso,
                "data": encoded,
                "return_code": rc,
            }
            json_str = json.dumps(payload)

            self.error_handler(child.name, json_str)

            if self.stop_event.is_set():
                break

            child.restarts += 1
            print(f"[{child.name}] restarting in {child.backoff}s (#{child.restarts})")
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=child.backoff)
                break
            except asyncio.TimeoutError:
                pass
            child.backoff = min(60, child.backoff * 2)
            await child.start()

        await child.stop()
        child.close()
        print(f"[{child.name}] supervisor exiting")

    async def zombie_reaper(self):
        while not self.stop_event.is_set():
            try:
                while True:
                    pid, _ = os.waitpid(-1, os.WNOHANG)
                    if pid == 0:
                        break
                    print(f"[reaper] reaped stray pid={pid}")
            except ChildProcessError:
                pass

            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                continue

    async def run(self):
        self.tasks = [asyncio.create_task(self.supervise(c)) for c in self.programs]
        self.tasks.append(asyncio.create_task(self.zombie_reaper()))
        await self.stop_event.wait()
        await self._shutdown()

    async def _shutdown(self):
        print("[ProcessScheduler] stopping children...")
        await asyncio.gather(*(c.stop() for c in self.programs), return_exceptions=True)
        try:
            await asyncio.wait_for(asyncio.gather(*self.tasks, return_exceptions=True), timeout=10)
        except asyncio.TimeoutError:
            print("[ProcessScheduler] force killing lingering children...")
            for c in self.programs:
                await c.stop(sig=signal.SIGKILL)
        for c in self.programs:
            c.close()
        print("[ProcessScheduler] stopped")

    def start(self):
        # Create a new event loop to avoid deprecation warning
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.run())
        finally:
            loop.close()

    def stop(self):
        self.stop_event.set()


# Test driver
if __name__ == "__main__":
    import sys
    import tempfile
    import shutil
    
    def run_tests():
        """Run test suite for the ProcessScheduler"""
        print("=" * 60)
        print("Running ProcessScheduler Test Suite")
        print("=" * 60)
        
        # Test 1: Basic process management
        print("\n[TEST 1] Basic process management")
        test_dir = Path(tempfile.mkdtemp(prefix="ProcessScheduler_test_"))
        print(f"Test directory: {test_dir}")
        
        try:
            # Create test scripts
            script1 = test_dir / "script1.py"
            script1.write_text("""
import time
import sys
print("Script 1 starting", file=sys.stderr)
time.sleep(2)
print("Script 1 ending", file=sys.stderr)
sys.exit(0)
""")
            
            script2 = test_dir / "script2.py"
            script2.write_text("""
import time
import sys
print("Script 2 starting", file=sys.stderr)
time.sleep(1)
print("Script 2 error!", file=sys.stderr)
sys.exit(1)
""")
            
            # Test with timeout
            async def test_basic():
                programs = {
                    "test_script1": ["python3", str(script1)],
                    "test_script2": ["python3", str(script2)],
                }
                
                error_count = {"count": 0}
                
                def test_handler(name: str, json_data: str):
                    error_count["count"] += 1
                    data = json.loads(json_data)
                    stderr = base64.b64decode(data["data"]).decode("utf-8", errors="ignore")
                    print(f"  [{name}] exited with code {data['return_code']}")
                    print(f"  stderr preview: {stderr[:100]}")
                
                agg = ProcessScheduler(test_dir, programs, err_dir=test_dir, error_handler=test_handler)
                
                # Run for 5 seconds then stop
                async def timeout_stop():
                    await asyncio.sleep(5)
                    agg.stop()
                
                asyncio.create_task(timeout_stop())
                await agg.run()
                
                print(f"  Total error events: {error_count['count']}")
                return error_count["count"] > 0
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(test_basic())
            loop.close()
            
            print(f"  Result: {'PASS' if result else 'FAIL'}")
            
        finally:
            shutil.rmtree(test_dir)
        
        # Test 2: Crash and restart behavior
        print("\n[TEST 2] Crash and restart with backoff")
        test_dir = Path(tempfile.mkdtemp(prefix="ProcessScheduler_test_"))
        
        try:
            crash_script = test_dir / "crash.py"
            crash_script.write_text("""
import sys
import os
print(f"Crash script PID {os.getpid()}", file=sys.stderr)
sys.exit(42)
""")
            
            async def test_restart():
                programs = {
                    "crasher": ["python3", str(crash_script)],
                }
                
                restart_times = []
                
                def restart_handler(name: str, json_data: str):
                    data = json.loads(json_data)
                    restart_times.append(data["timestamp"])
                    print(f"  Restart #{len(restart_times)} at {data['utc_iso']}")
                
                agg = ProcessScheduler(test_dir, programs, err_dir=test_dir, error_handler=restart_handler)
                
                async def stop_after_restarts():
                    await asyncio.sleep(10)  # Allow multiple restarts
                    agg.stop()
                
                asyncio.create_task(stop_after_restarts())
                await agg.run()
                
                # Check backoff behavior
                if len(restart_times) >= 3:
                    backoff1 = restart_times[1] - restart_times[0]
                    backoff2 = restart_times[2] - restart_times[1]
                    print(f"  Backoff intervals: {backoff1:.1f}s, {backoff2:.1f}s")
                    return backoff2 > backoff1  # Should increase
                return False
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(test_restart())
            loop.close()
            
            print(f"  Result: {'PASS - Backoff working' if result else 'FAIL'}")
            
        finally:
            shutil.rmtree(test_dir)
        
        # Test 3: Signal handling
        print("\n[TEST 3] Signal handling and graceful shutdown")
        test_dir = Path(tempfile.mkdtemp(prefix="ProcessScheduler_test_"))
        
        try:
            long_runner = test_dir / "long_runner.py"
            long_runner.write_text("""
import signal
import time
import sys

def handler(sig, frame):
    print("Received SIGTERM, shutting down gracefully", file=sys.stderr)
    sys.exit(0)

signal.signal(signal.SIGTERM, handler)
print("Long runner started", file=sys.stderr)

while True:
    time.sleep(0.5)
""")
            
            async def test_signals():
                programs = {
                    "long_runner": ["python3", str(long_runner)],
                }
                
                shutdown_graceful = {"status": False}
                
                def signal_handler(name: str, json_data: str):
                    data = json.loads(json_data)
                    stderr = base64.b64decode(data["data"]).decode("utf-8", errors="ignore")
                    if "gracefully" in stderr:
                        shutdown_graceful["status"] = True
                        print(f"  Process shut down gracefully")
                
                agg = ProcessScheduler(test_dir, programs, err_dir=test_dir, error_handler=signal_handler)
                
                async def trigger_shutdown():
                    await asyncio.sleep(2)
                    print("  Triggering shutdown...")
                    agg.stop()
                
                asyncio.create_task(trigger_shutdown())
                await agg.run()
                
                return shutdown_graceful["status"]
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(test_signals())
            loop.close()
            
            print(f"  Result: {'PASS' if result else 'FAIL'}")
            
        finally:
            shutil.rmtree(test_dir)
        
        print("\n" + "=" * 60)
        print("Test Suite Complete")
        print("=" * 60)
    
    def run_example():
        """Run the example usage"""
        print("Running example ProcessScheduler...")
        print("Configure BASE_DIR and PROGRAMS, then press Ctrl+C to stop\n")
        
        # You can modify these paths for your actual programs
        BASE_DIR = Path.cwd()
        
        # Example: Create simple test programs
        test_program = BASE_DIR / "test_program.py"
        if not test_program.exists():
            test_program.write_text("""
import time
import random
import sys

while True:
    time.sleep(random.randint(5, 15))
    if random.random() < 0.3:
        print("Random error occurred!", file=sys.stderr)
        sys.exit(1)
    print("Still running...", file=sys.stderr)
""")
            print(f"Created test program: {test_program}")
        
        PROGRAMS = {
            "test_program": ["python3", str(test_program)],
        }
        
        def my_handler(proc_name: str, json_data: str):
            data = json.loads(json_data)
            print(f"\n[ERROR HANDLER] Process: {proc_name}")
            print(f"  Return code: {data['return_code']}")
            print(f"  Timestamp: {data['utc_iso']}")
            stderr = base64.b64decode(data["data"]).decode("utf-8", errors="ignore")
            if stderr:
                print(f"  Stderr: {stderr[:200]}")
        
        agg = ProcessScheduler(BASE_DIR, PROGRAMS, error_handler=my_handler)
        try:
            agg.start()
        except KeyboardInterrupt:
            print("\nShutting down...")
            agg.stop()
    
    # Main entry point
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        run_tests()
    else:
        run_example()