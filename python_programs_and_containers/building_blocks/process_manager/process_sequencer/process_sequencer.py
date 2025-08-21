#!/usr/bin/env python3
import asyncio
import os
import signal
import time
import base64
import json
from pathlib import Path
from typing import Dict, List, Optional, Callable, Tuple
from datetime import datetime, timezone


class SequentialChildSpec:
    def __init__(self, name: str, cmd: List[str], timeout: float, err_dir: Path):
        self.name = name
        self.cmd = cmd
        self.timeout = timeout  # seconds, 0 or None means no timeout
        self.err_file = open(err_dir / f"{name}.stderr.log", "ab", buffering=0)
        self.process: Optional[asyncio.subprocess.Process] = None
        self.err_path = str(self.err_file.name)
        self.start_time = 0.0
        self.end_time = 0.0
        self.return_code: Optional[int] = None
        self.timed_out = False

    async def start(self):
        self.start_time = time.time()
        self.timed_out = False
        self.process = await asyncio.create_subprocess_exec(
            *self.cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=self.err_file,
            start_new_session=True,
        )
        print(f"[{self.name}] started pid={self.process.pid} (timeout={self.timeout}s)")

    async def wait(self) -> int:
        """Wait for process completion with timeout"""
        try:
            if self.timeout and self.timeout > 0:
                self.return_code = await asyncio.wait_for(
                    self.process.wait(), 
                    timeout=self.timeout
                )
            else:
                self.return_code = await self.process.wait()
        except asyncio.TimeoutError:
            self.timed_out = True
            print(f"[{self.name}] timeout after {self.timeout}s, sending SIGTERM")
            await self.stop(signal.SIGTERM)
            
            # Give it 5 seconds to terminate gracefully
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5)
                self.return_code = self.process.returncode
            except asyncio.TimeoutError:
                print(f"[{self.name}] forcing SIGKILL after graceful termination failed")
                await self.stop(signal.SIGKILL)
                await self.process.wait()
                self.return_code = self.process.returncode
        
        self.end_time = time.time()
        return self.return_code

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


class ProcessSequencer:
    def __init__(
        self,
        base_dir: Path,
        programs: Dict[str, Tuple[List[str], float]],  # name -> (cmd, timeout)
        err_dir: Path = Path("/tmp"),
        error_handler: Optional[Callable[[str, str], None]] = None,
        continue_on_error: bool = False,
    ):
        """
        programs: Dict mapping process name to tuple of (command list, timeout in seconds)
        continue_on_error: If False, stop sequence on first error. If True, continue with next process.
        error_handler: function(process_name: str, json_string: str)
        json_string will be of the form:
        {
            "process": "<name>",
            "timestamp": <utc seconds>,
            "utc_iso": "<UTC ISO string>",
            "data": "<base64 stderr contents>",
            "return_code": <int>,
            "timed_out": <bool>,
            "elapsed_time": <float>
        }
        """
        self.base_dir = base_dir
        self.programs = [
            SequentialChildSpec(name, cmd, timeout, err_dir) 
            for name, (cmd, timeout) in programs.items()
        ]
        self.err_dir = err_dir
        self.stop_event = asyncio.Event()
        self.error_handler = error_handler or self._default_error_handler
        self.continue_on_error = continue_on_error
        self.completed_processes = []
        self.failed_process = None

    def _default_error_handler(self, name: str, json_payload: str):
        print(f"[{name}] completion JSON:\\n{json_payload}")

    async def run_sequence(self, child: SequentialChildSpec) -> bool:
        """Run a single process to completion. Returns True if successful."""
        await child.start()
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
        elapsed = child.end_time - child.start_time

        payload = {
            "process": child.name,
            "timestamp": ts,
            "utc_iso": iso,
            "data": encoded,
            "return_code": rc,
            "timed_out": child.timed_out,
            "elapsed_time": elapsed,
        }
        json_str = json.dumps(payload)

        self.error_handler(child.name, json_str)
        
        success = (rc == 0) and not child.timed_out
        
        if success:
            print(f"[{child.name}] completed successfully in {elapsed:.2f}s")
        else:
            if child.timed_out:
                print(f"[{child.name}] failed: timeout after {child.timeout}s (return_code={rc})")
            else:
                print(f"[{child.name}] failed: return_code={rc} in {elapsed:.2f}s")
        
        child.close()
        return success

    async def zombie_reaper(self):
        """Reap any zombie processes"""
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
        """Run all processes in sequence"""
        reaper_task = asyncio.create_task(self.zombie_reaper())
        
        print(f"[ProcessSequencer] starting sequence of {len(self.programs)} processes")
        print(f"[ProcessSequencer] continue_on_error={self.continue_on_error}")
        
        for i, child in enumerate(self.programs, 1):
            if self.stop_event.is_set():
                print(f"[ProcessSequencer] stopped before process {i}/{len(self.programs)}: {child.name}")
                break
                
            print(f"[ProcessSequencer] running process {i}/{len(self.programs)}: {child.name}")
            
            success = await self.run_sequence(child)
            
            if success:
                self.completed_processes.append(child.name)
            else:
                self.failed_process = child.name
                if not self.continue_on_error:
                    print(f"[ProcessSequencer] stopping sequence due to failure of: {child.name}")
                    break
                else:
                    print(f"[ProcessSequencer] continuing despite failure of: {child.name}")
                    self.completed_processes.append(child.name)
        
        self.stop_event.set()
        reaper_task.cancel()
        
        print(f"[ProcessSequencer] sequence complete")
        print(f"[ProcessSequencer] completed: {len(self.completed_processes)}/{len(self.programs)} processes")
        if self.failed_process and not self.continue_on_error:
            print(f"[ProcessSequencer] stopped at: {self.failed_process}")

    async def _shutdown(self):
        """Emergency shutdown of any running process"""
        print("[ProcessSequencer] emergency shutdown...")
        for child in self.programs:
            if child.process and child.process.returncode is None:
                await child.stop(signal.SIGTERM)
                try:
                    await asyncio.wait_for(child.process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    await child.stop(signal.SIGKILL)
            child.close()
        print("[ProcessSequencer] shutdown complete")

    def start(self):
        """Start the sequencer"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.run())
        except KeyboardInterrupt:
            print("\\n[ProcessSequencer] interrupted!")
            loop.run_until_complete(self._shutdown())
        finally:
            loop.close()

    def stop(self):
        """Stop the sequencer"""
        self.stop_event.set()


# Test driver
if __name__ == "__main__":
    import sys
    import tempfile
    import shutil
    
    def run_tests():
        """Run test suite for the ProcessSequencer"""
        print("=" * 60)
        print("Running ProcessSequencer Test Suite")
        print("=" * 60)
        
        # Test 1: Basic sequential execution
        print("\\n[TEST 1] Basic sequential execution")
        test_dir = Path(tempfile.mkdtemp(prefix="ProcessSequencer_test_"))
        print(f"Test directory: {test_dir}")
        
        try:
            # Create test scripts
            script1 = test_dir / "script1.py"
            script1.write_text("""
import time
import sys
print("Script 1 starting", file=sys.stderr)
time.sleep(1)
print("Script 1 ending", file=sys.stderr)
sys.exit(0)
""")
            
            script2 = test_dir / "script2.py"
            script2.write_text("""
import time
import sys
print("Script 2 starting", file=sys.stderr)
time.sleep(0.5)
print("Script 2 ending", file=sys.stderr)
sys.exit(0)
""")
            
            script3 = test_dir / "script3.py"
            script3.write_text("""
import time
import sys
print("Script 3 starting", file=sys.stderr)
time.sleep(0.5)
print("Script 3 ending", file=sys.stderr)
sys.exit(0)
""")
            
            async def test_sequential():
                programs = {
                    "test_script1": (["python3", str(script1)], 10.0),
                    "test_script2": (["python3", str(script2)], 10.0),
                    "test_script3": (["python3", str(script3)], 10.0),
                }
                
                results = []
                
                def test_handler(name: str, json_data: str):
                    data = json.loads(json_data)
                    results.append({
                        "name": name,
                        "return_code": data["return_code"],
                        "timed_out": data["timed_out"]
                    })
                
                seq = ProcessSequencer(test_dir, programs, err_dir=test_dir, error_handler=test_handler)
                await seq.run()
                
                # Check all three completed in order
                success = (len(results) == 3 and 
                          results[0]["name"] == "test_script1" and
                          results[1]["name"] == "test_script2" and
                          results[2]["name"] == "test_script3" and
                          all(r["return_code"] == 0 for r in results))
                
                print(f"  Executed {len(results)} processes")
                return success
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(test_sequential())
            loop.close()
            
            print(f"  Result: {'PASS' if result else 'FAIL'}")
            
        finally:
            shutil.rmtree(test_dir)
        
        # Test 2: Timeout handling
        print("\\n[TEST 2] Timeout handling")
        test_dir = Path(tempfile.mkdtemp(prefix="ProcessSequencer_test_"))
        
        try:
            timeout_script = test_dir / "timeout.py"
            timeout_script.write_text("""
import time
import sys
import signal

def handler(sig, frame):
    print("Received SIGTERM", file=sys.stderr)
    sys.exit(143)

signal.signal(signal.SIGTERM, handler)
print("Starting long sleep", file=sys.stderr)
time.sleep(30)  # Will timeout
""")
            
            async def test_timeout():
                programs = {
                    "timeout_test": (["python3", str(timeout_script)], 2.0),  # 2 second timeout
                }
                
                timeout_detected = {"status": False}
                
                def timeout_handler(name: str, json_data: str):
                    data = json.loads(json_data)
                    if data["timed_out"]:
                        timeout_detected["status"] = True
                        print(f"  Timeout detected: {name}")
                
                seq = ProcessSequencer(test_dir, programs, err_dir=test_dir, error_handler=timeout_handler)
                await seq.run()
                
                return timeout_detected["status"]
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(test_timeout())
            loop.close()
            
            print(f"  Result: {'PASS' if result else 'FAIL'}")
            
        finally:
            shutil.rmtree(test_dir)
        
        # Test 3: Error handling with continue_on_error=True
        print("\\n[TEST 3] Error handling with continue_on_error=True")
        test_dir = Path(tempfile.mkdtemp(prefix="ProcessSequencer_test_"))
        
        try:
            good_script = test_dir / "good.py"
            good_script.write_text("""
import sys
print("Good script", file=sys.stderr)
sys.exit(0)
""")
            
            bad_script = test_dir / "bad.py"
            bad_script.write_text("""
import sys
print("Bad script failing", file=sys.stderr)
sys.exit(42)
""")
            
            async def test_continue_on_error():
                programs = {
                    "good1": (["python3", str(good_script)], 5.0),
                    "bad": (["python3", str(bad_script)], 5.0),
                    "good2": (["python3", str(good_script)], 5.0),
                }
                
                execution_order = []
                
                def error_handler(name: str, json_data: str):
                    data = json.loads(json_data)
                    execution_order.append((name, data["return_code"]))
                
                seq = ProcessSequencer(
                    test_dir, programs, err_dir=test_dir, 
                    error_handler=error_handler, 
                    continue_on_error=True
                )
                await seq.run()
                
                # Should run all three despite failure
                return (len(execution_order) == 3 and
                       execution_order[0] == ("good1", 0) and
                       execution_order[1] == ("bad", 42) and
                       execution_order[2] == ("good2", 0))
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(test_continue_on_error())
            loop.close()
            
            print(f"  Result: {'PASS - continued after error' if result else 'FAIL'}")
            
        finally:
            shutil.rmtree(test_dir)
        
        # Test 4: Error handling with continue_on_error=False
        print("\\n[TEST 4] Error handling with continue_on_error=False")
        test_dir = Path(tempfile.mkdtemp(prefix="ProcessSequencer_test_"))
        
        try:
            # Re-create the scripts for Test 4
            good_script = test_dir / "good.py"
            good_script.write_text("""
import sys
print("Good script", file=sys.stderr)
sys.exit(0)
""")
            
            bad_script = test_dir / "bad.py"
            bad_script.write_text("""
import sys
print("Bad script failing", file=sys.stderr)
sys.exit(42)
""")
            
            async def test_stop_on_error():
                programs = {
                    "good1": (["python3", str(good_script)], 5.0),
                    "bad": (["python3", str(bad_script)], 5.0),
                    "good2": (["python3", str(good_script)], 5.0),
                }
                
                execution_order = []
                
                def error_handler(name: str, json_data: str):
                    data = json.loads(json_data)
                    execution_order.append((name, data["return_code"]))
                
                seq = ProcessSequencer(
                    test_dir, programs, err_dir=test_dir, 
                    error_handler=error_handler, 
                    continue_on_error=False
                )
                await seq.run()
                
                # Should stop after bad script
                return (len(execution_order) == 2 and
                       execution_order[0] == ("good1", 0) and
                       execution_order[1] == ("bad", 42))
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(test_stop_on_error())
            loop.close()
            
            print(f"  Result: {'PASS - stopped after error' if result else 'FAIL'}")
            
        finally:
            shutil.rmtree(test_dir)
        
        print("\\n" + "=" * 60)
        print("Test Suite Complete")
        print("=" * 60)
    
    def run_example():
        """Run example usage"""
        print("Running example ProcessSequencer...")
        print("This will run processes sequentially with timeouts\\n")
        
        BASE_DIR = Path.cwd()
        
        # Create example scripts
        script1 = BASE_DIR / "seq_script1.py"
        script1.write_text("""
import time
import sys
print("Phase 1: Initializing...", file=sys.stderr)
time.sleep(2)
print("Phase 1: Complete", file=sys.stderr)
sys.exit(0)
""")
        
        script2 = BASE_DIR / "seq_script2.py"
        script2.write_text("""
import time
import sys
print("Phase 2: Processing...", file=sys.stderr)
time.sleep(3)
print("Phase 2: Complete", file=sys.stderr)
sys.exit(0)
""")
        
        script3 = BASE_DIR / "seq_script3.py"
        script3.write_text("""
import time
import sys
print("Phase 3: Finalizing...", file=sys.stderr)
time.sleep(1)
print("Phase 3: Complete", file=sys.stderr)
sys.exit(0)
""")
        
        # Define programs with timeouts
        PROGRAMS = {
            "phase1_init": (["python3", str(script1)], 10.0),
            "phase2_process": (["python3", str(script2)], 10.0),
            "phase3_finalize": (["python3", str(script3)], 10.0),
        }
        
        def my_handler(proc_name: str, json_data: str):
            data = json.loads(json_data)
            print(f"\\n[COMPLETION] Process: {proc_name}")
            print(f"  Return code: {data['return_code']}")
            print(f"  Elapsed time: {data['elapsed_time']:.2f}s")
            print(f"  Timed out: {data['timed_out']}")
            stderr = base64.b64decode(data["data"]).decode("utf-8", errors="ignore")
            if stderr:
                print(f"  Output: {stderr.strip()}")
        
        seq = ProcessSequencer(BASE_DIR, PROGRAMS, error_handler=my_handler, continue_on_error=False)
        try:
            seq.start()
        except KeyboardInterrupt:
            print("\\nInterrupted!")
        finally:
            # Cleanup
            for script in [script1, script2, script3]:
                if script.exists():
                    script.unlink()
    
    # Main entry point
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        run_tests()
    else:
        run_example()