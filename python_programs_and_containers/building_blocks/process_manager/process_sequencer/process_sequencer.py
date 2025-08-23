#!/usr/bin/env python3
import asyncio
import os
import sys
import signal
import time
import base64
import json
from pathlib import Path
from typing import List, Optional, Callable, Tuple, Dict, Any
from datetime import datetime, timezone


class SequentialChildSpec:
    def __init__(self, name: str, cmd: List[str], timeout: float, err_dir: Path):
        
        self.name = name
        self.cmd = cmd
        self.timeout = timeout  # seconds, 0 or None means no timeout
        self.err_file = open(err_dir / f"{name}.stderr.log", "wb", buffering=0)
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
        programs: List[Dict[str, Any]],  # [{"name": str, "cmd": List[str], "timeout": float}, ...]
        err_dir: Path = Path("/tmp"),
        error_handler: Optional[Callable[[str, str], None]] = None,
        continue_on_error: bool = False,
    ):
        """
        programs: List of dicts: [{"name": str, "cmd": List[str], "timeout": float}, ...]
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
        self.programs = []
        for program in programs:
            print(f"[{program['name']}] configuring with cmd: {program['cmd']} with err_dir: {err_dir} and timeout: {program['timeout']}")
            self.programs.append(SequentialChildSpec(program["name"], program["cmd"], program["timeout"], err_dir))
        self.err_dir = err_dir
        self.stop_event = asyncio.Event()
        self.error_handler = error_handler or self._default_error_handler
        self.continue_on_error = continue_on_error
        self.completed_processes = []
        self.failed_process = None
        
        # NEW: Store all results
        self.results = []
        self.results_by_name = {}
        
        # NEW: Store completion status
        self.is_complete = False
        self.all_successful = False

    def _default_error_handler(self, name: str, json_payload: str):
        print(f"[{name}] completion JSON:\n{json_payload}")

    async def run_sequence(self, child: SequentialChildSpec) -> bool:
        """Run a single process to completion. Returns True if successful."""
        await child.start()
        rc = await child.wait()
        
        # Read stderr contents
        try:
            with open(child.err_path, "rb") as f:
                raw = f.read()
            encoded = base64.b64encode(raw).decode("ascii")
            decoded = raw.decode("utf-8", errors="ignore")  # Store decoded version too
        except Exception as e:
            encoded = base64.b64encode(str(e).encode()).decode("ascii")
            decoded = str(e)

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
        
        # NEW: Store additional useful fields
        result = {
            **payload,
            "decoded_output": decoded,  # Decoded stderr for easy access
            "encoded_output": encoded,
            "success": (rc == 0) and not child.timed_out,
            "command": child.cmd,
            "err_file_path": child.err_path,
        }
        
        # Store the result
        self.results.append(result)
        self.results_by_name[child.name] = result
        
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
        
        # NEW: Set completion status
        self.is_complete = True
        self.all_successful = all(r["success"] for r in self.results)
        
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
            print("\n[ProcessSequencer] interrupted!")
            loop.run_until_complete(self._shutdown())
        finally:
            loop.close()

    def stop(self):
        """Stop the sequencer"""
        self.stop_event.set()
    
    # NEW: Methods to access results
    
    def get_results(self) -> List[Dict[str, Any]]:
        """Get all results in order of execution"""
        return self.results
    
    def get_result(self, name: str) -> Optional[Dict[str, Any]]:
        """Get result for a specific process by name"""
        return self.results_by_name.get(name)
    
    def get_decoded_output(self, name: str) -> Optional[str]:
        """Get decoded stderr output for a specific process"""
        result = self.results_by_name.get(name)
        return result["decoded_output"] if result else None
    
    def get_failed_processes(self) -> List[Dict[str, Any]]:
        """Get all failed processes"""
        return [r for r in self.results if not r["success"]]
    
    def get_successful_processes(self) -> List[Dict[str, Any]]:
        """Get all successful processes"""
        return [r for r in self.results if r["success"]]
    
    def get_summary(self) -> Dict[str, Any]:
        """Get execution summary"""
        return {
            "total_processes": len(self.programs),
            "completed": len(self.results),
            "successful": len(self.get_successful_processes()),
            "failed": len(self.get_failed_processes()),
            "all_successful": self.all_successful,
            "is_complete": self.is_complete,
            "continue_on_error": self.continue_on_error,
            "failed_process": self.failed_process,
            "completed_processes": self.completed_processes,
        }
    
    def print_summary(self):
        """Print a formatted summary of execution"""
        summary = self.get_summary()
        print("\n" + "=" * 60)
        print("EXECUTION SUMMARY")
        print("=" * 60)
        print(f"Total processes: {summary['total_processes']}")
        print(f"Completed: {summary['completed']}")
        print(f"Successful: {summary['successful']}")
        print(f"Failed: {summary['failed']}")
        print(f"All successful: {summary['all_successful']}")
        
        if summary['failed'] > 0:
            print("\nFailed processes:")
            for result in self.get_failed_processes():
                print(f"  - {result['process']}: return_code={result['return_code']}, "
                      f"timed_out={result['timed_out']}")
                if result['decoded_output']:
                    # Truncate long output
                    output = result['decoded_output'].strip()
                    if len(output) > 200:
                        output = output[:200] + "..."
                    print(f"    Error: {output}")
        print("=" * 60)
    
    def save_results(self, filepath: Path):
        """Save results to a JSON file"""
        # Remove non-serializable fields
        serializable_results = []
        for r in self.results:
            result_copy = r.copy()
            # Don't save the decoded output in JSON (it's derivable from data field)
            result_copy.pop('decoded_output', None)
            result_copy.pop('err_file_path', None)
            # Convert command list to string for readability
            result_copy['command'] = ' '.join(r['command'])
            serializable_results.append(result_copy)
        
        with open(filepath, 'w') as f:
            json.dump({
                "summary": self.get_summary(),
                "results": serializable_results
            }, f, indent=2)
        print(f"Results saved to {filepath}")


# Test driver
if __name__ == "__main__":
    import tempfile
    import shutil
    
    def run_comprehensive_tests():
        """Run comprehensive test suite for ProcessSequencer with result retrieval"""
        print("=" * 80)
        print("ProcessSequencer Comprehensive Test Suite")
        print("=" * 80)
        
        # Test 1: Basic execution with result retrieval
        print("\n" + "=" * 60)
        print("TEST 1: Basic Sequential Execution with Result Retrieval")
        print("=" * 60)
        
        test_dir = Path(tempfile.mkdtemp(prefix="ProcessSeq_test1_"))
        err_dir = test_dir / "errors"
        err_dir.mkdir()
        
        try:
            # Create test scripts
            script1 = test_dir / "hello.py"
            script1.write_text("""
import sys
print("Hello from script 1", file=sys.stderr)
print("This is line 2", file=sys.stderr)
sys.exit(0)
""")
            
            script2 = test_dir / "world.py"
            script2.write_text("""
import sys
import time
print("World from script 2", file=sys.stderr)
time.sleep(0.5)
print("Processing complete", file=sys.stderr)
sys.exit(0)
""")
            
            programs = [
                {"name": "hello_script", "cmd": ["python3", str(script1)], "timeout": 5.0},
                {"name": "world_script", "cmd": ["python3", str(script2)], "timeout": 5.0},
            ]
            
            seq = ProcessSequencer(test_dir, programs, err_dir=err_dir)
            seq.start()
            
            # Test result retrieval methods
            print("\n--- Testing Result Retrieval ---")
            
            # Get all results
            all_results = seq.get_results()
            print(f"Total results: {len(all_results)}")
            
            # Get specific result
            hello_result = seq.get_result("hello_script")
            if hello_result:
                print(f"\nhello_script result:")
                print(f"  Success: {hello_result['success']}")
                print(f"  Return code: {hello_result['return_code']}")
                print(f"  Output: {hello_result['decoded_output'].strip()}")
            
            # Get decoded output directly
            world_output = seq.get_decoded_output("world_script")
            if world_output:
                print(f"\nworld_script output:")
                print(f"  {world_output.strip()}")
            
            # Print summary
            seq.print_summary()
            
            # Save results
            results_file = test_dir / "results.json"
            seq.save_results(results_file)
            
            # Verify saved file exists
            assert results_file.exists(), "Results file not created"
            print(f"\n✓ Test 1 PASSED")
            
        finally:
            shutil.rmtree(test_dir)
        
        # Test 2: Error handling with failed processes
        print("\n" + "=" * 60)
        print("TEST 2: Error Handling and Failed Process Retrieval")
        print("=" * 60)
        
        test_dir = Path(tempfile.mkdtemp(prefix="ProcessSeq_test2_"))
        err_dir = test_dir / "errors"
        err_dir.mkdir()
        
        try:
            good_script = test_dir / "good.py"
            good_script.write_text("""
import sys
print("Good script running", file=sys.stderr)
sys.exit(0)
""")
            
            bad_script = test_dir / "bad.py"
            bad_script.write_text("""
import sys
print("ERROR: Something went wrong!", file=sys.stderr)
print("Stack trace here...", file=sys.stderr)
sys.exit(42)
""")
            
            programs = [
                {"name": "good_1", "cmd": ["python3", str(good_script)], "timeout": 5.0},
                {"name": "bad_process", "cmd": ["python3", str(bad_script)], "timeout": 5.0},
                {"name": "good_2", "cmd": ["python3", str(good_script)], "timeout": 5.0},
            ]
            
            # Test with continue_on_error=True
            seq = ProcessSequencer(test_dir, programs, err_dir=err_dir, continue_on_error=True)
            seq.start()
            
            # Get failed processes
            failed = seq.get_failed_processes()
            print(f"\nFailed processes: {len(failed)}")
            for f in failed:
                print(f"  - {f['process']}: return_code={f['return_code']}")
                print(f"    Error output: {f['decoded_output'].strip()}")
            
            # Get successful processes
            successful = seq.get_successful_processes()
            print(f"\nSuccessful processes: {len(successful)}")
            for s in successful:
                print(f"  - {s['process']}")
            
            # Check summary
            summary = seq.get_summary()
            assert summary['failed'] == 1, "Should have 1 failed process"
            assert summary['successful'] == 2, "Should have 2 successful processes"
            assert summary['all_successful'] == False, "Should not be all successful"
            
            print(f"\n✓ Test 2 PASSED")
            
        finally:
            shutil.rmtree(test_dir)
        
        # Test 3: Timeout handling
        print("\n" + "=" * 60)
        print("TEST 3: Timeout Detection and Result Capture")
        print("=" * 60)
        
        test_dir = Path(tempfile.mkdtemp(prefix="ProcessSeq_test3_"))
        err_dir = test_dir / "errors"
        err_dir.mkdir()
        
        try:
            timeout_script = test_dir / "slow.py"
            timeout_script.write_text("""
import sys
import time
import signal

def handler(sig, frame):
    print("Received termination signal", file=sys.stderr)
    sys.exit(143)

signal.signal(signal.SIGTERM, handler)
print("Starting long operation...", file=sys.stderr)
time.sleep(30)  # Will timeout
print("This should not print", file=sys.stderr)
""")
            
            programs = [
                {"name": "timeout_process", "cmd": ["python3", str(timeout_script)], "timeout": 2.0},
            ]
            
            seq = ProcessSequencer(test_dir, programs, err_dir=err_dir)
            seq.start()
            
            # Check timeout was detected
            result = seq.get_result("timeout_process")
            assert result is not None, "Should have result for timeout_process"
            assert result['timed_out'] == True, "Should be marked as timed out"
            assert result['success'] == False, "Should not be successful"
            
            print(f"\nTimeout process result:")
            print(f"  Timed out: {result['timed_out']}")
            print(f"  Elapsed time: {result['elapsed_time']:.2f}s")
            print(f"  Output: {result['decoded_output'].strip()}")
            
            print(f"\n✓ Test 3 PASSED")
            
        finally:
            shutil.rmtree(test_dir)
        
        # Test 4: Stop on error vs continue on error
        print("\n" + "=" * 60)
        print("TEST 4: Stop on Error Behavior")
        print("=" * 60)
        
        test_dir = Path(tempfile.mkdtemp(prefix="ProcessSeq_test4_"))
        err_dir = test_dir / "errors"
        err_dir.mkdir()
        
        try:
            script = test_dir / "script.py"
            
            # Create scripts that will have different exit codes
            programs = []
            for i in range(5):
                script_name = f"script_{i}.py"
                script_path = test_dir / script_name
                exit_code = 0 if i != 2 else 1  # Make script 2 fail
                script_path.write_text(f"""
import sys
print("Running script {i}", file=sys.stderr)
sys.exit({exit_code})
""")
                programs.append({
                    "name": f"process_{i}", 
                    "cmd": ["python3", str(script_path)], 
                    "timeout": 5.0
                })
            
            # Test with continue_on_error=False (should stop at process_2)
            seq = ProcessSequencer(test_dir, programs, err_dir=err_dir, continue_on_error=False)
            seq.start()
            
            results = seq.get_results()
            print(f"\nWith continue_on_error=False:")
            print(f"  Executed: {len(results)} processes")
            print(f"  Stopped at: {seq.failed_process}")
            
            assert len(results) == 3, "Should have executed only 3 processes (0, 1, and 2)"
            assert seq.failed_process == "process_2", "Should have stopped at process_2"
            
            print(f"\n✓ Test 4 PASSED")
            
        finally:
            shutil.rmtree(test_dir)
        
        # Test 5: Empty stderr and various edge cases
        print("\n" + "=" * 60)
        print("TEST 5: Edge Cases and Empty Output")
        print("=" * 60)
        
        test_dir = Path(tempfile.mkdtemp(prefix="ProcessSeq_test5_"))
        err_dir = test_dir / "errors"
        err_dir.mkdir()
        
        try:
            # Script with no output
            silent_script = test_dir / "silent.py"
            silent_script.write_text("import sys; sys.exit(0)")
            
            # Script with Unicode output
            unicode_script = test_dir / "unicode.py"
            unicode_script.write_text("""
import sys
print("Hello 世界 🌍", file=sys.stderr)
print("Émojis: 🎉 🚀 ⚡", file=sys.stderr)
sys.exit(0)
""")
            
            # Script with multiline output
            multiline_script = test_dir / "multiline.py"
            multiline_script.write_text("""
import sys
for i in range(5):
    print(f"Line {i+1}", file=sys.stderr)
sys.exit(0)
""")
            
            programs = [
                {"name": "silent", "cmd": ["python3", str(silent_script)], "timeout": 5.0},
                {"name": "unicode", "cmd": ["python3", str(unicode_script)], "timeout": 5.0},
                {"name": "multiline", "cmd": ["python3", str(multiline_script)], "timeout": 5.0},
            ]
            
            seq = ProcessSequencer(test_dir, programs, err_dir=err_dir)
            seq.start()
            
            # Check each result
            silent_result = seq.get_result("silent")
            print(f"\nSilent script output: '{silent_result['decoded_output']}'")
            assert silent_result['success'] == True, "Silent script should succeed"
            
            unicode_result = seq.get_result("unicode")
            print(f"Unicode script output: {unicode_result['decoded_output'].strip()}")
            assert "世界" in unicode_result['decoded_output'], "Should handle Unicode"
            
            multiline_result = seq.get_result("multiline")
            lines = multiline_result['decoded_output'].strip().split('\n')
            print(f"Multiline script lines: {len(lines)}")
            assert len(lines) == 5, "Should have 5 lines"
            
            print(f"\n✓ Test 5 PASSED")
            
        finally:
            shutil.rmtree(test_dir)
        
        print("\n" + "=" * 80)
        print("ALL TESTS PASSED! ✓")
        print("=" * 80)
    
    def run_example_usage():
        """Demonstrate example usage of ProcessSequencer with result handling"""
        print("=" * 80)
        print("ProcessSequencer Example Usage")
        print("=" * 80)
        
        # Create temporary directory for example
        example_dir = Path(tempfile.mkdtemp(prefix="ProcessSeq_example_"))
        err_dir = example_dir / "logs"
        err_dir.mkdir()
        
        try:
            # Create example scripts simulating a build pipeline
            
            # Step 1: Initialize
            init_script = example_dir / "initialize.py"
            init_script.write_text("""
import sys
import time
print("[INIT] Setting up environment...", file=sys.stderr)
time.sleep(1)
print("[INIT] Environment ready", file=sys.stderr)
sys.exit(0)
""")
            
            # Step 2: Build
            build_script = example_dir / "build.py"
            build_script.write_text("""
import sys
import time
print("[BUILD] Compiling sources...", file=sys.stderr)
time.sleep(2)
print("[BUILD] Build successful", file=sys.stderr)
sys.exit(0)
""")
            
            # Step 3: Test
            test_script = example_dir / "test.py"
            test_script.write_text("""
import sys
import time
print("[TEST] Running unit tests...", file=sys.stderr)
time.sleep(1)
print("[TEST] All tests passed (42/42)", file=sys.stderr)
sys.exit(0)
""")
            
            # Step 4: Deploy
            deploy_script = example_dir / "deploy.py"
            deploy_script.write_text("""
import sys
import time
print("[DEPLOY] Deploying to production...", file=sys.stderr)
time.sleep(1)
print("[DEPLOY] Deployment complete", file=sys.stderr)
sys.exit(0)
""")
            
            # Define the pipeline
            pipeline = [
                {"name": "initialize", "cmd": ["python3", str(init_script)], "timeout": 10.0},
                {"name": "build", "cmd": ["python3", str(build_script)], "timeout": 30.0},
                {"name": "test", "cmd": ["python3", str(test_script)], "timeout": 60.0},
                {"name": "deploy", "cmd": ["python3", str(deploy_script)], "timeout": 30.0},
            ]
            
            print("\nStarting build pipeline...\n")
            
            # Custom error handler to show progress
            def pipeline_handler(name: str, json_data: str):
                data = json.loads(json_data)
                status = "✓" if data['return_code'] == 0 else "✗"
                print(f"\n{status} {name} completed in {data['elapsed_time']:.2f}s")
            
            # Run the pipeline
            sequencer = ProcessSequencer(
                example_dir, 
                pipeline, 
                err_dir=err_dir,
                error_handler=pipeline_handler,
                continue_on_error=False
            )
            
            sequencer.start()
            
            # Display results
            print("\n" + "=" * 60)
            print("PIPELINE RESULTS")
            print("=" * 60)
            
            for result in sequencer.get_results():
                print(f"\n{result['process']}:")
                print(f"  Status: {'SUCCESS' if result['success'] else 'FAILED'}")
                print(f"  Duration: {result['elapsed_time']:.2f}s")
                print(f"  Output:")
                for line in result['decoded_output'].strip().split('\n'):
                    print(f"    {line}")
            
            # Print summary
            sequencer.print_summary()
            
            # Save results to file
            results_path = example_dir / "pipeline_results.json"
            sequencer.save_results(results_path)
            
            # Demonstrate querying specific results
            print("\n" + "=" * 60)
            print("QUERYING SPECIFIC RESULTS")
            print("=" * 60)
            
            # Check if build was successful
            build_result = sequencer.get_result("build")
            if build_result and build_result['success']:
                print("✓ Build stage completed successfully")
            
            # Get test output
            test_output = sequencer.get_decoded_output("test")
            if test_output and "All tests passed" in test_output:
                print("✓ All tests passed")
            
            # Check overall success
            if sequencer.all_successful:
                print("\n🎉 Pipeline completed successfully!")
            else:
                print("\n❌ Pipeline failed")
                failed = sequencer.get_failed_processes()
                if failed:
                    print(f"Failed stages: {', '.join(f['process'] for f in failed)}")
            
        finally:
            # Cleanup
            shutil.rmtree(example_dir)
            print("\n" + "=" * 80)
            print("Example complete")
            print("=" * 80)
    
    # Main entry point
    if len(sys.argv) > 1:
        if sys.argv[1] == "--test":
            run_comprehensive_tests()
        elif sys.argv[1] == "--example":
            run_example_usage()
        else:
            print("Usage:")
            print("  python process_sequencer.py --test     # Run comprehensive tests")
            print("  python process_sequencer.py --example  # Run example pipeline")
            print("  python process_sequencer.py           # Run both tests and example")
    else:
        # Run both by default
        run_comprehensive_tests()
        print("\n" + "=" * 80 + "\n")
        run_example_usage()