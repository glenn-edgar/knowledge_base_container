import json
import time
from pathlib import Path
from typing import List, Tuple
import sys



home_dir = Path.home()
build_block_path = home_dir/ "knowledge_base_assembly" / "python_programs_and_containers" / "building_blocks"
sys.path.append(str(build_block_path))

from process_manager.process_sequencer.process_sequencer import ProcessSequencer
from process_manager.process_scheduler.process_scheduler import ProcessScheduler




def start_initialization_tasks():
    base_path = home_dir / "knowledge_base_assembly" / "python_programs_and_containers"/"stand_alone_programs"
    programs = {}
    programs["start_db_containers"] = (["python3", "start_db_containers/start_db_containers.py"], 20)
    programs["knowledge_base_model_interface"] = (["python3", "knowledge_base_load/cold_start.py","--force-update"], 15)
    
    error_handler = lambda err, out: print(f"Error: {err}\nOutput: {out}")
    process_sequencer = ProcessSequencer(
        base_dir=str(base_path),
        programs=programs,
        err_dir= Path("/tmp/node_control"),
        error_handler=None,
        continue_on_error=False)
    process_sequencer.start()
   
    
def main():
    start_initialization_tasks()
    
    
if __name__ == "__main__":
  main()



