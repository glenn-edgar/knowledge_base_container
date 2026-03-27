"""
Entry point for: python -m yaml_to_headers_python <input_file> <output_dir> [handle_name] [--no-support]
"""
import sys
from pathlib import Path
from .orchestrator import PipelineOrchestrator

def main():
    if len(sys.argv) < 3:
        print("Usage: python -m yaml_to_headers_python <input_file> <output_dir> [handle_name] [--no-support]")
        print("")
        print("  input_file   : Path to ChainTree .json or .yaml configuration")
        print("  output_dir   : Directory for generated .h/.c files")
        print("  handle_name  : Name for the handle type (default: chaintree_handle)")
        print("  --no-support : Skip generating chaintree_support.h/.c")
        sys.exit(1)
    
    yaml_file = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    
    handle_name = "chaintree_handle"
    generate_support = True
    
    for arg in sys.argv[3:]:
        if arg == "--no-support":
            generate_support = False
        else:
            handle_name = arg
    
    if not yaml_file.exists():
        print(f"Error: {yaml_file} not found.")
        sys.exit(1)
    
    orchestrator = PipelineOrchestrator(
        yaml_file=yaml_file,
        handle_name=handle_name,
        output_dir=output_dir,
        generate_support_header=generate_support,
    )
    orchestrator.run()

main()