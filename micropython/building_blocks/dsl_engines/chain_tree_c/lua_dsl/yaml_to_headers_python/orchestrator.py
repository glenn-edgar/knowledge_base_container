"""
ChainTree Pipeline Orchestrator

Coordinates the 6-stage pipeline for generating C header files from ChainTree YAML:

  Stage 1: Load data (JSON or YAML)      (stage1_handle.py)
  Stage 2: Build node ordering/indices  (stage2_node_index.py)
  Stage 3: Build function indices       (stage3_function_index.py)
  Stage 4: Build link tables            (stage4_link_table.py)
  Stage 5: Encode node data (JSON)      (stage5_node_data.py)
  Stage 6: Generate C header/impl files (stage6_codegen.py)

Usage:
    from chaintree_pipeline import PipelineOrchestrator
    
    orchestrator = PipelineOrchestrator(
        yaml_file=Path("chaintree_config.yaml"),
        handle_name="my_chaintree"
    )
    orchestrator.run()
"""

import random
import string
from pathlib import Path
from typing import Dict, Optional

from .stage1_handle import ChainTreeHandle
from .stage2_node_index import NodeIndexBuilder
from .stage3_function_index import FunctionIndexBuilder
from .stage4_link_table import LinkTableBuilder
from .stage5_node_data import NodeDataEncoder
from .stage6_codegen import CCodeGenerator


class PipelineOrchestrator:
    """
    Main pipeline orchestrator for ChainTree header file generation.
    
    Runs all 6 stages in dependency order and produces matched .h/.c pairs.
    """
    
    def __init__(
        self,
        yaml_file: Path,
        handle_name: str,
        output_dir: Optional[Path] = None,
        generate_support_header: bool = True,
    ):
        """
        Args:
            yaml_file: Path to ChainTree YAML configuration
            handle_name: Name for the handle type (used in all generated symbols)
            output_dir: Directory for generated .h/.c files (defaults to yaml_file's directory)
            generate_support_header: Whether to generate chaintree_support.h/.c
        """
        self.yaml_file = yaml_file
        self.handle_name = handle_name
        self.output_dir = Path(output_dir) if output_dir else yaml_file.parent
        self.generate_support_header = generate_support_header
        
        # Generate unique 8-character identifier for symbol namespacing
        self.unique_id = 'ct_' + ''.join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        
        # Stage objects (populated during run)
        self.handle: Optional[ChainTreeHandle] = None
        self.node_builder: Optional[NodeIndexBuilder] = None
        self.function_builder: Optional[FunctionIndexBuilder] = None
        self.link_builder: Optional[LinkTableBuilder] = None
        self.data_encoder: Optional[NodeDataEncoder] = None
        self.code_generator: Optional[CCodeGenerator] = None
        
        # Main function usage tracking (computed between stages 3 and 6)
        self.main_function_usage: Dict[int, int] = {}
    
    def run(self) -> None:
        """Execute the complete 6-stage pipeline."""
        
        print("=" * 70)
        print("ChainTree Pipeline")
        print("=" * 70)
        print(f"  YAML file:   {self.yaml_file}")
        print(f"  Output dir:  {self.output_dir}")
        print(f"  Handle name: {self.handle_name}")
        print(f"  Unique ID:   {self.unique_id}")
        print()
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self._run_stage1()
        self._run_stage2()
        self._run_stage3()
        self._run_stage4()
        self._run_stage5()
        self._count_main_function_usage()
        self._run_stage6()
        
        print()
        print("=" * 70)
        print("✓ Pipeline completed successfully!")
        print("=" * 70)
    
    # =========================================================================
    # Individual Stage Runners
    # =========================================================================
    
    def _run_stage1(self) -> None:
        """Stage 1: Load YAML data into handle."""
        print("Stage 1: Loading YAML data...")
        self.handle = ChainTreeHandle(self.yaml_file)
        self.handle.print_summary()
    
    def _run_stage2(self) -> None:
        """Stage 2: Build node ordering and indices."""
        print("\nStage 2: Building node ordering...")
        self.node_builder = NodeIndexBuilder(self.handle)
        self.node_builder.build()
        self.node_builder.print_summary()
    
    def _run_stage3(self) -> None:
        """Stage 3: Build function indices."""
        print("\nStage 3: Building function indices...")
        self.function_builder = FunctionIndexBuilder(self.handle)
        self.function_builder.build()
        self.function_builder.print_summary()
    
    def _run_stage4(self) -> None:
        """Stage 4: Build link tables."""
        print("\nStage 4: Building link tables...")
        self.link_builder = LinkTableBuilder(self.handle, self.node_builder)
        self.link_builder.build()
        self.link_builder.print_summary()
    
    def _run_stage5(self) -> None:
        """Stage 5: Encode node data."""
        print("\nStage 5: Encoding node data...")
        self.data_encoder = NodeDataEncoder(
            self.handle, self.node_builder, self.function_builder
        )
        self.data_encoder.build()
        self.data_encoder.print_summary()
    
    def _count_main_function_usage(self) -> None:
        """Count how many nodes reference each main function (between stages 5 and 6)."""
        print("\nCounting main function usage...")
        
        for i in range(self.function_builder.main_indexer.get_count()):
            self.main_function_usage[i] = 0
        
        for ltree_name in self.node_builder.ltree_to_final_index.keys():
            functions = self.handle.get_node_functions(ltree_name)
            main_func = functions.get('main', 'CFL_NULL')
            
            if main_func and main_func != 'CFL_NULL':
                try:
                    func_index = self.function_builder.main_indexer.get_index(main_func)
                    self.main_function_usage[func_index] += 1
                except KeyError:
                    pass
        
        total = sum(self.main_function_usage.values())
        print(f"  Total main function references: {total}")
    
    def _run_stage6(self) -> None:
        """Stage 6: Generate C header and implementation files."""
        print("\nStage 6: Generating C header and implementation files...")
        
        self.code_generator = CCodeGenerator(
            output_dir=self.output_dir,
            handle_name=self.handle_name,
            unique_id=self.unique_id,
            handle=self.handle,
            node_builder=self.node_builder,
            function_builder=self.function_builder,
            link_builder=self.link_builder,
            data_encoder=self.data_encoder,
            main_function_usage=self.main_function_usage,
            generate_support=self.generate_support_header,
        )
        
        self.code_generator.generate_all()


# =========================================================================
# CLI Entry Point
# =========================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python -m chaintree_pipeline.orchestrator <yaml_file> <output_dir> [handle_name] [--no-support]")
        print("")
        print("  yaml_file    : Path to ChainTree YAML configuration")
        print("  output_dir   : Directory for generated .h/.c files")
        print("  handle_name  : Name for the handle type (default: chaintree_handle)")
        print("  --no-support : Skip generating chaintree_support.h/.c")
        sys.exit(1)
    
    yaml_file = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    
    # Parse optional positional and flag arguments
    handle_name = "chaintree_handle"
    generate_support = True
    
    remaining = sys.argv[3:]
    for arg in remaining:
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