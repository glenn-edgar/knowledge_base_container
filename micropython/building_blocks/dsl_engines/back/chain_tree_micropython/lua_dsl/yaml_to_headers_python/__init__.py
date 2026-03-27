"""
ChainTree Pipeline - JSON/YAML to C Header Generation

A 6-stage pipeline that transforms ChainTree configurations
into embedded-ready C header and implementation files.

Usage:
    from yaml_to_headers_python.orchestrator import PipelineOrchestrator

Stages:
    1. stage1_handle         - Load and organize input data (JSON or YAML)
    2. stage2_node_index     - Build node ordering and indices
    3. stage3_function_index - Build function index tables
    4. stage4_link_table     - Build parent-child link tables
    5. stage5_node_data      - Encode node data as JSON records
    6. stage6_codegen        - Generate all C header/impl files
"""