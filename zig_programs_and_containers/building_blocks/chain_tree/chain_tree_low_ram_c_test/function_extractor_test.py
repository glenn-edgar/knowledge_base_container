from chain_tree_c_low_ram.yaml_to_c_h.c_function_extractor import CFunctionRegistry
from chain_tree_c_low_ram.yaml_to_c_h.string_indexer import StringIndexer


def main():
    registry = CFunctionRegistry()
    string_indexer = StringIndexer()
    registry.parse_file("chain_tree_c_low_ram/c_functions/cfl_functions/one_shot/cfl_one_shot_functions.c")
    print(registry)
       # Iterate over all functions
    for name, func in registry.items():
        print(f"--- {name} ---")
        print(f"Prototype: {func['prototype']}")
        print(func['code'])
        string_indexer.add_string(func['code'])
        print(string_indexer.get_index(func['code']))
        print()
        try:
            registry.output_files(
                function_names=["cfl_log_message", "cfl_column_init", "cfl_column_termination"],
                c_file_path="output.c",
                h_file_path="output.h",
                h_includes=["<stdio.h>", "<stdint.h>", '"types.h"'],
                c_includes=["<string.h>", "<stdlib.h>", '"utils.h"']
            )
            print("Files generated successfully!")
        except KeyError as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
    