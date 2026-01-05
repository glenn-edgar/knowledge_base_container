from yaml_to_c_h.header_file_generator import HeaderFileGenerator
from pathlib import Path

def main(yaml_file, handle_name, generate_support_header):
    header_file_generator = HeaderFileGenerator(yaml_file, handle_name, generate_support_header)
    header_file_generator.run_pipeline()
    


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python chain_tree_yaml_header_test.py <yaml_file> <handle_name>  <generate_support_header>")
        sys.exit(1)
    yaml_file = Path(sys.argv[1])
    handle_name = sys.argv[2]


    print( sys.argv[3] )    
    generate_support_header = sys.argv[3] == "True"
    main(yaml_file, handle_name, generate_support_header)
