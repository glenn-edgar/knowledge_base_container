from .yaml_generator.yaml_generator import YamlGenerator
from pathlib import Path
from .parts.parts_assembly import PartsAssembly

class MasterBuild:
    def __init__(self, yaml_file: str,  part_assembly_name: str, deploy_assembly_name: str,path_list: list=[]):
        self.yaml_file = Path(yaml_file)
        if not self.yaml_file.parent.exists():
            raise FileNotFoundError(f"Parent directory for yaml file does not exist: {self.yaml_file.parent}")
        self.path_list = path_list
        self.yaml_generator = YamlGenerator(yaml_file=self.yaml_file, path_list=self.path_list)
        self.parts_assembly = PartsAssembly(yaml_generator=self.yaml_generator,parts_assembly_name=part_assembly_name)
        
        
        
        
if __name__ == "__main__":
    master_build = MasterBuild("config.yaml","test_part_assembly","test_deploy_assembly")
    
    
    master_build.parts_assembly.start_parts_assembly()
    master_build.parts_assembly.end_parts_assembly()
    master_build.yaml_generator.generate_yaml()
    print("made it here")
    
