from pathlib import Path
import yaml
from building_blocks.libraries.yaml_handler.yaml_handler import YAMLHandler

yaml_handler = YAMLHandler()

class DecodeConfigurationYAML:
    def __init__(self, yaml_file: Path):
        self.yaml_file = yaml_file

    def decode_yaml(self):
        yaml_data = yaml_handler.decode_yaml_file(self.yaml_file)
        return yaml_data
    

if __name__ == "__main__":
    decode_yaml = DecodeConfigurationYAML(Path("/home/gedgar/mount_startup/kb_settings.yaml"))
    yaml_data = decode_yaml.decode_yaml()
    print(yaml_data)