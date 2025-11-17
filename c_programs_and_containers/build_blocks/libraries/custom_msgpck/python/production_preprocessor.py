#!/usr/bin/env python3
"""
Production Preprocessor Example

Demonstrates how to use MsgPackCodeGenerator in a production build system
that generates multiple configurations with a shared runtime.
"""

from msgpack_gen import MsgPackCodeGenerator, RuntimeGenerator
from pathlib import Path
import sys

class ProductionPreprocessor:
    """
    Production preprocessor for generating multiple MessagePack configurations
    with a shared runtime
    """
    
    def __init__(self, output_dir: str = "generated", 
                 runtime_name: str = "msgpack_runtime",
                 verbose: bool = False):
        """
        Initialize preprocessor
        
        Args:
            output_dir: Output directory for all generated files
            runtime_name: Name for shared runtime files
            verbose: Enable verbose output
        """
        self.output_dir = Path(output_dir)
        self.runtime_name = runtime_name
        self.verbose = verbose
        self.runtime = RuntimeGenerator()
        self.generated_configs = []
        
    def add_config(self, name: str, data: dict) -> None:
        """
        Add a configuration to generate
        
        Args:
            name: Configuration name (becomes C variable name)
            data: Configuration data (dict)
        """
        if self.verbose:
            print(f"Adding configuration: {name}")
        
        gen = MsgPackCodeGenerator(name, verbose=self.verbose)
        gen.load_dict(data)
        
        # Generate data files only
        files = gen.generate_data_only(str(self.output_dir))
        
        # Merge hashes into shared runtime
        self.runtime.merge(gen)
        
        self.generated_configs.append({
            'name': name,
            'files': files,
            'stats': gen.get_stats()
        })
        
        if self.verbose:
            print(f"  Generated {len(files)} files")
            print(f"  Stats: {gen.get_stats()}")
    
    def add_config_from_file(self, name: str, filepath: str) -> None:
        """
        Add configuration from JSON file
        
        Args:
            name: Configuration name
            filepath: Path to JSON file
        """
        gen = MsgPackCodeGenerator(name, verbose=self.verbose)
        gen.load_auto(filepath)
        
        files = gen.generate_data_only(str(self.output_dir))
        self.runtime.merge(gen)
        
        self.generated_configs.append({
            'name': name,
            'files': files,
            'stats': gen.get_stats()
        })
    
    def finalize(self) -> dict:
        """
        Finalize generation - creates shared runtime
        
        Returns:
            Dictionary with all generated file paths
        """
        if self.verbose:
            print(f"\nGenerating shared runtime: {self.runtime_name}")
            print(f"  Total unique hashes: {self.runtime.get_hash_count()}")
            print(f"  Configurations: {len(self.generated_configs)}")
        
        # Generate shared runtime
        runtime_files = self.runtime.generate(str(self.output_dir), 
                                             self.runtime_name)
        
        return {
            'configs': self.generated_configs,
            'runtime': runtime_files,
            'summary': {
                'config_count': len(self.generated_configs),
                'hash_count': self.runtime.get_hash_count(),
                'runtime_name': self.runtime_name
            }
        }
    
    def print_summary(self):
        """Print summary of generated files"""
        print("\n" + "="*70)
        print("PRODUCTION PREPROCESSOR SUMMARY")
        print("="*70)
        
        total_bytes = 0
        
        print(f"\nConfigurations ({len(self.generated_configs)}):")
        for config in self.generated_configs:
            print(f"\n  {config['name']}:")
            print(f"    Files: {', '.join(Path(f).name for f in config['files'].values())}")
            stats = config['stats']
            print(f"    Size:  {stats.total_bytes} bytes "
                  f"({stats.node_count} nodes, {stats.hashed_keys} keys)")
            total_bytes += stats.total_bytes
        
        print(f"\nShared Runtime:")
        print(f"  Name:  {self.runtime_name}")
        print(f"  Files: {self.runtime_name}.h, {self.runtime_name}.c")
        print(f"  Unique hashes: {self.runtime.get_hash_count()}")
        
        print(f"\nTotal data size: {total_bytes} bytes")
        print(f"Output directory: {self.output_dir}")
        print("\n" + "="*70)


def example_production_build():
    """Example production build with multiple configurations"""
    
    # Create preprocessor
    prep = ProductionPreprocessor(
        output_dir="build/generated",
        runtime_name="shared_msgpack_runtime",
        verbose=True
    )
    
    # Add configurations
    device_config = {
        "device_id": "DEV-12345",
        "firmware_version": 100,
        "network": {
            "wifi_ssid": "Company-IoT",
            "server_url": "https://api.example.com"
        }
    }
    
    sensor_config = {
        "sensors": {
            "temperature": {"enabled": True, "threshold": 85},
            "humidity": {"enabled": True, "threshold": 90},
            "pressure": {"enabled": False}
        },
        "sample_rate_ms": 1000
    }
    
    network_config = {
        "protocols": ["mqtt", "https"],
        "mqtt": {
            "broker": "mqtt.example.com",
            "port": 1883,
            "qos": 1
        },
        "https": {
            "timeout_ms": 5000,
            "retry_count": 3
        }
    }
    
    prep.add_config("device_config", device_config)
    prep.add_config("sensor_config", sensor_config)
    prep.add_config("network_config", network_config)
    
    # Finalize and generate shared runtime
    result = prep.finalize()
    
    # Print summary
    prep.print_summary()
    
    print("\nTo use in C code:")
    print("  #include \"device_config_data.h\"")
    print("  #include \"sensor_config_data.h\"")
    print("  #include \"network_config_data.h\"")
    print("  #include \"shared_msgpack_runtime.h\"")
    print()
    print("  device_config_init();")
    print("  sensor_config_init();")
    print("  network_config_init();")


if __name__ == "__main__":
    example_production_build()

