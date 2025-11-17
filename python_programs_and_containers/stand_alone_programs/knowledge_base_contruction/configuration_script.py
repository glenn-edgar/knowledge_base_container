from building_blocks.yaml_generator.data_structures import DataStructures
from building_blocks.kb_build.parts.part_accumulator import PartAccumulator
from pathlib import Path

if __name__ == "__main__":
    ds = DataStructures(yaml_file=Path("/home/gedgar/mount_startup/kb_definitions.yaml"), starting_kb="kb_version")
    pa = PartAccumulator(ds, starting_kb="kb_version",version="1.0.0")
    
   
   
    pa.add_kb("software_resources")
    pa.select_kb("software_resources")
    pa.define_systems(systems_name="systems1")
    pa.define_system(system_name="system1", system_description="system1", system_meta_data={"description": "system1"}, system_data={"description": "system1"})
    pa.define_container(container_name="container1", container_description="container1")
    pa.leave_system(system_name="system1")
    pa.leave_systems(systems_name="systems1")
    pa.leave_kb()
    
    pa.add_kb("hardware_resources")
    pa.select_kb("hardware_resources")
    pa.define_system(system_name="system1", system_description="system1", system_meta_data={"description": "system1"}, system_data={"description": "system1"})
    
    pa.define_site(site_name="site1", site_description="site1", nodes=["node1"], master_node="node1", site_meta_data={}, site_data={})
    
    pa.define_master(master_name="master1", master_description="master1", master_meta_data={}, master_data={})
    pa.use_container(container_name="postgres-vector")
    pa.use_container(container_name="nats-js-ram")
    pa.leave_master(master_name="master1")
    
    pa.define_node(node_name="node1", node_description="node1", architecture={"arch":"ARM64","ram":"64GB" }, node_meta_data={}, node_data={})
    
    
    
    pa.leave_node(node_name="node1")
    pa.leave_site(site_name="site1")
    pa.leave_system(system_name="system1")
    
    pa.leave_kb()
   
    
    
    pa.generate_yaml()
    