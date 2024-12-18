from dataclasses import dataclass
from typing import List, Dict
import pandas as pd
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CPUSpec:
    # Required
    core_count: int
    core_speed: int  # MHz
    # Optional
    name : str = None
    vendor : str = None
    arch : str = None
    count : int = None
@dataclass
class MemorySpec:
    # Required
    memory_size: int  # Bytes
    # Optional
    name : str = None
    vendor : str = None
    arch : str = None
    count : int = None
    memorySpeed : int = None

@dataclass
class PowerSpec:
    # Required
    vendor: str  = None
    modelName: str = None
    arch: str = None
    # Optional
    idle_power: float = None  # Watts
    max_power: float = None  # Watts
    carbonTracePath: str = None

@dataclass
class ServerSpec:
    manufacturer: str
    model: str
    cpu: CPUSpec
    memory: MemorySpec
    power: PowerSpec

class ServerSpecRegistry:
    def __init__(self):
        self.specs: Dict[str, ServerSpec] = {}
    
    def add_spec(self, manufacturer: str, model: str, spec: ServerSpec):
        key = f"{manufacturer}-{model}"
        self.specs[key] = spec
    
    def get_spec(self, manufacturer: str, model: str) -> ServerSpec:
        key = f"{manufacturer}-{model}"
        return self.specs.get(key)

class DataCenterConverter:
    def __init__(self, spec_registry: ServerSpecRegistry):
        self.spec_registry = spec_registry
        self.cluster_counts = {}

    def process_excel_data(self, df: pd.DataFrame) -> dict:
        clusters = []
        
        # Group by manufacturer and model
        grouped = df.groupby(['MANUFACTURER', 'MODEL']).size().reset_index(name='count')
        
        for idx, row in grouped.iterrows():
            manufacturer = row['MANUFACTURER']
            model = row['MODEL']
            
            # Skip if not ZANTAZ
            if manufacturer.strip().upper() != "ZANTAZ":
                logger.info(f"Skipping non-ZANTAZ entry: {manufacturer} {model}")
                continue
                
            count = row['count']
            
            spec = self.spec_registry.get_spec(manufacturer, model)
            if not spec:
                logger.warning(f"No specification found for ZANTAZ model: {model}")
                continue
            
            logger.info(f"Processing ZANTAZ model: {model} with count: {count}")
            
            cluster_name = f"{manufacturer}-{model}"
            
            # Create cluster configuration
            cluster = {
                "name": cluster_name,
                "count": int(count),
                "hosts": [
                    {
                        "count": 1,
                        "cpu": 
                            {   
                                "coreCount": spec.cpu.core_count,
                                "coreSpeed": spec.cpu.core_speed
                            },
                        "memory": {
                            "memorySize": spec.memory.memory_size
                        },
                        "powerModel": {
                            "power": spec.power.power
                        }
                    }
                ]
            }
            
            clusters.append(cluster)
        
        return {"clusters": clusters}

def create_sample_registry():
    registry = ServerSpecRegistry()
    
    # Example spec for ZANTAZ Z2-059-0-HP
    zantaz_spec = ServerSpec(
        manufacturer="ZANTAZ",
        model="Z2-059-0-HP",
        cpu=CPUSpec(
            core_count=16,
            core_speed=2400
        ),
        memory=MemorySpec(
            memory_size=128 * 1024 * 1024 * 1024  # 128GB in bytes
        ),
        power=PowerSpec(
            # model_type="linear",
            idle_power=200.0,
            max_power=400.0
        )
    )
    
    registry.add_spec("ZANTAZ", "Z2-059-0-HP", zantaz_spec)
    return registry

def main():
    try:
        # Create registry and add specifications
        registry = create_sample_registry()
        
        # Create converter
        converter = DataCenterConverter(registry)
        
        # Read Excel file
        print("Reading Excel file...")
        df = pd.read_excel("citi_data/citiHardwareSheet.xlsx")
        
        # Process data
        print("Processing data...")
        result = converter.process_excel_data(df)
        
        # Save to JSON file
        print("Saving results to JSON...")
        with open("datacenter_config.json", "w") as f:
            json.dump(result, f, indent=4)
            
        print("Conversion completed successfully!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
    #./OpenDCExperimentRunner/bin/OpenDCExperimentRunner --experiment-path "experiments/simple_experiment.json"
    # This was too run via command line
    