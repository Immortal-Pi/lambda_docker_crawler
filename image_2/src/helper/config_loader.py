import yaml 
import os 

def load_config(config_path: str='config.yaml')-> dict:
    with open(config_path,'r') as file:
        config=yaml.safe_load(file)
    return config 