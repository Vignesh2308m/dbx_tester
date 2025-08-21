
import json
import os
from pathlib import Path
from pydantic import BaseModel, AfterValidator, ValidationError
from typing import Annotated

def is_valid_path(value:Path) -> Path:
    if not value.exists():
        raise ValueError(f"{value} path not exists")
    if not value.is_dir():
        raise ValueError(f"{value} is not a directory")
    return value

def create_if_not_exist(value:Path) -> Path:
    try:
        if not value.exists():
            value.mkdir(parents=True)
    except Exception as err:
        raise Exception(f"{value} unable to create the path")
        

class GlobalConfig(BaseModel):
    TEST_PATH: Annotated[Path, AfterValidator(is_valid_path)]
    CLUSTER_ID: str
    REPO_PATH: Path = None
    TEST_CACHE_PATH: Annotated[Path, AfterValidator(create_if_not_exist)] = None
    LOG_PATH: Annotated[Path, AfterValidator(create_if_not_exist)] = None


    
class GlobalConfigManager:
    def __init__(self):
        self.config_path = Path("/Workspace/Shared/dbx_tester_cfg.json")

    
    def add_config(self, test_path, cluster_id, repo_path = None, test_cache_path =None, log_path=None):
        try:
            with open(self.config_path, 'w') as f:

                cfg = GlobalConfig(
                    TEST_PATH=test_path,
                    CLUSTER_ID=cluster_id,
                    REPO_PATH = repo_path,
                    TEST_CACHE_PATH= test_cache_path,
                    LOG_PATH= log_path   
                )
                glb_config = json.load(f)
                glb_config[cfg.TEST_PATH] = cfg.model_dump()
                json.dump(glb_config, f, indent=4)

        except Exception as err:
            pass