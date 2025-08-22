
import json
import os
from pathlib import Path
from pydantic import BaseModel, AfterValidator, ValidationError
from typing import Annotated, Optional

# from dbx_tester.utils.databricks_api import get_notebook_path


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
    REPO_PATH: Optional[Path] = None
    TEST_CACHE_PATH: Annotated[Path, AfterValidator(create_if_not_exist)] = None
    LOG_PATH: Annotated[Path, AfterValidator(create_if_not_exist)] = None


    
class GlobalConfigManager:
    def __init__(self):
        self.config_path = Path("./Workspace/Shared/dbx_tester_cfg.json")
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.config_path.exists():
            with open(self.config_path, 'w') as f:
                json.dump({'dbx_tester':'v1'}, f)
                print("Writed")
        self._config = {}

    def add_config(self, test_path, cluster_id, repo_path = None, test_cache_path =None, log_path=None):
        try:
            if test_cache_path == None:
                test_cache_path = test_path

            if log_path == None:
                log_path = test_path


            with open(self.config_path, 'r+') as f:
                
                cfg = GlobalConfig(
                    TEST_PATH= Path(test_path),
                    CLUSTER_ID= cluster_id,
                    REPO_PATH = repo_path,
                    TEST_CACHE_PATH= Path(test_cache_path),
                    LOG_PATH= Path(log_path)   
                )
                glb_config = json.load(f)
                glb_config[cfg.TEST_PATH.as_posix()] = cfg.model_dump()
            
            with open(self.config_path, 'w') as f:
                json.dump(glb_config, f, indent=4)

        except Exception as err:
            raise Exception(f"CONFIG ERROR: Unable add config due to \n{err}")
    
    def _load_config(self):
        with open(self.config_path, 'r') as f:
            glb_config = dict(json.load(f))

        curr_path = Path(get_notebook_path())
        for i in glb_config.keys():
            if curr_path.is_relative_to(i):
                self._config = glb_config.get(i)
        else:
            raise Exception("No active config found")
    
    def _load_config_from_test_path(self, test_path):
        with open(self.config_path, 'r') as f:
            glb_config = dict(json.load(f))
        
        cfg = glb_config.get(test_path)
        if cfg is None:
            raise ValueError(f"Unable to find configuration in global config for path {test_path}")
        self._config = cfg
        
    @property
    def TEST_PATH(self):
        return self._config['TEST_PATH']
    
    @property
    def CLUSTER_ID(self):
        return self._config['CLUSTER_ID']
    
    @property
    def REPO_PATH(self):
        return self._config['REPO_PATH']
    
    @property
    def TEST_CACHE_PATH(self):
        return self._config['TEST_CACHE_PATH']
    
    @property
    def LOG_PATH(self):
        return self._config['LOG_PATH']
    


        
            