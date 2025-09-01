import json
import os
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

from dbx_tester.utils.databricks_api import get_notebook_path


def is_valid_path(value: Path) -> Path:
    """Validates if the given path exists and is a directory.

    Args:
        value (Path): The path to validate.

    Returns:
        Path: The validated path.

    Raises:
        ValueError: If the path does not exist or is not a directory.
    """
    if not Path(value).exists():
        raise ValueError(f"{value} path not exists")
    if not Path(value).is_dir():
        raise ValueError(f"{value} is not a directory")


def create_if_not_exist(value: Path) -> Path:
    """Creates the directory if it does not exist.

    Args:
        value (Path): The path to create.

    Returns:
        Path: The created or existing path.

    Raises:
        Exception: If unable to create the path.
    """
    try:
        if not Path(value).exists():
            Path(value).mkdir(parents=True)
    except Exception as err:
        raise Exception(f"{value} unable to create the path")


def validate(value, validator):
    """Validates a value using a specified validator function.

    Args:
        value: The value to validate.
        validator: The validator function to use.

    Returns:
        The validated value.
    """
    validator(value)
    return value


@dataclass
class GlobalConfig:
    TEST_PATH: str
    CLUSTER_ID: str
    REPO_PATH: Optional[str] = None
    TEST_CACHE_PATH: str = None
    LOG_PATH: str = None


class GlobalConfigManager:
    def __init__(self):
        """Initializes the GlobalConfigManager and ensures the config path exists."""
        self.config_path = Path("/Workspace/Shared/dbx_tester_cfg.json")
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.config_path.exists():
            with open(self.config_path, 'w') as f:
                json.dump({'dbx_tester': 'v1'}, f)
                print("Writed")
        self._config = {}

    def add_config(self, test_path, cluster_id=None, repo_path=None, test_cache_path=None, log_path=None):
        """Adds a new configuration to the global config file.

        Args:
            test_path (str): The test path.
            cluster_id (str, optional): The cluster ID.
            repo_path (str, optional): The repository path.
            test_cache_path (str, optional): The test cache path.
            log_path (str, optional): The log path.

        Raises:
            Exception: If unable to add the configuration.
        """
        try:
            if test_cache_path is None:
                test_cache_path = test_path

            if log_path is None:
                log_path = test_path

            cfg = GlobalConfig(
                TEST_PATH=validate(test_path, is_valid_path),
                CLUSTER_ID=cluster_id,
                REPO_PATH=repo_path,
                TEST_CACHE_PATH=validate(test_cache_path, create_if_not_exist),
                LOG_PATH=validate(log_path, create_if_not_exist)
            )

            with open(self.config_path, 'r+') as f:
                glb_config = json.load(f)
                glb_config[cfg.TEST_PATH] = asdict(cfg)

            with open(self.config_path, 'w') as f:
                json.dump(glb_config, f, indent=4)

        except Exception as err:
            raise Exception(f"CONFIG ERROR: Unable add config due to \n{err}")

    def _load_config(self):
        """Loads the active configuration based on the current notebook path.

        Raises:
            Exception: If no active configuration is found.
        """
        with open(self.config_path, 'r') as f:
            glb_config = dict(json.load(f))

        curr_path = Path(get_notebook_path())
        for i in glb_config.keys():
            if curr_path.is_relative_to(i):
                self._config = glb_config.get(i)
                break
        else:
            raise Exception("No active config found")

    def _load_config_from_test_path(self, test_path):
        """Loads a configuration from the global config using a test path.

        Args:
            test_path (str): The test path to load the configuration for.

        Raises:
            ValueError: If the configuration for the test path is not found.
        """
        with open(self.config_path, 'r') as f:
            glb_config = dict(json.load(f))

        cfg = glb_config.get(test_path)
        if cfg is None:
            raise ValueError(f"Unable to find configuration in global config for path {test_path}")
        self._config = cfg

    @property
    def TEST_PATH(self):
        """str: The test path from the active configuration."""
        if self._config == {}:
            self._load_config()
        return self._config['TEST_PATH']

    @property
    def CLUSTER_ID(self):
        """str: The cluster ID from the active configuration."""
        if self._config == {}:
            self._load_config()
        return self._config['CLUSTER_ID']

    @property
    def REPO_PATH(self):
        """str: The repository path from the active configuration."""
        if self._config == {}:
            self._load_config()
        return self._config['REPO_PATH']

    @property
    def TEST_CACHE_PATH(self):
        """str: The test cache path from the active configuration."""
        if self._config == {}:
            self._load_config()
        return self._config['TEST_CACHE_PATH']

    @property
    def LOG_PATH(self):
        """str: The log path from the active configuration."""
        if self._config == {}:
            self._load_config()
        return self._config['LOG_PATH']