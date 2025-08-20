
import json
import os
from pathlib import Path


class GlobalConfig:
    def __init__(self):
        self.config_path = Path('/Workspace/Shared') / "dbx_tester_cfg.json"
        self._config = {}
        self._load_config()

    def _load_config(self):
        """Load configuration from file or create empty config if file doesn't exist."""
        if self.config_path.exists():
            with open(self.config_path, "r") as f:
                self._config = json.load(f)
        else:
            self._config = {}
            self._save_config()

    def _save_config(self):
        """Save the current configuration to file."""
        with open(self.config_path, "w") as f:
            json.dump(self._config, f, indent=2)

    @property
    def REPO_PATH(self):
        return self._config.get("REPO_PATH")

    @REPO_PATH.setter
    def REPO_PATH(self, value):
        self._config["REPO_PATH"] = value
        self._save_config()

    @property
    def TEST_PATH(self):
        return self._config.get("TEST_PATH")

    @TEST_PATH.setter
    def TEST_PATH(self, value):
        self._config["TEST_PATH"] = value
        self._save_config()

    @property
    def TEST_CACHE_PATH(self):
        return self._config.get("TEST_CACHE_PATH")

    @TEST_CACHE_PATH.setter
    def TEST_CACHE_PATH(self, value):
        self._config["TEST_CACHE_PATH"] = value
        self._save_config()

    @property
    def LOG_PATH(self):
        return self._config.get("LOG_PATH")

    @LOG_PATH.setter
    def LOG_PATH(self, value):
        self._config["LOG_PATH"] = value
        self._save_config()

    @property
    def CLUSTER_ID(self):
        return self._config.get("CLUSTER_ID")

    @CLUSTER_ID.setter
    def CLUSTER_ID(self, value):
        self._config["CLUSTER_ID"] = value
        self._save_config()