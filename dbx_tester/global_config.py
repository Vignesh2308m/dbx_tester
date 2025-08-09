import json
import os


class GlobalConfig:
    def __init__(self, config_path):
        self.config_path = "./"
        self._REPO_PATH = None
        self._TEST_PATH = None
        self._TEST_CACHE_PATH = None
        self._LOG_PATH = None
        
        if not os.path.exists(self.config_path):
            with open(self.config_path, "w") as f:
                json.dump({}, f)

    @property
    def REPO_PATH(self):
        if os.path.exists(self.config_path):
            with open(self.config_path, "r") as f:
                config = json.load(f)
                return config.get("REPO_PATH")
        return self._REPO_PATH

    @REPO_PATH.setter
    def REPO_PATH(self, value):
        self._REPO_PATH = value
        self.save()

    @property
    def TEST_PATH(self):
        if os.path.exists(self.config_path):
            with open(self.config_path, "r") as f:
                config = json.load(f)
                return config.get("TEST_PATH")
        return self._TEST_PATH

    @TEST_PATH.setter
    def TEST_PATH(self, value):
        self._TEST_PATH = value
        self.save()

    @property
    def TEST_CACHE_PATH(self):
        if os.path.exists(self.config_path):
            with open(self.config_path, "r") as f:
                config = json.load(f)
                return config.get("TEST_CACHE_PATH")
        return self._TEST_CACHE_PATH

    @TEST_CACHE_PATH.setter
    def TEST_CACHE_PATH(self, value):
        self._TEST_CACHE_PATH = value
        self.save()

    @property
    def LOG_PATH(self):
        if os.path.exists(self.config_path):
            with open(self.config_path, "r") as f:
                config = json.load(f)
                return config.get("LOG_PATH")
        return self._LOG_PATH

    @LOG_PATH.setter
    def LOG_PATH(self, value):
        self._LOG_PATH = value
        self.save()
    
    def save(self):
        config = {
            "REPO_PATH": self._REPO_PATH,
            "TEST_PATH": self._TEST_PATH,
            "TEST_CACHE_PATH": self._TEST_CACHE_PATH,
            "LOG_PATH": self._LOG_PATH
        }
        with open(self.config_path, "w") as f:
            json.dump(config, f)
