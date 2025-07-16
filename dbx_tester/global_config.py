class GlobalConfig:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GlobalConfig, cls).__new__(cls)
            cls._instance.REPO_PATH = False
        return cls._instance