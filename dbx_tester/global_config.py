import json
import os
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
import logging
from contextlib import contextmanager

from dbx_tester.utils.databricks_api import get_notebook_path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass


class PathValidationError(ConfigurationError):
    """Custom exception for path validation errors."""
    pass


@dataclass
class GlobalConfig:
    """Data class representing global configuration settings."""
    TEST_PATH: str
    CLUSTER_ID: str
    REPO_PATH: Optional[str] = None
    TEST_CACHE_PATH: Optional[str] = None
    LOG_PATH: Optional[str] = None

    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate_required_fields()
        self._set_default_paths()

    def _validate_required_fields(self) -> None:
        """Validate required configuration fields."""
        if not self.TEST_PATH or not self.TEST_PATH.strip():
            raise ConfigurationError("TEST_PATH is required and cannot be empty")
        
        if not self.CLUSTER_ID or not self.CLUSTER_ID.strip():
            raise ConfigurationError("CLUSTER_ID is required and cannot be empty")

    def _set_default_paths(self) -> None:
        """Set default paths if not provided."""
        if self.TEST_CACHE_PATH is None:
            self.TEST_CACHE_PATH = self.TEST_PATH
        
        if self.LOG_PATH is None:
            self.LOG_PATH = self.TEST_PATH

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GlobalConfig':
        """Create configuration from dictionary."""
        return cls(**data)


class PathValidator:
    """Utility class for path validation and creation."""
    
    @staticmethod
    def validate_existing_directory(path: Path) -> Path:
        """Validate that the path exists and is a directory.
        
        Args:
            path: The path to validate.
            
        Returns:
            The validated path.
            
        Raises:
            PathValidationError: If the path doesn't exist or isn't a directory.
        """
        path_obj = Path(path)
        
        if not path_obj.exists():
            raise PathValidationError(f"Path does not exist: {path}")
        
        if not path_obj.is_dir():
            raise PathValidationError(f"Path is not a directory: {path}")
        
        return path_obj

    @staticmethod
    def create_directory_if_not_exists(path: Path) -> Path:
        """Create directory if it doesn't exist.
        
        Args:
            path: The path to create.
            
        Returns:
            The created or existing path.
            
        Raises:
            PathValidationError: If unable to create the directory.
        """
        path_obj = Path(path)
        
        try:
            if not path_obj.exists():
                path_obj.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory: {path_obj}")
            return path_obj
        except (OSError, PermissionError) as e:
            raise PathValidationError(f"Unable to create directory {path}: {e}")

    @staticmethod
    def validate_and_apply(value: Any, validator_func) -> Any:
        """Apply a validator function to a value.
        
        Args:
            value: The value to validate.
            validator_func: The validator function to apply.
            
        Returns:
            The validated value.
        """
        validator_func(value)
        return value


class ConfigFileManager:
    """Handles configuration file operations."""
    
    def __init__(self, config_path: Path):
        self.config_path = Path(config_path)
        self._ensure_config_file_exists()

    def _ensure_config_file_exists(self) -> None:
        """Ensure the configuration file and its parent directory exist."""
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            
            if not self.config_path.exists():
                self._create_initial_config()
                logger.info(f"Created initial configuration file: {self.config_path}")
        except (OSError, PermissionError) as e:
            raise ConfigurationError(f"Unable to create config file: {e}")

    def _create_initial_config(self) -> None:
        """Create initial configuration file with default structure."""
        initial_config = {'dbx_tester': 'v1'}
        self.write_config(initial_config)

    @contextmanager
    def _file_lock(self, mode: str = 'r'):
        """Context manager for file operations with proper error handling."""
        try:
            with open(self.config_path, mode) as file:
                yield file
        except (IOError, json.JSONDecodeError) as e:
            raise ConfigurationError(f"File operation failed: {e}")

    def read_config(self) -> Dict[str, Any]:
        """Read configuration from file.
        
        Returns:
            Dictionary containing the configuration data.
            
        Raises:
            ConfigurationError: If unable to read or parse the configuration.
        """
        with self._file_lock('r') as file:
            try:
                return json.load(file)
            except json.JSONDecodeError as e:
                raise ConfigurationError(f"Invalid JSON in config file: {e}")

    def write_config(self, config_data: Dict[str, Any]) -> None:
        """Write configuration to file.
        
        Args:
            config_data: Dictionary containing the configuration data.
            
        Raises:
            ConfigurationError: If unable to write the configuration.
        """
        with self._file_lock('w') as file:
            json.dump(config_data, file, indent=4, sort_keys=True)

    def update_config(self, key: str, value: Any) -> None:
        """Update a specific configuration entry.
        
        Args:
            key: The configuration key to update.
            value: The new value for the configuration key.
        """
        config_data = self.read_config()
        config_data[key] = value
        self.write_config(config_data)


class GlobalConfigManager:
    """Manages global configuration settings for the DBX tester."""
    
    DEFAULT_CONFIG_PATH = Path("/Workspace/Shared/dbx_tester_cfg.json")
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize the GlobalConfigManager.
        
        Args:
            config_path: Optional custom path for the configuration file.
        """
        self.config_path = config_path or self.DEFAULT_CONFIG_PATH
        self.file_manager = ConfigFileManager(self.config_path)
        self._config: Optional[GlobalConfig] = None

    def add_config(
        self,
        test_path: str,
        cluster_id: Optional[str] = None,
        repo_path: Optional[str] = None,
        test_cache_path: Optional[str] = None,
        log_path: Optional[str] = None
    ) -> None:
        """Add a new configuration to the global config file.
        
        Args:
            test_path: The test path (required).
            cluster_id: The cluster ID (optional).
            repo_path: The repository path (optional).
            test_cache_path: The test cache path (optional, defaults to test_path).
            log_path: The log path (optional, defaults to test_path).
            
        Raises:
            ConfigurationError: If unable to add the configuration.
        """
        try:
            # Validate and prepare paths
            validated_test_path = PathValidator.validate_existing_directory(test_path)
            
            # Create configuration object (validation happens in __post_init__)
            config = GlobalConfig(
                TEST_PATH=str(validated_test_path),
                CLUSTER_ID=cluster_id,
                REPO_PATH=repo_path,
                TEST_CACHE_PATH=test_cache_path,
                LOG_PATH=log_path
            )
            
            # Validate and create cache and log directories
            PathValidator.create_directory_if_not_exists(config.TEST_CACHE_PATH)
            PathValidator.create_directory_if_not_exists(config.LOG_PATH)
            
            # Update configuration file
            self.file_manager.update_config(config.TEST_PATH, config.to_dict())
            
            logger.info(f"Added configuration for test path: {test_path}")
            
        except (PathValidationError, ConfigurationError) as e:
            raise ConfigurationError(f"Unable to add configuration: {e}")

    def _load_config(self) -> None:
        """Load the active configuration based on the current notebook path.
        
        Raises:
            ConfigurationError: If no active configuration is found.
        """
        try:
            config_data = self.file_manager.read_config()
            current_path = Path(get_notebook_path())
            
            # Find matching configuration
            for test_path, config_dict in config_data.items():
                if test_path == 'dbx_tester':  # Skip metadata
                    continue
                    
                try:
                    if current_path.is_relative_to(Path(test_path)):
                        self._config = GlobalConfig.from_dict(config_dict)
                        logger.debug(f"Loaded configuration for path: {test_path}")
                        return
                except (ValueError, TypeError):
                    # Skip invalid paths
                    continue
            
            raise ConfigurationError(
                f"No matching configuration found for current path: {current_path}"
            )
            
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {e}")

    def _load_config_from_test_path(self, test_path: str) -> None:
        """Load configuration for a specific test path.
        
        Args:
            test_path: The test path to load configuration for.
            
        Raises:
            ConfigurationError: If configuration not found for the test path.
        """
        try:
            config_data = self.file_manager.read_config()
            config_dict = config_data.get(test_path)
            
            if config_dict is None:
                raise ConfigurationError(
                    f"Configuration not found for test path: {test_path}"
                )
            
            self._config = GlobalConfig.from_dict(config_dict)
            logger.debug(f"Loaded configuration from test path: {test_path}")
            
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration from test path: {e}")

    def _ensure_config_loaded(self) -> None:
        """Ensure configuration is loaded, loading it if necessary."""
        if self._config is None:
            self._load_config()

    def get_config(self) -> GlobalConfig:
        """Get the current configuration object.
        
        Returns:
            The current GlobalConfig instance.
        """
        self._ensure_config_loaded()
        return self._config

    def reload_config(self) -> None:
        """Reload configuration from file."""
        self._config = None
        self._load_config()

    # Properties for backward compatibility
    @property
    def TEST_PATH(self) -> str:
        """The test path from the active configuration."""
        return self.get_config().TEST_PATH

    @property
    def CLUSTER_ID(self) -> str:
        """The cluster ID from the active configuration."""
        return self.get_config().CLUSTER_ID

    @property
    def REPO_PATH(self) -> Optional[str]:
        """The repository path from the active configuration."""
        return self.get_config().REPO_PATH

    @property
    def TEST_CACHE_PATH(self) -> str:
        """The test cache path from the active configuration."""
        return self.get_config().TEST_CACHE_PATH

    @property
    def LOG_PATH(self) -> str:
        """The log path from the active configuration."""
        return self.get_config().LOG_PATH

    def list_configurations(self) -> Dict[str, GlobalConfig]:
        """List all available configurations.
        
        Returns:
            Dictionary mapping test paths to their configurations.
        """
        try:
            config_data = self.file_manager.read_config()
            configurations = {}
            
            for test_path, config_dict in config_data.items():
                if test_path == 'dbx_tester':  # Skip metadata
                    continue
                
                try:
                    configurations[test_path] = GlobalConfig.from_dict(config_dict)
                except Exception as e:
                    logger.warning(f"Invalid configuration for {test_path}: {e}")
                    
            return configurations
            
        except Exception as e:
            raise ConfigurationError(f"Failed to list configurations: {e}")

    def remove_config(self, test_path: str) -> bool:
        """Remove a configuration for a specific test path.
        
        Args:
            test_path: The test path to remove configuration for.
            
        Returns:
            True if configuration was removed, False if it didn't exist.
        """
        try:
            config_data = self.file_manager.read_config()
            
            if test_path in config_data:
                del config_data[test_path]
                self.file_manager.write_config(config_data)
                logger.info(f"Removed configuration for test path: {test_path}")
                return True
            
            return False
            
        except Exception as e:
            raise ConfigurationError(f"Failed to remove configuration: {e}")