from pathlib import Path
from box import ConfigBox

from src.classifier.constants import CONFIG_FILE_PATH, PARAMS_FILE_PATH, SCHEMA_FILE_PATH
from src.classifier.utils.common import read_yaml, create_directories
from src.classifier.entity.config_entity import DataIngestionConfig


class ConfigurationManager:
    def __init__(
        self,
        config_filepath: Path = CONFIG_FILE_PATH,
        params_filepath: Path = PARAMS_FILE_PATH,
        schema_filepath: Path = SCHEMA_FILE_PATH,
    ) -> None:
        """Load configuration, params and schema and prepare artifacts directory."""
        self.config = read_yaml(config_filepath)
        self.params = read_yaml(params_filepath)

        # schema can be optional/empty in early stages
        try:
            self.schema = read_yaml(schema_filepath)
        except Exception:
            self.schema = ConfigBox({})

        # Ensure the top-level artifacts directory exists
        create_directories([self.config.artifacts_root])

    def get_data_ingestion_config(self) -> DataIngestionConfig:
        """Create and return a DataIngestionConfig object.

        Handles minor key naming differences like `source_URL` vs `source_url`.
        Ensures required directories exist.
        """
        cfg = self.config.data_ingestion

        # Ensure the data ingestion root directory exists
        create_directories([cfg.root_dir])

        # Map source URL irrespective of naming in YAML
        source_url_value = getattr(cfg, "source_url", getattr(cfg, "source_URL", None))
        if source_url_value is None:
            raise KeyError("`source_url` (or `source_URL`) not found in data_ingestion config")

        return DataIngestionConfig(
            root_dir=Path(cfg.root_dir),
            source_url=str(source_url_value),
            local_data_file=Path(cfg.local_data_file),
            unzip_dir=Path(cfg.unzip_dir),
        )
