import logging

import toml
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Read the TOML config file
root_path = Path(__file__).parent.parent.absolute()
config_path = os.path.join(root_path, 'config.toml')
logger.info(f"Config path : {config_path}")
with open(config_path, 'r', encoding='utf-8') as f:
    config = toml.load(f)
