import yaml
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

with open(ROOT_DIR / "config/config.yaml", "r") as file:
    conf = yaml.safe_load(file)

raw_path = conf["files"]["raw_path"]
wip_path = conf["files"]["wip_path"]
processed_path = conf["files"]["processed_path"]
config_path = conf["files"]["config_path"]