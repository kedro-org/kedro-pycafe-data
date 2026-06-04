from pathlib import Path

import yaml


CATALOG_PATH = Path(__file__).resolve().parents[1] / "conf" / "base" / "catalog.yml"


def test_file_backed_ibis_datasets_use_public_type():
    catalog = yaml.safe_load(CATALOG_PATH.read_text())

    for dataset_name, dataset_config in catalog.items():
        if dataset_config.get("filepath") and dataset_config.get("file_format"):
            assert dataset_config["type"] == "ibis.FileDataset", dataset_name
