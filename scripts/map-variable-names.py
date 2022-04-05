#!/usr/bin/env python3
"""Script for printing Cloudnet variable names."""
import json

import cloudnet_processing.utils as process_utils


def main():
    """The main function."""

    all_ids = {}
    file_types = (
        "radar",
        "lidar",
        "categorize",
        "classification",
        "iwc",
        "lwc",
        "drizzle",
        "model",
    )
    for file_type in file_types:
        all_ids = {**all_ids, **process_utils.get_plottable_variables_info(file_type)}
    print(all_ids)
    print(json.dumps(all_ids, indent=1))


if __name__ == "__main__":
    main()
