import fnmatch
from pathlib import Path

import toml

if __name__ == "__main__":
    src = Path(__file__).parent.joinpath("config.toml")
    config = toml.load(src)
    toc = "# Housekeeping variables\n\n## Table of contents\n\n"
    content = ""
    for instrument_id, metadata in config["metadata"].items():
        variables = set()
        for name, fmt in config["format"].items():
            if not name.startswith(instrument_id + "_"):
                continue
            variables |= set(fmt["vars"].values())
        toc += f"- [{instrument_id}](#{instrument_id})\n"
        content += f"\n## {instrument_id}\n\n"
        content += "<table>\n"
        content += "<tr>\n"
        content += "<th>Variable</th>\n"
        content += "<th>Unit</th>\n"
        content += "<th>Description</th>\n"
        content += "</tr>\n"
        for variable in sorted(variables):
            content += "<tr>\n"
            for pattern, pattern_meta in metadata["vars"].items():
                if fnmatch.fnmatch(variable, pattern):
                    content += f"<td>\n\n`{variable.strip()}`\n\n</td>\n"
                    content += f"<td>{pattern_meta.get('unit', '')}</td>\n"
                    content += "<td>\n\n"
                    content += pattern_meta.get("description", "")
                    content += "\n\n</td>\n"
                    break
            else:
                raise Exception(f"Metadata for variable '{variable}' not found")
            content += "</tr>\n"
        content += "</table>\n"

    dst = Path(__file__).parent.joinpath("variables.md")
    dst.write_text(toc + content)
