import glob
import tempfile

import pytest

from data_processing import instrument_process
from data_processing.processing_tools import Uuid


class Foo:
    temp_dir_target = "/tmp"
    date_str = "2021-01-01"
    site_meta = {"name": "dummy", "altitude": 123}


@pytest.mark.parametrize(
    "suffix, result",
    [
        (".lv1", ".lv1.mmclx"),
        (".kissa", ".kissa.mmclx"),
        (".mmclx", ".mmclx"),
        (".mmclx.gz", ".mmclx.gz"),
        (".lv1.gz", ".lv1.gz"),
    ],
)
def test_process_radar(suffix, result):
    temp_dir = tempfile.TemporaryDirectory()
    _create_file(temp_dir.name, suffix)
    obj = instrument_process.ProcessRadar(Foo(), tempfile.NamedTemporaryFile(), Uuid())
    obj._fix_suffices(temp_dir.name, ".mmclx")
    files = glob.glob(f"{temp_dir.name}/*")
    assert len(files) == 1
    assert files[0].endswith(result)


def _create_file(dir_name: str, suffix: str) -> None:
    filename = f"foo{suffix}"
    f = open(f"{dir_name}/{filename}", "w")
    f.close()
