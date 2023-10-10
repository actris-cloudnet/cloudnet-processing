import glob
from tempfile import NamedTemporaryFile, TemporaryDirectory

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
    with TemporaryDirectory() as temp_dir, NamedTemporaryFile() as out_file:
        in_files = [_create_file(temp_dir, suffix)]
        obj = instrument_process.ProcessRadar(
            Foo(), out_file, Uuid(), "some_instrument_pid"
        )
        out_files = obj._fix_suffices(in_files, ".mmclx")
        assert len(out_files) == 1
        assert out_files[0].endswith(result)

        out_files = glob.glob(f"{temp_dir}/*")
        assert len(out_files) == 1
        assert out_files[0].endswith(result)


def _create_file(dir_name: str, suffix: str) -> str:
    filename = f"{dir_name}/foo{suffix}"
    f = open(filename, "w")
    f.close()
    return filename
