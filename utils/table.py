import os
import typing as T

from . import humanise
from . import finder
from lurge_types.directory_report import DirectoryReport


def print_table(directory_info: T.Dict[str, DirectoryReport], volume: str, mode: str, report_dir: str) -> None:
    report_path = finder.findReport(volume, report_dir)
    mpistat_date = int(os.stat(report_path).st_mtime)

    paths = list(directory_info.keys())
    paths.sort()

    for key in paths:
        try:
            _project = "/".join(key.split("/")[:2])
        except IndexError:
            _project = "Total"

        _path = "/".join(key.split("/")[2:])
        if _path == "":
            _path = "Total"

        _files = directory_info[key].num_files
        # 86400 seconds/day
        _mtime = round((mpistat_date - directory_info[key].mtime)/86400, 1)

        _size = humanise(directory_info[key].size)
        _bam = humanise(directory_info[key].bam)
        _cram = humanise(directory_info[key].cram)
        _vcf = humanise(directory_info[key].vcf)
        _pedbed = humanise(directory_info[key].pedbed)

        _unix_group = directory_info[key].group_name
        _pi = directory_info[key].pi
        _volume = directory_info[key].scratch_disk[-3:]

        if mode == "project":
            print(f"{_project}\t{_path}\t{_size}\t{_bam}\t{_cram}\t{_vcf}\t{_pedbed}\t{_files}\t{_mtime}\t{_pi}\t{_unix_group}\t{_volume}")
        elif mode == "general":
            print(
                f"{key}\t{_size}\t{_bam}\t{_cram}\t{_vcf}\t{_pedbed}\t{_files}\t{_mtime}")
