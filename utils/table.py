import os
import typing as T

from . import finder
from lurge_types.directory_report import DirectoryReport


def humanise(number):
    """Converts bytes to human-readable string."""
    if number/2**10 < 1:
        return "{}".format(number)
    elif number/2**20 < 1:
        return "{} KiB".format(round(number/2**10, 2))
    elif number/2**30 < 1:
        return "{} MiB".format(round(number/2**20, 2))
    elif number/2**40 < 1:
        return "{} GiB".format(round(number/2**30, 2))
    elif number/2**50 < 1:
        return "{} TiB".format(round(number/2**40, 2))
    else:
        return "{} PiB".format(round(number/2**50, 2))


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
