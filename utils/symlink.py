import re

from directory_config import MDT_SYMLINKS


def get_mdt_symlink(path: str) -> str:
    for full_path, human_path in MDT_SYMLINKS.items():
        if re.match(full_path, path):
            return re.sub(full_path, human_path, path)
    return path