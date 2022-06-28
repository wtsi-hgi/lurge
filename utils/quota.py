import csv
import subprocess
import typing as T

import git
import git.exc

from directory_config import REPORT_DIR

QUOTA_GIT_REPO_LOCATION = REPORT_DIR + "/.lustrequota"


class QuotaReader:
    """
    NOTE: `lurge-gitlab` must be defined in the running user's
    SSH config as pointing to Sanger GitLab, using a valid SSH key

    This is in KiB, so we multiply it by 1024 to get bytes.
    """

    def __init__(self, volume: int) -> None:
        self.volume = volume

        try:
            quota_repo = git.Repo(QUOTA_GIT_REPO_LOCATION)
            quota_repo.head.reset(index=True, working_tree=True)
            quota_repo.remotes.origin.pull()
        except git.exc.NoSuchPathError:
            git.Repo.clone_from(
                "git@lurge-gitlab:ISG/lustrequotamanagement",
                QUOTA_GIT_REPO_LOCATION)

        with open(QUOTA_GIT_REPO_LOCATION + f"/scratch{volume}") as quota_file:
            _quota_reader = csv.DictReader(quota_file)
            self.quotas: T.Dict[str, int] = {line["group"]: int(
                line["limit"]) * 1024 for line in _quota_reader}

    def get_quota(self, group: str) -> T.Optional[int]:
        # NOTE: this only does the size quota, not the inode quota

        try:
            return int(subprocess.check_output(
                ["lfs", "quota", "-gq", group,
                    f"/lustre/scratch{self.volume}"],
                encoding="UTF-8"
            ).split()[3]) * 1024
        except subprocess.CalledProcessError:
            # ok, we can't get the 'live' quota (probably because
            # we don't have permission), so we'll get it from ISG's
            # repo

            return self.quotas.get(group)
