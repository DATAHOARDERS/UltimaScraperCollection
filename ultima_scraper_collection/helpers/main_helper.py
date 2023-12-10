import os
from pathlib import Path
from shutil import disk_usage
from typing import Any

from ultima_scraper_api import user_types
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    UserModel as DBUserModel,
)

from ultima_scraper_collection.config import Directory


def check_space(
    custom_directory: list[Directory],
):
    root = ""
    while not root:
        paths: list[dict[str, Any]] = []
        for directory in custom_directory:
            # ISSUE
            # Could cause problems w/ relative/symbolic links that point to another hard drive
            # Haven't tested if it calculates hard A or relative/symbolic B's total space.
            # size is in GB
            assert directory.path
            obj_Disk = disk_usage(str(directory.path.parent))
            free = obj_Disk.free / (1024.0**3)
            x = {}
            x["path"] = directory.path
            x["free"] = free
            x["min_space"] = directory.minimum_space
            paths.append(x)
        for item in paths:
            download_path = item["path"]
            free = item["free"]
            if free > item["min_space"]:
                root = download_path
                break
    return root


from ultima_scraper_api.apis.onlyfans.classes.user_model import (
    create_user as OFUserModel,
)


async def is_valuable(user: DBUserModel | user_types):
    # Checks if performer has active subscription or has supplied content to a buyer
    if isinstance(user, DBUserModel):
        if await user.find_buyers(active=True):
            return True
        else:
            return False
    else:
        if user.is_performer():
            if isinstance(user, OFUserModel):
                if (
                    user.subscribed_is_expired_now == False
                    or await user.get_paid_contents()
                ):
                    return True
                else:
                    return False
            else:
                # We need to add paid_content checker
                if user.following:
                    return True
                else:
                    return False
        else:
            return False


async def is_notif_valuable(api_user: user_types):
    if await is_valuable(api_user):
        if await api_user.subscription_price() == 0:
            if isinstance(api_user, OFUserModel) and await api_user.get_paid_contents():
                return True
            return False
        else:
            return True
    return False


async def walk(directory: Path):
    all_files: list[Path] = []
    for root, _subdirs, files in os.walk(directory):
        x = [Path(root, x) for x in files]
        all_files.extend(x)
    return all_files


def find_unused_filename(filepath: Path):
    base_name = filepath.stem  # Get the filename without extension
    extension = filepath.suffix  # Get the file extension
    counter = 2

    while filepath.exists():
        new_name = f"{base_name} ({counter}){extension}"
        filepath = filepath.with_name(new_name)
        counter += 1

    return filepath
