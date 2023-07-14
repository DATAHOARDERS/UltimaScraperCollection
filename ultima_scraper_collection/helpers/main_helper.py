from shutil import disk_usage
from typing import Any

from ultima_scraper_api import user_types
from ultima_scraper_db.databases.ultima.schemas.templates.site import (
    UserModel as DBUserModel,
)

from ultima_scraper_collection.config import Directory


def check_space(
    custom_directory: list[Directory],
    min_size: int = 0,
    priority: str = "download",
):
    root = ""
    while not root:
        paths: list[dict[str, Any]] = []
        for directory in custom_directory:
            # ISSUE
            # Could cause problems w/ relative/symbolic links that point to another hard drive
            # Haven't tested if it calculates hard A or relative/symbolic B's total space.
            assert directory.path
            obj_Disk = disk_usage(str(directory.path.parent))
            free = obj_Disk.free / (1024.0**3)
            x = {}
            x["path"] = directory.path
            x["free"] = free
            paths.append(x)
        if priority == "download":
            for item in paths:
                download_path = item["path"]
                free = item["free"]
                if free > min_size:
                    root = download_path
                    break
        elif priority == "upload":
            paths.sort(key=lambda x: x["free"])
            item = paths[0]
            root = item["path"]
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
        if user.isPerformer:
            if isinstance(user, OFUserModel):
                if (
                    user.subscribedIsExpiredNow == False
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
