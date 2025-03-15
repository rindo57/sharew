from pathlib import Path
import config
from pyrogram.types import InputMediaDocument
import pickle, os, random, string, asyncio
from utils.logger import Logger
from datetime import datetime, timezone

logger = Logger(__name__)

cache_dir = Path("./cache")
cache_dir.mkdir(parents=True, exist_ok=True)
drive_cache_path = cache_dir / "drive.data"


def getRandomID():
    global DRIVE_DATA
    while True:
        id = "".join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=15))

        if id not in DRIVE_DATA.used_ids:
            DRIVE_DATA.used_ids.append(id)
            return id


def get_current_utc_time():
    return datetime.now(timezone.utc).strftime("Date - %Y-%m-%d | Time - %H:%M:%S")


class Folder:
    def __init__(self, name: str, path: str, uploader: str) -> None:
        self.name = name
        self.contents = {}
        if name == "/":
            self.id = "root"
        else:
            self.id = getRandomID()
        self.type = "folder"
        self.trash = False
        self.path = ("/" + path.strip("/") + "/").replace("//", "/")
        
        self.upload_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.uploader = uploader
        self.auth_hashes = []


class File:
    def __init__(
        self,
        name: str,
        file_id: int,
        size: int,
        path: str,
        rentry_link: str, 
        paste_url: str,
        uploader: str, 
        audio: str,
        subtitle: str,
        resolution: str,
        codec: str,
        bit_depth: str,
        duration: str
    ) -> None:
        self.name = name
        self.type = type
        self.file_id = file_id
        self.id = getRandomID()
        self.size = size
        self.type = "file"
        self.trash = False
        self.path = path[:-1] if path[-1] == "/" else path
        self.upload_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.rentry_link = rentry_link
        self.paste_url = paste_url
        self.uploader = uploader
        self.audio = audio
        self.subtitle = subtitle
        self.resolution = resolution
        self.codec = codec
        self.bit_depth = bit_depth
        self.duration = duration


class NewDriveData:
    def __init__(self, contents: dict, used_ids: list) -> None:
        self.contents = contents
        self.used_ids = used_ids
        self.isUpdated = False

    def save(self) -> None:
        with open(drive_cache_path, "wb") as f:
            pickle.dump(self, f)

        self.isUpdated = True

    def new_folder(self, path: str, name: str, uploader: str)-> None:
        logger.info(f"Creating new folder {name} in {path} by {uploader}")
        
        print("New some path ", path)
        folder = Folder(name, path, uploader)
        if path == "/":
            directory_folder: Folder = self.contents[path]
            directory_folder.contents[folder.id] = folder
        else:
            paths = path.strip("/").split("/")
            directory_folder: Folder = self.contents["/"]
            for path in paths:
                directory_folder = directory_folder.contents[path]
            directory_folder.contents[folder.id] = folder

        self.save()
        return folder.path + folder.id

    def new_file(self, path: str, name: str, file_id: int, size: int, rentry_link: str, paste_url: str, uploader: str, audio: str, subtitle: str, resolution: str, codec: str, bit_depth: str, duration: str) -> None:
        logger.info(f"Creating new file {name} in {path} by {uploader}")

        file = File(name, file_id, size, path, rentry_link, paste_url, uploader, audio, subtitle, resolution, codec, bit_depth, duration)
        if path == "/":
            directory_folder: Folder = self.contents[path]
            directory_folder.contents[file.id] = file
        else:
            paths = path.strip("/").split("/")
            directory_folder: Folder = self.contents["/"]
            for path in paths:
                directory_folder = directory_folder.contents[path]
            directory_folder.contents[file.id] = file
        logger.info("just before saving")
        self.save()
        logger.info(f"Created new file {name} in {path} by {uploader} successfully")

    def get_directory(
        self, path: str, is_admin: bool = True, auth: str = None
    ) -> Folder:
        folder_data: Folder = self.contents["/"]
        auth_success = False
        auth_home_path = None

        if path != "/":
            path = path.strip("/")

            if "/" in path:
                path = path.split("/")
            else:
                path = [path]

            for folder in path:
                folder_data = folder_data.contents[folder]

                if auth in folder_data.auth_hashes:
                    auth_success = True
                    auth_home_path = (
                        "/" + folder_data.path.strip("/") + "/" + folder_data.id
                    )

        if not is_admin and not auth_success:
            return None

        if auth_success:
            return folder_data, auth_home_path

        return folder_data

    def get_directory2(
        self, path: str, is_admin: bool = True, auth: str = None
    ) -> Folder:
        folder_data: Folder = self.contents["/"]
        auth_success = False
        auth_home_path = None

        if path != "/":
            path = path.strip("/")

            if "/" in path:
                path = path.split("/")
            else:
                path = [path]

            for folder in path:
                folder_data = folder_data.contents[folder]

                #if auth in folder_data.auth_hashes:
                auth_success = True
                auth_home_path = (
                    "/" + folder_data.path.strip("/") + "/" + folder_data.id
                )


        return folder_data
        
    def get_folder_auth(self, path: str) -> None:
        auth = getRandomID()
        folder_data: Folder = self.contents["/"]

        if path != "/":
            path = path.strip("/")

            if "/" in path:
                path = path.split("/")
            else:
                path = [path]

            for folder in path:
                folder_data = folder_data.contents[folder]

        folder_data.auth_hashes.append(auth)
        self.save()
        return auth

    def get_file(self, path) -> File:
        if len(path.strip("/").split("/")) > 0:
            folder_path = "/" + "/".join(path.strip("/").split("/")[:-1])
            file_id = path.strip("/").split("/")[-1]
        else:
            folder_path = "/"
            file_id = path.strip("/")

        folder_data = self.get_directory(folder_path)
        return folder_data.contents[file_id]

    def rename_file_folder(self, path: str, new_name: str) -> None:
        logger.info(f"Renaming {path} to {new_name}")

        if len(path.strip("/").split("/")) > 0:
            folder_path = "/" + "/".join(path.strip("/").split("/")[:-1])
            file_id = path.strip("/").split("/")[-1]
        else:
            folder_path = "/"
            file_id = path.strip("/")
        folder_data = self.get_directory(folder_path)
        folder_data.contents[file_id].name = new_name
        self.save()

    def trash_file_folder(self, path: str, trash: bool) -> None:
        logger.info(f"Trashing {path}")

        if len(path.strip("/").split("/")) > 0:
            folder_path = "/" + "/".join(path.strip("/").split("/")[:-1])
            file_id = path.strip("/").split("/")[-1]
        else:
            folder_path = "/"
            file_id = path.strip("/")
        folder_data = self.get_directory(folder_path)
        folder_data.contents[file_id].trash = trash
        self.save()

    def get_trashed_files_folders(self):
        root_dir = self.get_directory("/")
        trash_data = {}

        def traverse_directory(folder):
            for item in folder.contents.values():
                if item.type == "folder":
                    if item.trash:
                        trash_data[item.id] = item
                    else:
                        # Recursively traverse the subfolder
                        traverse_directory(item)
                elif item.type == "file":
                    if item.trash:
                        trash_data[item.id] = item

        traverse_directory(root_dir)
        return trash_data

    def delete_file_folder(self, path: str) -> None:
        logger.info(f"Deleting {path}")

        if len(path.strip("/").split("/")) > 0:
            folder_path = "/" + "/".join(path.strip("/").split("/")[:-1])
            file_id = path.strip("/").split("/")[-1]
        else:
            folder_path = "/"
            file_id = path.strip("/")

        folder_data = self.get_directory(folder_path)
        del folder_data.contents[file_id]
        self.save()


    
    def search_file_folder(self, query: str = None, path: str = None):
        if path=="":
            root_dir = self.get_directory("/")
        elif path=="/":
            root_dir = self.get_directory("/")
        else:   
            root_dir = self.get_directory(path)
            print(root_dir)
        search_results = {}

        def traverse_directory(folder):
            for item in folder.contents.values():
                if query.lower() in item.name.lower():
                    search_results[item.id] = item
                if item.type == "folder":
                    traverse_directory(item)
        traverse_directory(root_dir)
        
        return search_results

    def search_file_foldertg(self, path: str = None):
        if path in ("", "/"):
            root_dir = self.get_directory("/")
        else:   
            root_dir = self.get_directory(path)
            print(root_dir)

        search_results = {}

        def traverse_directory(folder):
            for item in folder.contents.values():
                if item.type == "folder":  # Only include folders
                    search_results[item.id] = item
                #traverse_directory(item)  # Continue traversing subfolders

        traverse_directory(root_dir)
        print("search results ", search_results)
        return search_results  # Ensure the function returns the results

    def search_file_folderx(self, query: str):
        root_dir = self.get_directory("/")
        search_results = {}

        def traverse_directory(folder):
            for item in folder.contents.values():
                if query.lower() in item.name.lower():
                    search_results[item.id] = item
                if item.type == "folder":
                    traverse_directory(item)

        traverse_directory(root_dir)
        return search_results

    def search_file_folder2(self, query: str, path: str, is_admin: bool, auth: str):
        if path=="":
            root_dir, auth_home_path = self.get_directory("/", is_admin, auth)
        elif path=="/":
            root_dir, auth_home_path = self.get_directory("/", is_admin, auth)
        else:   
            root_dir = self.get_directory(path)
            print(root_dir)
        search_results = {}

        def traverse_directory(folder):
            for item in folder.contents.values():
                if query.lower() in item.name.lower():
                    search_results[item.id] = item
                if item.type == "folder":
                    traverse_directory(item)

        traverse_directory(root_dir)
        return search_results

class NewBotMode:
    def __init__(self, drive_data: NewDriveData) -> None:
        self.drive_data = drive_data

        # Set the current folder to root directory by default
        self.current_folder = "/"
        self.current_folder_name = "/ (root directory)"

    def set_folder(self, folder_path: str, name: str) -> None:
        self.current_folder = folder_path
        self.current_folder_name = name
        self.drive_data.save()


DRIVE_DATA: NewDriveData = None
BOT_MODE: NewBotMode = None


# Function to backup the drive data to telegram
async def backup_drive_data(loop=True):
    """
    Backup the drive data to Telegram.
    This version reads the drive.data file into memory using a BytesIO buffer so that
    the file is immediately closed, preventing the "Too many open files" error.
    The file name is passed as a parameter to edit_message_media.
    """
    global DRIVE_DATA
    logger.info("Starting backup drive data task")

    while True:
        try:
            if not DRIVE_DATA.isUpdated:
                if not loop:
                    break
                await asyncio.sleep(config.DATABASE_BACKUP_TIME)
                continue

            logger.info("Backing up drive data to telegram")
            from utils.clients import get_client

           # client = get_client()
            time_text = f"📅 **Last Updated :** {get_current_utc_time()} (UTC +00:00)"
            caption = ("UI")
            msgx = await client.get_messages(
                config.STORAGE_CHANNEL, config.DATABASE_BACKUP_MSG_ID
            )
            if msgx.caption == "Script":
                await loadDriveData()
            # Create the media document without file_name.
                media_doc = InputMediaDocument(drive_cache_path, caption=caption)
            # Pass file_name as parameter to edit_message_media.
                msg = await client.edit_message_media(
                    config.STORAGE_CHANNEL,
                    config.DATABASE_BACKUP_MSG_ID,
                    media=media_doc,
                    file_name="drive.data",
                )

                DRIVE_DATA.isUpdated = False
                logger.info("Drive data backed up to telegram")
            elif msgx.caption == "UI":
             #   await loadDriveData2()
            # Create the media document without file_name.
                media_doc = InputMediaDocument(drive_cache_path, caption=caption)
            # Pass file_name as parameter to edit_message_media.
                msg = await client.edit_message_media(
                    config.STORAGE_CHANNEL,
                    config.DATABASE_BACKUP_MSG_ID,
                    media=media_doc,
                    file_name="drive.data",
                )

                DRIVE_DATA.isUpdated = False
                logger.info("Drive data backed up to telegram")
            try:
                await msg.pin()
            except Exception as pin_e:
                logger.error(f"Error pinning backup message: {pin_e}")

            if not loop:
                break

            await asyncio.sleep(config.DATABASE_BACKUP_TIME)
        except Exception as e:
            logger.error("Backup Error : " + str(e))
            await asyncio.sleep(10)

async def backup_drive_data2():
    """
    Backup the drive data to Telegram.
    This version reads the drive.data file into memory using a BytesIO buffer so that
    the file is immediately closed, preventing the "Too many open files" error.
    The file name is passed as a parameter to edit_message_media.
    """
    global DRIVE_DATA
    logger.info("Starting backup drive data task")

    try:
        if not DRIVE_DATA.isUpdated:
            logger.info("Drive data is not updated. Skipping backup.")
            return

        logger.info("Backing up drive data to telegram")
        from utils.clients import get_client

        client = get_client()
        time_text = f"📅 **Last Updated :** {get_current_utc_time()} (UTC +00:00)"
        caption = ("Script")
        msgx = await client.get_messages(
            config.STORAGE_CHANNEL, config.DATABASE_BACKUP_MSG_ID
        )
        if msgx.caption == "UI":
            await loadDriveData2()
            # Create the media document without file_name.
            media_doc = InputMediaDocument(drive_cache_path, caption=caption)
            # Pass file_name as parameter to edit_message_media.
            msg = await client.edit_message_media(
                config.STORAGE_CHANNEL,
                config.DATABASE_BACKUP_MSG_ID,
                media=media_doc,
                file_name="drive.data",
            )

            DRIVE_DATA.isUpdated = False
            logger.info("Drive data backed up to telegram")
        elif msgx.caption == "Script":
            await loadDriveData2()
            # Create the media document without file_name.
            media_doc = InputMediaDocument(drive_cache_path, caption=caption)
            # Pass file_name as parameter to edit_message_media.
            msg = await client.edit_message_media(
                config.STORAGE_CHANNEL,
                config.DATABASE_BACKUP_MSG_ID,
                media=media_doc,
                file_name="drive.data",
            )

        DRIVE_DATA.isUpdated = False
        logger.info("Drive data backed up to telegram")

    except Exception as e:
        logger.error("Backup Error : " + str(e))



async def init_drive_data():
    # auth_hashes attribute is added to all the folders in the drive data if it doesn't exist

    global DRIVE_DATA

    root_dir = DRIVE_DATA.get_directory("/")
    if not hasattr(root_dir, "auth_hashes"):
        root_dir.auth_hashes = []

    def traverse_directory(folder):
        for item in folder.contents.values():
            if item.type == "folder":
                traverse_directory(item)

                if not hasattr(item, "auth_hashes"):
                    item.auth_hashes = []

    traverse_directory(root_dir)

    DRIVE_DATA.save()

async def loadDriveData2():
    global DRIVE_DATA

    # Checking if the backup file exists on telegram
    from utils.clients import get_client

    client = get_client()
    try:
        try:
            msg = await client.get_messages(
                config.STORAGE_CHANNEL, config.DATABASE_BACKUP_MSG_ID
            )
        except Exception as e:
            logger.error(e)
            raise Exception("Failed to get DATABASE_BACKUP_MSG_ID on telegram")

        if msg.document.file_name == "drive.data":
            dl_path = await msg.download()
            print("load drive 2 ", dl_path)
            with open(dl_path, "rb") as f:
                DRIVE_DATA = pickle.load(f)

            logger.info("Drive data loaded from backup file from telegram")
        else:
            raise Exception("Backup drive.data file not found on telegram")
    except Exception as e:
        logger.warning(e)
        logger.info("Creating new drive.data file")
        DRIVE_DATA = NewDriveData({"/": Folder("/", "/", "root")}, [])
        DRIVE_DATA.save()

    # For updating the changes in already existing old backup drive.data file
    await init_drive_data()
    
async def loadDriveData():
    global DRIVE_DATA, BOT_MODE

    # Checking if the backup file exists on telegram
    from utils.clients import get_client

    client = get_client()
    try:
        try:
            msg = await client.get_messages(
                config.STORAGE_CHANNEL, config.DATABASE_BACKUP_MSG_ID
            )
        except Exception as e:
            logger.error(e)
            raise Exception("Failed to get DATABASE_BACKUP_MSG_ID on telegram")

        if msg.document.file_name == "drive.data":
            dl_path = await msg.download()
            print("load drive", dl_path)
            with open(dl_path, "rb") as f:
                DRIVE_DATA = pickle.load(f)

            logger.info("Drive data loaded from backup file from telegram")
        else:
            raise Exception("Backup drive.data file not found on telegram")
    except Exception as e:
        logger.warning(e)
        logger.info("Creating new drive.data file")
        DRIVE_DATA = NewDriveData({"/": Folder("/", "/", "root")}, [])
        DRIVE_DATA.save()

    # For updating the changes in already existing old backup drive.data file
    await init_drive_data()

    # Start Bot Mode
    if config.MAIN_BOT_TOKEN:
        from utils.bot_mode import start_bot_mode

        BOT_MODE = NewBotMode(DRIVE_DATA)
        await start_bot_mode(DRIVE_DATA, BOT_MODE)
