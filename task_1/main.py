import asyncio
import os
from argparse import ArgumentParser
import aiofiles
import logging

logging.basicConfig(
    level=logging.ERROR, format='%(pastime)s - %(levelness)s - %(message)s'
)

parser = ArgumentParser(description="Sort files by extension asynchronously.")
parser.add_argument(
    "--source", help="Source folder path", required=True
)
parser.add_argument(
    "--destination", help="Destination folder path", required=True
)
args = parser.parse_args()


async def create_folder_if_not_exists(folder_path):
    """Asynchronously checks if a folder exists, and creates it if not."""
    try:
        if not os.path.exists(folder_path):
            await asyncio.to_thread(os.makedirs, folder_path)
    except Exception as e:
        logging.error(f"Error creating folder {folder_path}: {e}")


async def copy_file(source_path, destination_path):
    """Asynchronously copies a file to a destination folder."""

    try:
        extension: str = os.path.splitext(source_path)[1][1:]
        target_folder: str = os.path.join(destination_path, extension)
        await create_folder_if_not_exists(target_folder)
        target_path: str = os.path.join(
            target_folder,
            os.path.basename(source_path)
        )
        async with (
            aiofiles.open(source_path, 'rb') as src,
            aiofiles.open(target_path, 'wb') as dst
        ):
            await dst.write(await src.read())
        logging.info(f"File {source_path} successfully copied "
                     f"to {destination_path}")
    except Exception as e:
        logging.error(f"Error copying file {source_path} "
                      f"to {destination_path}: {e}")


async def read_folder(folder_path, destination_path):
    """
    Asynchronously reads a folder and copies files to a destination folder.
    """
    try:
        for entry in await asyncio.to_thread(os.scandir, folder_path):
            if entry.is_dir():
                await read_folder(entry.path, destination_path)
            else:
                await copy_file(entry.path, destination_path)
    except Exception as e:
        logging.error(f"Error reading folder {folder_path}: {e}")


async def main():
    """The entry point of the program."""
    try:
        source_path = args.source
        destination_path = args.destination
        await read_folder(source_path, destination_path)
    except Exception as e:
        logging.error(f"Error in main: {e}")

if __name__ == "__main__":
    asyncio.run(main())
