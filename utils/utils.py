import os
from typing import List, Tuple, Dict


def check_file_exists(filename: str) -> None:
    """
    Checks file existence, if not exists, raises an error
    :param filename: Name of file
    :return: None
    """

    if not os.path.exists(filename):
        raise ValueError("File doesn't exist")

def get_offset_list(filename: str) -> (List[str], List[Tuple[int, int]]):
    """
    Calculates chunk size depending on file size and number of CPU cores.
    Scans file, reads header and finds byte offsets for each chunk
    :param filename: Name of large csv file
    :return: Header of file and list of byte offsets, each describing start and end of one chunk
    """
    # Getting file size
    file_size = os.path.getsize(filename)

    # Getting number of CPU-s for machine this code runs on
    cpu_count = os.cpu_count() + 1

    # Calculates chunk size
    chunk_size = file_size // cpu_count

    with open(filename) as f:

        # Initializing list for storing chunk offsets
        offset_list = []

        # Reads first line, which is a csv header
        try:
            headers = f.readline().strip().split(",")
        except ValueError:
            raise

        # Gets byte offset after reading first line
        seek_start_position = f.tell()

        # Set the end of the first chunk roughly
        seek_end_position = chunk_size

        # Each iteration of this loop calculates a precise start and end offsets for each chunk
        for i in range(2, cpu_count + 1):

            # Gets to the rough end position of the chunk
            f.seek(seek_end_position)

            counter = 0
            # Reads file byte-by-byte to a newline character and this point must be a precise end of the current chunk
            while f.read(1) != '\n':
                counter += 1

            # Calculates byte offset for the chunk end position precisely
            seek_end_position = seek_end_position + counter + 2

            # Stores calculated offsets for the current chunk
            offset_list.append((seek_start_position, seek_end_position))

            # End of the current chunk will be start of the following chunk
            seek_start_position = seek_end_position

            # Calculate the end of the following chunk roughly
            seek_end_position = chunk_size * i

        # Adds offsets for the last chunk
        offset_list.append((seek_start_position, file_size))

    # Lowering camel case keys from data header
    headers[0] = headers[0].lower()
    headers[7] = headers[7].lower()

    return headers, offset_list


def make_dict(keys: List[str], values: List[str]) -> Dict:
    """
    Makes dictionary from keys and values passed as a parameter
    :param keys: List of keys
    :param values: List of values
    :return: A dictionary
    """
    return {key: value for key, value in zip(keys, values)}
