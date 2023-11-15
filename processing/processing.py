from multiprocessing import Process, Queue
from typing import List, Tuple
from utils import make_dict
from data_models import DriveData


def process_chunk(
        filename: str,
        headers: List[str],
        chunk_id: int,
        seek_start_position: int,
        seek_end_position: int,
        queue: Queue
) -> None:
    """
    Reads and processes chunk data and counts total revenue per vendor.
    Runs as a separate process
    :param filename: Name of file
    :param headers: Header of the file data
    :param chunk_id: ID of the chunk to be processes
    :param seek_start_position: Starting offset of the chunk
    :param seek_end_position: Ending offset of the chunk
    :param queue: Multiprocessing.Queue object for putting result
    :return: None
    """
    # Initializing list for storing subtotal revenues for each vendor
    vendor_revenue = [0, 0]

    with open(filename) as f:

        # Gets to the start position to begin reading
        f.seek(seek_start_position)

        # Initializing variable to count lines
        line = 0
        # Checks if it reaches to end of chunk
        while f.tell() < seek_end_position:

            # Gets record and transforms it to list
            record = f.readline().strip().split(",")

            # Makes dictionary of a data from the header and the record
            record_as_a_dict = make_dict(headers, record)
            try:
                # Trying to validate data dictionary with pydantic
                record_pydantic = DriveData(**record_as_a_dict)
            except Exception:
                print(f"Error occurred in the chunk {chunk_id}, at the line {line}")
                print(record)
                continue
            finally:
                line += 1

            # Increases a revenue for the corresponding vendor
            vendor_revenue[record_pydantic.vendorid - 1] += record_pydantic.total_amount

    # Puts result in the queue for the main process
    queue.put(vendor_revenue)


def create_processes(filename: str, header: List[str], offset_list: List[Tuple[int, int]]) -> Queue:
    """
    Creates processes per chunk
    :param filename: Name of file to be processed
    :param header: Header of csv file
    :param offset_list: Byte offsets describing start and end of chunks
    :return: Queue filled by results of processes
    """

    # Initializing the pool of processes
    processes = []

    # Initializing queue for passing to processes
    result_queue = Queue()

    # Creating separate process for each chunk and add it to the process pool
    for chunk_idx, (seek_start_pos, seek_end_pos) in enumerate(offset_list):
        process = Process(target=process_chunk, args=(filename, header, chunk_idx, seek_start_pos, seek_end_pos, result_queue))
        processes.append(process)

    # Starting processes
    [process.start() for process in processes]

    # Waiting all process to be done
    [process.join() for process in processes]

    return result_queue


def extract_queue(result_queue: Queue, vendors: Tuple[str, str]) -> None:

    # Initializing list for storing total revenues for each vendor
    total_vendor_revenue = [0, 0]

    # Extracting subtotal revenues from the result queue
    while not result_queue.empty():

        vendor1_rev, vendor2_rev = result_queue.get()

        # Adding subtotal revenues to the total revenues list
        total_vendor_revenue[0] += vendor1_rev
        total_vendor_revenue[1] += vendor2_rev

    print("\nTotal revenue by vendor:\n")
    # Printing total revenues rounded to two decimal
    for vendor_id in range(2):
        print(f"{vendors[vendor_id]}: {round(total_vendor_revenue[vendor_id], 2)} USD")
