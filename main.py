from time import perf_counter
from utils import get_offset_list, check_file_exists
from processing import create_processes, extract_queue

FILE_NAME = '/Users/cellnfix/jpenv/working/archive/yellow_tripdata_2015-01.csv'

# As mentioned at website, there is only these two vendors
VENDORS = ("Creative Mobile Technologies", "VeriFone Inc.")


def main() -> None:
    """
    Main function
    :return: None
    """

    start_time = perf_counter()
    print("Started...")

    # Check if file exists
    check_file_exists(FILE_NAME)

    # Gets header and offset list for chunks
    header, offset_list = get_offset_list(FILE_NAME)

    # Creates processes per chunk of data and returns result queue of those processes
    q = create_processes(FILE_NAME, header, offset_list)

    # After processes end, extracting result queue to summarize data
    extract_queue(q, VENDORS)

    print(f"\nExecuted in {perf_counter() - start_time} seconds")


if __name__ == '__main__':
    main()
