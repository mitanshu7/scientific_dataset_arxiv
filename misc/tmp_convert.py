from scientific_dataset_arxiv.fulltext import convert_directory, convert_directory_parallel

from scientific_dataset_arxiv.config import start_year, end_year
from multiprocessing import cpu_count
import os

local_folder_path = f'unprocessed_txts_{start_year}_to_{end_year}'


if __name__ == '__main__':

    # convert_directory_parallel(local_folder_path, cpu_count())
    convert_directory(local_folder_path, cpu_count())