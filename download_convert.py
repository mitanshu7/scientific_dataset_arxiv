## Importing the required libraries

import os
from time import time
from glob import glob
from multiprocessing import Pool, cpu_count # Pool is used to create multiple processes
from scientific_dataset_arxiv.fulltext import convert_directory_parallel, reextension
from scientific_dataset_arxiv.config import start_year, end_year, max_pdfs_per_month, skip_n


#####################################################################################################################
#####################################################################################################################
## Functions

## Function to create a folder if it doesn't exist
def create_folder(directory_path):
    """
    Create a folder at the specified directory path if it doesn't already exist.

    Args:
        directory_path (str): The path of the directory to create.

    Returns:
        None
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def download_folder_transfer_manager(bucket_name, bucket_folder_name, local_folder_path, workers=cpu_count(), max_results=max_pdfs_per_month, skip_first_n=skip_n):
    """
    Downloads a folder from the bucket, skipping PDFs with corresponding TXT files.

    Args:
        bucket_name (str): The name of the bucket.
        bucket_folder_name (str): The name of the folder in the bucket.
        local_folder_path (str): The local path where the folder will be downloaded.
        workers (int, optional): The number of workers to use for parallel downloading. Defaults to the number of CPUs.
        max_results (int, optional): The maximum number of results to retrieve from the bucket. Defaults to 10000.
        skip_first_n (int, optional): The number of results to skip. Defaults to 0.

    Returns:
        None

    Raises:
        None
    """

    from google.cloud.storage import Client, transfer_manager

    # Create the folder if it doesn't exist
    create_folder(local_folder_path)

    # Create an anonymous client for the bucket
    storage_client = Client.create_anonymous_client()

    # Get the bucket and list the blobs
    bucket = storage_client.bucket(bucket_name)

    # Get list of existing TXT filenames
    existing_txt_files = glob(f"{local_folder_path}/**/*.txt", recursive=True)

    # Strip local_folder_path from each path, to match with blob names
    existing_txt_files = [os.path.relpath(path, local_folder_path) for path in existing_txt_files]

    # Normalize all paths in existing_txt_files and convert to a set for faster lookup
    existing_txt_files = set(os.path.normpath(path) for path in existing_txt_files)

    # Filter blobs to download (skip PDFs with corresponding TXT)
    blobs_to_download = []
    skip_count = 0
    for blob in bucket.list_blobs(prefix=bucket_folder_name, max_results=max_results):

        # Skip the first n blobs
        if skip_count < skip_first_n:
            skip_count += 1
            continue

        # Get blob name
        blob_name = blob.name
        
        # Get the corresponding txt file name
        txt_filename = reextension(blob_name, 'txt')

        # Normalize the corresponding txt file name
        txt_filename = os.path.normpath(txt_filename)

        # Check if corresponding TXT file exists using set membership
        # If the txt file does not exist, add the blob to the list of blobs to download
        if txt_filename not in existing_txt_files:
            blobs_to_download.append(blob)


    blob_names = [blob.name for blob in blobs_to_download]

    if not blob_names:
        print(f"No new PDFs to download in {bucket_folder_name}, as all have corresponding TXT files or the total pdfs are less than {skip_first_n}.")
        return

    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=local_folder_path, max_workers=workers, skip_if_exists=True
    )

    for name, result in zip(blob_names, results):
        # The results list is either `None` or an exception for each blob in
        # the input list, in order.

        if isinstance(result, Exception):
            print("Failed to download {} due to exception: {}".format(name, result))
        else:
            print("Downloaded {} to {}.".format(name, local_folder_path + name))



## Function to delete the original pdfs after they are converted to txt files
def delete_pdfs_safe(directory_path):
    """
    Deletes PDF files safely by checking if there is a corresponding TXT file.

    Args:
        directory_path (str): The path to the directory containing the PDF and TXT files.

    Returns:
        None
    """

    pdf_files = glob(f"{directory_path}/**/*.pdf", recursive=True)
    txt_files = set(glob(f"{directory_path}/**/*.txt", recursive=True))

    ## Check if there are any pdf files
    if not pdf_files:  # Check if pdf_files is empty
        print(f"No PDFs found to delete in: {directory_path}")
        return

    ## Check if there are any txt files
    for pdf in pdf_files:

        ## Get the txt file name
        txt_name = reextension(pdf, 'txt')

        ## Check if the txt file exists
        if txt_name in txt_files:
            try:
                os.remove(pdf)
                print(f"Deleted PDF: {pdf}")
            except OSError as e:
                print(f"Error deleting PDF: {pdf} - {e}")  # Log errors
                continue
        else:
            print(f"No corresponding TXT file found for: {pdf}, hence will not delete")  # Handle missing TXT


## Creating a list for the year and month
def create_yymm_list(start_year, end_year):
    """
    Create a list of YYMM strings representing years and months between the given start_year and end_year.

    Args:
        start_year (int): The starting year.
        end_year (int): The ending year.

    Returns:
        list: A list of YYMM strings representing years and months between the start_year and end_year.
    """
    start_year = start_year % 100
    end_year = end_year % 100
    yymm_list = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            yymm = year * 100 + month

            ## Pad a 0 if the year is less than 10
            if len(str(yymm)) == 3:
                yymm = '0' + str(yymm)
            
            yymm_list.append(yymm)

    ## Convert the yymm list to string
    yymm_list = [str(i) for i in yymm_list]

    return yymm_list


#####################################################################################################################
#####################################################################################################################

## Main code


if __name__ == '__main__':

    ## Track time
    tic = time()

    ## Create a yymm list from the year 2020 to 2023
    yymm_list = create_yymm_list(start_year, end_year)

    ## loop to download the files, convert them to text and delete the pdfs
    for yymm in yymm_list:

        ## Track the progress
        print(f"Processing {yymm}.")
        

        ## Create a local folder path
        local_folder_path = f'unprocessed_txts_{start_year}_to_{end_year}/{yymm}'
        
        ## Download all (max 10,000) the pdfs published on Arxiv in the year 20yy and month mm
        print(f"Downloading PDFs for {yymm}.")
        download_folder_transfer_manager(bucket_name='arxiv-dataset', bucket_folder_name=f'arxiv/arxiv/pdf/{yymm}', local_folder_path=local_folder_path)

        ## Convert all the pdfs in the yymm directory to text
        print(f"Converting PDFs to TXTs for {yymm}.")
        convert_directory_parallel(local_folder_path, cpu_count())
        
        ## Delete them pdfs if they have been converted to txts
        print(f"Deleting PDFs for {yymm}.")
        delete_pdfs_safe(local_folder_path)
    
    ## Track time
    toc = time()
    print(f"Time taken: {toc - tic} seconds.")