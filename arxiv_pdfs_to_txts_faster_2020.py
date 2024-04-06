# Description: This script downloads the pdfs from the Arxiv dataset, converts them to text files and deletes the pdfs.

#####################################################################################################################
#####################################################################################################################

## Importing the required libraries

# from pypdf import PdfReader
import numpy as np
# import pandas as pd
# from google.cloud import storage
import os
from glob import glob
# import json
import multiprocessing
from arxiv_public_datasets.arxiv_public_data.fulltext import convert_directory_parallel
from time import time

#####################################################################################################################
#####################################################################################################################
## Constants

## Track time
start_time = time()

## Get the total number of cpus
total_cpu = multiprocessing.cpu_count()
# print(total_cpu)

#####################################################################################################################
#####################################################################################################################
## Functions

## Function to create a folder if it doesn't exist
def create_folder(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

## Function to download a folder from the bucket
def download_folder_transfer_manager(bucket_name, bucket_folder_name, local_folder_path, workers=total_cpu, max_results=10000):
    """Downloads a folder from the bucket."""

    from google.cloud.storage import Client, transfer_manager

    ## Create the folder if it doesn't exist
    create_folder(local_folder_path)

    ## Create an anonymous client for the bucket
    storage_client = Client.create_anonymous_client()

    ## Get the bucket and list the blobs
    bucket = storage_client.bucket(bucket_name)

    blob_names = [blob.name for blob in bucket.list_blobs(max_results=max_results, prefix=bucket_folder_name)]

    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=local_folder_path, max_workers=workers
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

    ## Get all pdf files
    pdf_files = glob(f"{directory_path}/**/*.pdf", recursive=True)
    
    ## Get all txt files
    txt_files = glob(f"{directory_path}/**/*.txt", recursive=True)
    
    ## Convert to a set for faster searching
    txt_files = set(txt_files)
    

    ## Remove pdf only if there is a corresponding txt file
    for pdf in pdf_files:
        
        ## Get the pdf name
        pdf_name = pdf.split('/')[-1].split('.')[0] + '.' + pdf.split('/')[-1].split('.')[1]
        
        ## Get the txt name
        txt_name = pdf.replace('.pdf', '.txt')
        
        ## Check if txt file exists
        if txt_name in txt_files:
            
            ## Remove the pdf
            os.remove(pdf)

## Function to remove the pdfs from a given directory
def remove_pdfs(directory_path):
    pdf_files = glob(f"{directory_path}/**/*.pdf", recursive=True)
    for pdf in pdf_files:
        os.remove(pdf)

#####################################################################################################################
#####################################################################################################################

## Main code            

## Creating a list for the year and month
yymm_list = np.arange(start=2001, stop=2013, step=1)

## Skipping the ones that have already been processed
# yymm_list = np.arange(start=2005, stop=2013, step=1)


## Convert the list to strings
yymm_list = [str(i) for i in yymm_list]
# print(yymm_list)

## loop to download the files, convert them to text and delete the pdfs
for yymm in yymm_list:

    ## Track the progress
    print(f"Processing {yymm}.")
    tic = time()

    ## Create a local folder path
    local_folder_path = f'scientific_dataset_2020/{yymm}'
    
    # ## Test whether things are working as expected
    # download_folder_transfer_manager(bucket_name='arxiv-dataset', bucket_folder_name=f'arxiv/arxiv/pdf/{yymm}', local_folder_path=local_folder_path, max_results=10)

    ## Download all the pdfs published on Arxiv in the year 20yy and month mm
    download_folder_transfer_manager(bucket_name='arxiv-dataset', bucket_folder_name=f'arxiv/arxiv/pdf/{yymm}', local_folder_path=local_folder_path)

    ## Convert all the pdfs in the yymm directory to text
    convert_directory_parallel(local_folder_path, total_cpu)

    ## Delete them pdfs if they have been converted to txts
    delete_pdfs_safe(local_folder_path)

    ## Track the progress
    toc = time()
    print(f"Processing {yymm} took {toc - tic} seconds.")

## Track time
end_time = time()
print(f"Time taken: {end_time - start_time} seconds.")