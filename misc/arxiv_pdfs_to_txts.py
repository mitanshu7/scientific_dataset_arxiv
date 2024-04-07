# Description: This script downloads the pdfs from the Arxiv dataset, converts them to text files and deletes the pdfs.

#####################################################################################################################
#####################################################################################################################

## Importing the required libraries

# from pypdf import PdfReader
import numpy as np
# import pandas as pd
from google.cloud import storage
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
def download_folder(bucket_name, folder_name, local_folder_path):
    """Downloads a folder from the bucket."""

    ## Create the folder if it doesn't exist
    create_folder(local_folder_path)

    ## Create an anonymous client for the bucket
    storage_client = storage.Client.create_anonymous_client()

    ## Get the bucket and list the blobs
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_name)

    ## Download the blobs
    for blob in blobs:
        filename = blob.name.replace('/', '_')
        blob.download_to_filename(f"{local_folder_path}/{filename}")

    print(f"Folder {folder_name} downloaded to {local_folder_path}.")

## Function to download a folder from the bucket with a count
def download_folder_counted(bucket_name, folder_name, local_folder_path, count=10):
    """Downloads a folder from the bucket qith max count pdfs."""

    ## Create the folder if it doesn't exist
    create_folder(local_folder_path)

    ## Create an anonymous client for the bucket
    storage_client = storage.Client.create_anonymous_client()

    ## Get the bucket and list the blobs
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_name)

    ## Initiate the counter
    tmp_count = 0

    ## Download the blobs
    for blob in blobs:
        
        ## Check if the counter is less than the count
        if tmp_count < count:

          ## Increment the counter
          tmp_count +=1

          ## Download the file
          filename = blob.name.replace('/', '_')
          blob.download_to_filename(f"{local_folder_path}/{filename}")
        ## If the counter is greater than the count, break the loop
        else:
          break


## Function to delete the original pdfs after they are converted to txt files
def delete_pdfs(directory_path):

    ## Get all pdf files
    pdf_files = glob(f"{directory_path}/*.pdf")
    
    ## Get all txt files
    txt_files = glob(f"{directory_path}/*.txt")
    
    ## Convert to a set for faster searching
    txt_files = set(txt_files)
    

    ## Remove pdf only if there is a corresponding txt file
    for pdf in pdf_files:
        
        ## Get the pdf name
        pdf_name = pdf.split('/')[-1].split('.')[0] + '.' + pdf.split('/')[-1].split('.')[1]
        
        ## Get the txt name
        txt_name = f"{directory_path}/{pdf_name}.txt"
        
        ## Check if txt file exists
        if txt_name in txt_files:
            
            ## Remove the pdf
            os.remove(pdf)

#####################################################################################################################
#####################################################################################################################

## Main code            

## Creating a list for the year and month
yymm_list = np.arange(start=2301, stop=2313, step=1)
yymm_list = [str(i) for i in yymm_list]
# print(yymm_list)

## loop to download the files, convert them to text and delete the pdfs
for yymm in yymm_list:

    ## Track the progress
    print(f"Processing {yymm}.")
    tic = time()

    ## Create a local folder path
    local_folder_path = f'scientific_dataset_2023/{yymm}'
    
    # ## Test whether things are working as expected
    # download_folder_counted(bucket_name='arxiv-dataset', folder_name=f'arxiv/arxiv/pdf/{yymm}', local_folder_path=local_folder_path)

    ## Download all the pdfs published on Arxiv in the year 20yy and month mm
    download_folder(bucket_name='arxiv-dataset', folder_name=f'arxiv/arxiv/pdf/{yymm}', local_folder_path=local_folder_path)

    ## Convert all the pdfs in the yymm directory to text
    convert_directory_parallel(local_folder_path, total_cpu)

    ## Delete them pdfs if they have been converted to txts
    delete_pdfs(local_folder_path)

    ## Track the progress
    toc = time()
    print(f"Processing {yymm} took {toc - tic} seconds.")

## Track time
end_time = time()
print(f"Time taken: {end_time - start_time} seconds.")