## This code will be used to create the dataset from scratch

"""
Step 1: Download the papers from the google storage bucket
Step 2: Extract the text from the papers
Step 3: Extract articles from the papers
Step 4: Load the processed the metadata
Step 5: Create the dataset by merging the metadata and articles
"""

#####################################################################################################################
#####################################################################################################################
#####################################################################################################################
## Step 1: Download the papers from the google storage bucket
## Step 2: Extract the text from the papers

# Description: This script downloads the pdfs from the Arxiv dataset, converts them to text files and deletes the pdfs.

#####################################################################################################################
## Enter the year range for which you want to download the papers and convert them to text files
## The year range is inclusive, valid for yymm >=0704. Arxiv metadata starts from 2007-04
start_year = 2018
end_year = 2019
#####################################################################################################################

## Importing the required libraries


import os
from glob import glob
from multiprocessing import Pool
from arxiv_public_datasets.arxiv_public_data.fulltext import convert_directory_parallel
from time import time
from google.cloud.storage import Client, transfer_manager
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

if __name__ == '__main__':
    print('Running the main script')


    program_start_time = time() # Track the time taken to run the program

    #####################################################################################################################
    #####################################################################################################################
    ## Constants

    ## Track time
    start_time = time()

    ## Get the total number of cpus
    total_cpu = Pool()._processes
    # print(total_cpu)

    #####################################################################################################################
    #####################################################################################################################
    ## Functions

    ## Function to create a folder if it doesn't exist
    def create_folder(directory_path):
        """
        Creates a folder at the specified directory path if it doesn't already exist.

        Args:
            directory_path (str): The path of the directory to be created.

        Returns:
            None
        """
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

    ## Function to download a folder from the bucket
    def download_folder_transfer_manager(bucket_name, bucket_folder_name, local_folder_path, workers=total_cpu, max_results=10000):
        """Downloads a folder from the bucket using transfer manager.

        Args:
            bucket_name (str): The name of the bucket.
            bucket_folder_name (str): The name of the folder in the bucket.
            local_folder_path (str): The local path where the folder will be downloaded.
            workers (int, optional): The number of worker threads to use for parallel downloads. Defaults to total_cpu.
            max_results (int, optional): The maximum number of blob names to retrieve from the bucket. Defaults to 10000.

        Returns:
            None

        Raises:
            None

        """

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
            # pdf_name = pdf.split('/')[-1].split('.')[0] + '.' + pdf.split('/')[-1].split('.')[1]
            
            ## Get the txt name
            txt_name = pdf.replace('.pdf', '.txt')
            
            ## Check if txt file exists
            if txt_name in txt_files:
                
                ## Remove the pdf
                os.remove(pdf)

    ## Creating a list for the year and month
    def create_yymm_list(start_year, end_year):
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


    ## Create a yymm list from the year 2020 to 2023
    yymm_list = create_yymm_list(start_year, end_year)

    ## loop to download the files, convert them to text and delete the pdfs
    for yymm in yymm_list:

        ## Track the progress
        print(f"Processing {yymm}.")
        tic = time()

        ## Create a local folder path
        local_folder_path = f'unprocessed_txts/{yymm}'
        
        # ## Test whether things are working as expected
        download_folder_transfer_manager(bucket_name='arxiv-dataset', bucket_folder_name=f'arxiv/arxiv/pdf/{yymm}', local_folder_path=local_folder_path, max_results=10)

        ## Download all the pdfs published on Arxiv in the year 20yy and month mm
        # download_folder_transfer_manager(bucket_name='arxiv-dataset', bucket_folder_name=f'arxiv/arxiv/pdf/{yymm}', local_folder_path=local_folder_path)

        ## Convert all the pdfs in the yymm directory to text
        convert_directory_parallel(local_folder_path, total_cpu)

        ## Delete them pdfs if they have been converted to txts
        delete_pdfs_safe(local_folder_path)

        ## Track the progress
        toc = time()
        print(f"Downloading {yymm} took {toc - tic} seconds.")

    ## Track time
    end_time = time()
    print(f"Time taken to download the pdfs and convert them to txt files: {end_time - start_time} seconds.")

    #####################################################################################################################
    #####################################################################################################################
    #####################################################################################################################
    ## Step 3: Extract articles from the papers


    #####################################################################################################################
    #####################################################################################################################
    ## Constants

    ## Track time
    start_time = time()

    #####################################################################################################################
    #####################################################################################################################
    ## Functions
    ## Function to extract the article from the text file
    def find_text_after_term(content, term):

        ## Find the index of the term in the content
        index = content.find(term.lower()) 

        ## If the term is found in the content
        if index != -1:
            
            ## Extract the text after the term
            selection = content[index:] 

            ## Return the selection
            return selection
        
        else:
            return None
        
    # Function to process a file
    ## By default the function will extract the text after the term 'introduction'
    def process_file(file_path):
        
        ## Open the file and read the content
        with open(file_path, 'r') as rf:
            content = rf.read().lower() ## Convert the content to lower case

        ## Find the text after the term 'introduction'
        result = find_text_after_term(content, 'introduction')

        # If the term was not found, skip this file
        if result is None:
            print(f"Skipped file: {file_path}")
            return

        # Extract the directory from the source file path
        src_dir = os.path.dirname(file_path)

        # Append the source directory to the 'extracted_article' directory
        dst_dir = os.path.join('extracted_articles', src_dir)

        # Get the original filename
        original_filename = os.path.basename(file_path)

        # Create the destination file path
        dst = os.path.join(dst_dir, original_filename)

        # Create the destination directory if it does not exist
        os.makedirs(dst_dir, exist_ok=True)

        # Write the result to the new file
        with open(dst, 'w') as wf:
            wf.write(result)

    #####################################################################################################################
    #####################################################################################################################

    ## Main code

    ## Get a list of all txt files from the scientific_dataset*
    txt_files = glob('unprocessed_txts/**/*.txt', recursive=True)

    # Create a multiprocessing Pool
    pool = Pool()

    # Use the Pool to process the files in parallel
    pool.map(process_file, txt_files)

    # Close the Pool
    pool.close()
    pool.join()

    ## Track time
    end_time = time()
    print(f"Time taken to extract articles: {end_time - start_time} seconds.")