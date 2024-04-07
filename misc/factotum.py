## This code will be used to create the dataset from scratch

"""
Step 1: Download the papers from the google storage bucket
Step 2: Extract the text from the papers
Step 3: Extract articles from the papers
Step 4: Process the metadata
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
start_year = 2019
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

    #####################################################################################################################
    #####################################################################################################################
    #####################################################################################################################
    ## Step 4: Process the metadata

    ## Track the time taken to process the metadata
    start_time = time()

    ## Download the arXiv metadata from Kaggle
    api = KaggleApi() # Create an API client object
    api.authenticate() # Authenticate the API client

    ## Defining paths
    download_path = 'datasets'
    data_file = f'{download_path}/arxiv-metadata-oai-snapshot.json'

    ## Download the dataset if it doesn't exist
    if not os.path.exists(data_file):
        print(f'Downloading {data_file}')
        api.dataset_download_files('Cornell-University/arxiv', path=download_path, unzip=True)
    else:
        print(f'{data_file} already exists')

    ## Load the entire dataset into a pandas dataframe
    ## Beware, it take about 23 GBs of RAM. In case you dont have that much ram, the code/computer will crash
    print('Loading the entire dataset into a pandas dataframe')
    arxiv_metadata_all = pd.read_json(data_file, lines=True)

    ## Extract the metadata for the papers with id that starts with the elements in yymm_list
    print(f'Extracting the metadata for the papers with id that starts with {yymm_list}')
    arxiv_metadata = arxiv_metadata_all[arxiv_metadata_all['id'].str.match('|'.join(yymm_list))]

    ## Only save columns id, abstract, title
    print('Saving only columns id, abstract, title')
    arxiv_metadata = arxiv_metadata[['id', 'abstract', 'title']]

    ## Turn the contents of the dataframe to lowercase
    print('Turning the contents of the dataframe to lowercase')
    arxiv_metadata = arxiv_metadata.apply(lambda x: x.str.lower())

    ## Save the dataframe to a parquet file
    print('Saving the dataframe to a parquet file')
    trimmed_data_file = f'{download_path}/arxiv_metadata_{start_year}_to_{end_year}.parquet'
    arxiv_metadata.to_parquet(trimmed_data_file, index=False)

    ## Print the shape of the metadata dataframe
    print(f'Shape of metadata dataframe: {arxiv_metadata.shape}')

    ## Print the time taken to process the metadata
    end_time = time()
    print(f'Time taken to extract relevant metadata: {end_time - start_time:.2f} seconds')

    #####################################################################################################################
    #####################################################################################################################
    #####################################################################################################################
    ## Step 5: Create the dataset by merging the metadata and articles

    ## This script is used to merge the articles and abstracts into a single file

    ## Track the time taken to merge the articles and abstracts
    start_time = time()

    ## Helper functions
    ## Function to extract the id from the file path
    def extract_id_from_file(file_path):
        full_id = file_path.split('/')[-1].split('.')[0] + '.' + file_path.split('/')[-1].split('.')[1]
        id_without_version = full_id.split('v')[0]
        return id_without_version

    ## Function to process a file
    def process_file(file_path):

        ## Extract the id from the file path
        id_without_version = extract_id_from_file(file_path)

        ## Get the metadata for the id
        metadata = metadata_df[metadata_df['id'] == id_without_version]

        ## If the metadata is found
        if not metadata.empty:

            ## Get the title, abstract and article
            title = metadata['title'].values[0]
            abstract = metadata['abstract'].values[0]

            with open(file_path, 'r') as rf:
                article = rf.read()

            ## Create a new row
            new_row = pd.DataFrame({'id': [id_without_version], 'title': [title], 'abstract': [abstract], 'article': [article]}, index=[0])

            return new_row
        else:
            print(f"Metadata not found for {id_without_version}")
            return None
        
    ## Load the trimmed dataframe into memory
    print('Loading the trimmed metadata dataframe into memory')
    metadata_df = pd.read_parquet(trimmed_data_file)

    ## Create a dataset dataframe
    dataset_df = pd.DataFrame(columns=['id', 'title', 'abstract', 'article'])

    ## Get a list of all txt files from the extracted_articles
    txt_files = glob('extracted_articles/**/*.txt', recursive=True)
    txt_files.sort()

    print(f'Processing {len(txt_files)} files')

    ## Process the files in parallel
    with Pool() as pool:
        results = pool.map(process_file, txt_files)

    ## Concatenate the results into a single dataframe
    dataset_df = pd.concat([res for res in results if res is not None], ignore_index=True)

    ## Remove duplicates
    dataset_df.drop_duplicates(subset=['id'], keep='last', inplace=True)

    ## Save the dataset dataframe to a parquet file
    print('Saving the dataset dataframe to a parquet file')
    dataset_file = f'{download_path}/arxiv_dataset_{start_year}_to_{end_year}.parquet'
    dataset_df.to_parquet(dataset_file, index=False)

    ## Print the shape of the dataset dataframe
    print(f'Shape of dataset dataframe: {dataset_df.shape}')

    ## Print the time taken to merge the articles and abstracts
    end_time = time()
    print(f'Time taken to merge the extracted articles and abstracts: {end_time - start_time:.2f} seconds')

    ## Track the time taken to run the program
    program_end_time = time()

    print(f'Time taken to run the program: {program_end_time - program_start_time:.2f} seconds')

    #####################################################################################################################
    #####################################################################################################################
    #####################################################################################################################

    ## End of the code