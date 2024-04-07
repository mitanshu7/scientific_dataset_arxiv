if __name__ == '__main__':    
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
