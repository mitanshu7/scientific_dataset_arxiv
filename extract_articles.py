#####################################################################################################################
## Step 3: Extract articles from the papers

## Importing the necessary libraries
import os
from time import time
from glob import glob
from multiprocessing import Pool
from scientific_dataset_arxiv.config import start_year, end_year

#####################################################################################################################
#####################################################################################################################
## Functions
## Function to extract the article from the text file
def find_text_after_term(content, term):
    """
    Extracts the text after a given term in the content.

    Parameters:
    content (str): The content to search for the term.
    term (str): The term to search for.

    Returns:
    str: The text after the term, or None if the term is not found.
    """
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
    """
    Processes a file by extracting the text after a given term and saving it to a new file.

    Parameters:
    file_path (str): The path of the file to process.
    """
    ## Open the file and read the content
    with open(file_path, 'r', encoding='utf-8') as rf:
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
    dst_dir = os.path.join(f'extracted_articles_{start_year}_to_{end_year}', src_dir)

    # Get the original filename
    original_filename = os.path.basename(file_path)

    # Create the destination file path
    dst = os.path.join(dst_dir, original_filename)

    # Create the destination directory if it does not exist
    os.makedirs(dst_dir, exist_ok=True)

    # Write the result to the new file
    with open(dst, 'w', encoding='utf-8') as wf:
        wf.write(result)

#####################################################################################################################
#####################################################################################################################

## Main code

if __name__ == '__main__':
    """
    Main code to extract articles from the papers.
    """
    ## Start the timer
    tic = time()

    ## Get a list of all txt files from the unprocessed_txts directory
    txt_files = glob(f'unprocessed_txts_{start_year}_to_{end_year}/**/*.txt', recursive=True)

    # Create a multiprocessing Pool
    pool = Pool()

    # Use the Pool to process the files in parallel
    pool.map(process_file, txt_files)

    # Close the Pool
    pool.close()
    pool.join()

    ## Stop the timer
    toc = time()
    print(f"Time taken: {toc - tic:.2f} seconds")

    

#####################################################################################################################
#####################################################################################################################