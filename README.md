
# Process Arxiv Metadata

This project contains several Python scripts that are used to process arxiv metadata.

## Scripts

1. `download_convert.py`: This script is used to download PDFs from a GCP bucket and convert them into text files.

2. `extract_articles.py`: This script is used to extract articles from the text files generated by `download_convert.py`.

3. `merge_metadata_articles.py`: This script is used to merge the metadata, which contains ID, title, and abstract, with the articles extracted by `extract_articles.py`.

4. `merge_parquet.py`: This script is used to merge all the files together into one dataset.

## Usage

To use these scripts, run them in the order listed above. Make sure to replace start_year and end_year for your preffered years to get the dataset for.

## Note

All the scripts make extensive use of the multiprocessing module to significantly speed up the job
