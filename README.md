
# Create a scientific dataset like [this](https://huggingface.co/datasets/scientific_papers).

This project contains several Python scripts that are used to process arxiv metadata and create a dataset.

## Setup

1. Create a new virtual environment from your preferred python distribution.
2. Install using `pip install -r requirements.txt` if using pip or `conda env create -f environment.yml` if using Anaconda or Miniforge.
3. Setup `scientific_dataset_arxiv/config.py` file for intended usage. You can configure the start and end year, and the maximum pdfs you want to download per month.
4. You can also customise the search term after which the data would be returned from the txt files. The default `search_term = 'introduction'` is a good choice. This is how the reference dataset was created too.


## Scripts

1. `download_convert.py`: This script is used to download PDFs from Arxiv GCP bucket and convert them into text files.
2. `merge_metadata_articles.py`: This script is used to merge the metadata, which contains ID, title, and abstract, with the articles extracted.
3. `merge_parquet.py`: This script is used to merge all the files together into one dataset.
4. Check out a sample of the end result in the `test_merged_parquet.ipynb` notebook.


## Usage

To use these scripts, run them in the order listed above. Make sure to replace start_year, end_year, and max_pdfs_per_month for your preferred years to get the dataset for in all four scripts.

In the end, you should end up with a dataset that looks a little like [scientific_papers](https://huggingface.co/datasets/scientific_papers). However, it is updated with the latest articles for a more up to date training!

## Note

You can find preprocessed data [here](https://huggingface.co/datasets/bluuebunny/arxiv_dataset_by_year). 
This was calulated for upto max 10,000 pdf downloads per month for the years 2007 to 2023. 
However, many files would be discarded either due to loss in conversion to text file or missing search_term.
Check out the `data` folder in the Hugging Face repo to find yearwise parquet files.
 
You can find the raw data, which combines metadata from [Arxiv](https://www.kaggle.com/datasets/Cornell-University/arxiv) and the full text extracted from the pdfs, [here](https://huggingface.co/datasets/bluuebunny/arxiv_raw_dataset_by_year) into a single parquet file for futher customised processing. 
Check out the `data` folder in the Hugging Face repo to find yearwise parquet files.

## Some screenshots of the same

1. Hugging Face reference dataset:
![Screenshot (261)](https://github.com/mitanshu7/scientific_dataset_arxiv/assets/39945712/80658083-f608-431d-9cef-d5772643a8c1)

2. Self preprocessed dataset:
![Screenshot (264)](https://github.com/mitanshu7/scientific_dataset_arxiv/assets/39945712/ec4a1b73-8a2b-481a-9a86-0f135720062e)

3. Raw dataset:
![Screenshot (262)](https://github.com/mitanshu7/scientific_dataset_arxiv/assets/39945712/b28228bc-c3bf-4983-9227-0c04713f6c66)
![Screenshot (263)](https://github.com/mitanshu7/scientific_dataset_arxiv/assets/39945712/80d5c7b3-d9bb-4426-afde-131b0d1385e3)
 
