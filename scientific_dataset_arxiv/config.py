#####################################################################################################################
## Enter the year range for which you want to download the papers and convert them to text files
## The year range is inclusive, valid for yymm >=0704. Arxiv metadata starts from 2007-04
start_year = 2007
end_year = 2023
#####################################################################################################################
## Here you can set the maximum number of PDFs to download and convert per month
## The default value is 10000. In the merged dataset, you will get about the 50% of the maximum value.
## For example, if you download for 1 year, you'll download 12 months * 10,000 PDFs = 1,00,000 PDFs. 
## The merged dataset will have about 50,000 articles.
## If you want to download all the articles, set the value to None.
## The skip_n is used to skip the first n PDFs. The default value is 0.
## The following is used in download_convert.py
max_pdfs_per_month = 20000
skip_n = 10000
#####################################################################################################################
## Here you can decide on the search term in the text file to extract the article.
## The extracted article is the content after the search term.
## The search term is case-insensitive.
## The default search term is 'introduction'.
## The following is used in merge_metadata_articles.py
search_term = 'introduction'