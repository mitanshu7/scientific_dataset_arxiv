#####################################################################################################################
## Enter the year range for which you want to download the papers and convert them to text files
## The year range is inclusive, valid for yymm >=0704. Arxiv metadata starts from 2007-04
start_year = 2019
end_year = 2020
#####################################################################################################################
## Here you can set the maximum number of PDFs to download and convert per month
## The default value is 10000. In the merged dataset, you will get about the 50% of the maximum value.
## For example, if you download for 1 year, you'll download 12 months * 10,000 PDFs = 1,00,000 PDFs. 
## The merged dataset will have about 50,000 articles.
max_pdfs_per_month = 1