#####################################################################################################################

## Import pymupdf library to extract text from pdf files
import fitz
from . import fixunicode

from multiprocessing import Pool, cpu_count
from pebble import ProcessPool, ProcessExpired
from functools import partial
from concurrent.futures import TimeoutError

import os
import glob
import re
import logging

#####################################################################################################################

TIMEOUT = 2*60  # Timeout in seconds

#####################################################################################################################

## Set up logging
# Create the log folder if it doesn't exist
log_folder = 'logs'  # Change this to your desired folder name
os.makedirs(log_folder, exist_ok=True)  # Create folder if it doesn't exist

# Get the filename of the currently executed Python script
current_filename = os.path.basename(__file__)
current_filename = current_filename.split('.')[0]

# Define the log file path within the folder
log_file = os.path.join(log_folder, f'{current_filename}.log')  # Join folder path and filename

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_file,  # Specify the log file path
    filemode='a'  # Append logs to the file
)

log = logging.getLogger(__name__)

#####################################################################################################################

## Helper functions
## Function to change the extension of a file
def reextension(filename: str, extension: str) -> str:
    """ Give a filename a new extension

    Args:
        filename (str): The original filename.
        extension (str): The new extension to be added.

    Returns:
        str: The filename with the new extension.

    """
    name, _ = os.path.splitext(filename)
    return '{}.{}'.format(name, extension)

## Function to extract text from a pdf file
def extract_text_from_pdf(pdf_path):
    """
    Extracts text from a PDF file.

    Args:
        pdf_path (str): The path to the PDF file.

    Returns:
        str: The extracted text from the PDF.
    """
    log.info(f"Extracting text from {pdf_path}")

    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()
    doc.close()
    log.debug(f"Extracted text from {pdf_path}")
    return text

def average_word_length(txt):
    """
    Gather statistics about the text, primarily the average word length

    Parameters
    ----------
    txt : str

    Returns
    -------
    word_length : float
        Average word length in the text
    """
    #txt = re.subn(RE_REPEATS, '', txt)[0]
    nw = len(txt.split())
    nc = len(txt)
    avgw = nc / (nw + 1)
    return avgw

#####################################################################################################################

def fulltext(pdffile: str):
    """
    Given a pdf file, extract the unicode text and run through very basic
    unicode normalization routines. Determine the best extracted text and
    return as a string.

    Parameters
    ----------
    pdffile : str
        Path to PDF file from which to extract text

    timelimit : int
        Time in seconds to allow the extraction routines to run

    Returns
    -------
    fulltext : str
        The full plain text of the PDF
    """
    if not os.path.isfile(pdffile):
        raise FileNotFoundError(pdffile)

    if os.stat(pdffile).st_size == 0:  # file is empty
        raise RuntimeError('"{}" is an empty file'.format(pdffile))

    output = extract_text_from_pdf(pdffile)

    output = fixunicode.fix_unicode(output)
    wordlength = average_word_length(output)

    if wordlength <= 45:

        log.debug('Fixed unicode and extracted text from "{}"'.format(pdffile))
        return output

    else:
        raise RuntimeError(
            'No accurate text could be extracted from "{}"'.format(pdffile)
        )
    
def sorted_files(globber: str):
    """
    Give a globbing expression of files to find. They will be sorted upon
    return.  This function is most useful when sorting does not provide
    numerical order,

    e.g.:
        9 -> 12 returned as 10 11 12 9 by string sort

    In this case use num_sort=True, and it will be sorted by numbers in the
    string, then by the string itself.

    Parameters
    ----------
    globber : str
        Expression on which to search for files (bash glob expression)


    """
    files = glob.glob(globber, recursive = True) # return a list of path, including sub directories
    files.sort()

    allfiles = []

    for fn in files:
        nums = re.findall(r'\d+', fn) # regular expression, find number in path names
        data = [str(int(n)) for n in nums] + [fn]
        # a list of [first number, second number,..., filename] in string format otherwise sorted fill fail
        allfiles.append(data) # list of list

    allfiles = sorted(allfiles)
    return [f[-1] for f in allfiles] # sorted filenames


def convert_directory(path: str):
    """
    Convert all pdfs in a given `path` to full plain text. For each pdf, a file
    of the same name but extension .txt will be created. If that file exists,
    it will be skipped.

    Parameters
    ----------
    path : str
        Directory in which to search for pdfs and convert to text

    Returns
    -------
    output : list of str
        List of converted files
    """
    outlist = []

    globber = os.path.join(path, '**/*.pdf') # search expression for glob.glob
    pdffiles = sorted_files(globber) # a list of paths 

    log.info('Searching "{}"...'.format(globber))
    log.info('Found: {} pdfs'.format(len(pdffiles)))

    for pdffile in pdffiles:
        txtfile = reextension(pdffile, 'txt')

        ## Skip conversion when there is a text file already
        if os.path.exists(txtfile):
            log.info('Skipping "{}"'.format(pdffile))
            continue

        # we don't want this function to stop half way because of one failed
        # file so just charge onto the next one
        try:
            text = fulltext(pdffile)
            with open(txtfile, 'w', encoding='utf-8') as f:
                f.write(text)
        except Exception as e:
            log.error("Conversion failed for '{}'".format(pdffile))
            log.exception(e)
            continue

        outlist.append(pdffile)
    return outlist

def convert_directory_parallel(path: str, processes: int = cpu_count()):
    """
    Convert all pdfs in a given `path` to full plain text. For each pdf, a file
    of the same name but extension .txt will be created. If that file exists,
    it will be skipped.

    Parameters
    ----------
    path : str
        Directory in which to search for pdfs and convert to text

    Returns
    -------
    output : list of str
        List of converted files
    """
    globber = os.path.join(path, '**/*.pdf') # search expression for glob.glob
    pdffiles = sorted_files(globber)  # a list of path

    log.info('Searching "{}"...'.format(globber))
    log.info('Found: {} pdfs'.format(len(pdffiles)))

    with ProcessPool(max_workers=processes) as pool:
        future = pool.map(convert_safe, pdffiles, timeout=TIMEOUT) # timeout in seconds
        iterator = future.result()

        while True:
            try:
                result = next(iterator)
                if result:
                    log.info('Converted "{}"'.format(result))
            except StopIteration:
                break
            except TimeoutError as error:
                log.debug("function took longer than %d seconds" % error.args[1])
            except ProcessExpired as error:
                log.debug("%s. Exit code: %d" % (error, error.exitcode))
            except Exception as error:
                log.debug("function raised %s" % error)
                log.debug(error.traceback)  # Python's traceback of remote process
                
def convert_safe(pdffile: str):
    """ Conversion function that never fails """
    try:
        convert(pdffile)
    except Exception as e:
        log.error('File conversion failed for {}: {}'.format(pdffile, e))


def convert(path: str) -> str:
    """
    Convert a single PDF to text.

    Parameters
    ----------
    path : str
        Location of a PDF file.

    Returns
    -------
    str
        Location of text file.
    """
    if not os.path.exists(path):
        raise RuntimeError('No such path: %s' % path)
    
    outpath = reextension(path, 'txt')

    ## Skip conversion when there is a text file already
    if os.path.exists(outpath):
        log.info('Skipping "{}"'.format(path))
        return outpath

    try:
        content = fulltext(path)

        log.debug('Writing text to "{}"'.format(outpath))

        with open(outpath, 'w', encoding='utf-8') as f:
            f.write(content)
            
        log.debug('Wrote text to "{}"'.format(outpath))

    except Exception as e:
        msg = "Conversion failed for '%s': %s"
        log.error(msg, path, e)
        raise RuntimeError(msg % (path, e)) from e
    return outpath
