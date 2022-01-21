## Download files from Azure Storage

### Instructions

The script accepts three way of providing filepaths to download, all three ways can be combined freely:

- `python download.py path/1.pdf path/2.pdf path/3.pdf`
- `python download.py -p path/1.pdf -p path/2.pdf --path path/3.pdf`
- `python download.py -f paths.csv` read a list of paths from a file, line delimitered

The script requires a key to connect to Azure Storage, this key is provided with `--key` or by setting it to the environment variable `AZURE_STORAGE_CONNECTION_STRING` 

### Help

```
usage: download.py [-h] [-p PATH] [-o OUTPUT] [-c CONTAINER] [-f FILE] [--key KEY] [--transform]
                   [--dry] [--no-rename] [--silent] [--delimiter DELIMITER]
                   [paths ...]

Download files hosted on Azure Storage

positional arguments:
  paths                 path on azure storage to download

options:
  -h, --help            show this help message and exit
  -p PATH, --path PATH  path on azure storage to download, can be repeated
  -o OUTPUT, --output OUTPUT
                        output folder
  -c CONTAINER, --container CONTAINER
                        download from a specific container (container name will be taken from      
                        path if this is not supplied)
  -f FILE, --file FILE  file to parse paths to download from (txt, csv)
  --key KEY             connection key to access azure storage (see:
                        https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-   
                        blobs-python#copy-your-credentials-from-the-azure-portal)
  --transform           transform and normalize file paths so they fit what Azure expects
  --dry                 perform a dry run where nothing is downloaded or created
  --no-rename           don't rename the filename if it already exists in output folder (file      
                        will just be skipped)
  --silent              only print errors
  --delimiter DELIMITER
                        excel may use ';' as delimiter, you can change that here
```