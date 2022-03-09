import csv
import argparse
import os
import traceback
import pathlib
from typing import Dict, List, Tuple
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

# environment variable to retrieve key for Azure Storage access from if none is provided
ENV_KEY = "AZURE_STORAGE_CONNECTION_STRING"

log = print
DRY = False
RENAME = True

parser = argparse.ArgumentParser(description='Download files hosted on Azure Storage')
parser.add_argument('paths', nargs='*', help='path on azure storage to download')
parser.add_argument('-p', '--path', action='append', help='path on azure storage to download, can be repeated')
parser.add_argument('-o', '--output', default='download', help="output folder")
parser.add_argument('-c', '--container', help="download from a specific container (container name will be taken from path if this is not supplied)")
parser.add_argument('-f', '--file', help="file to parse paths to download from (txt, csv)")
parser.add_argument('--key', help="connection key to access azure storage (see: https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal). Remember to quote the connection string.")
parser.add_argument('--transform', action="store_true", help='transform and normalize file paths so they fit what Azure expects')
parser.add_argument('--dry', action="store_true", help='perform a dry run where nothing is downloaded or created')
parser.add_argument('--no-rename', action="store_true", help="don't rename the filename if it already exists in output folder (file will just be skipped)")
parser.add_argument('--silent', action="store_true", help='only print errors')
parser.add_argument('--delimiter', default=',', help="excel may use ';' as delimiter, you can change that here")


class AzureDownloader:

    blob_service_client: BlobServiceClient = None
    container_clients: Dict[str, ContainerClient] = {}

    def __init__(self, key: str, transform = False) -> None:
        self.key = key
        self.transform = transform
        self._fetched_paths = []

    def create(self, path):
        "Create the output folder if it doesnt already exist"
        if not DRY and not os.path.isdir(path):
            os.mkdir(path)

    def get_container_client(self, name: str):
        if not self.key:
            raise SystemExit("ERROR: no connection key to access azure storage provided")
        
        cls = AzureDownloader

        if not cls.blob_service_client:
            # Instantiate a BlobServiceClient using a connection string
            cls.blob_service_client = BlobServiceClient.from_connection_string(self.key)

        if name not in self.container_clients:
            # Instantiate a ContainerClient
            self.container_clients[name] = cls.blob_service_client.get_container_client(name)

        return self.container_clients[name]

    def _transform_paths(self, paths):
        "transform and normalize file paths so they fit what Azure expects"

        # if we encounter OriginalFiler, rename to OriginalFiles because that's what it is called on Azure Storage
        azure_paths = []

        for f in paths:
            fparts = list(pathlib.Path(f).parts)
            
            for i, name in enumerate(fparts.copy()):
                if name == 'OriginalFiler':
                    fparts[i] = 'OriginalFiles'

            ap = os.path.join(*fparts)
            # i think azure only accepts forward slashes
            ap = ap.replace('\\', '/')

            azure_paths.append(ap)
        return azure_paths

    def get_output_path(self, fpath: str, output_dir: str):
        """
        Return the path to save file to and whether it was renamed
        """
        # get filename
        fname = os.path.split(fpath)[1]
        opath = os.path.join(output_dir, fname)
        
        # rename path if it already exists
        rename_count = 0
        while os.path.exists(opath):
            rename_count += 1
            p, ext = os.path.splitext(fname)
            opath = os.path.join(output_dir, p+'_'+str(rename_count)+ext)

        return opath, rename_count > 0
        

    def download(self, output_dir: str, filepaths: List[str], container_name: str = None):
        """
        Download example files from Azure Storage into this format folder
        """

        if self.transform:
            filepaths = self._transform_paths(filepaths)

        default_container_name = container_name

        log(f"Downloading {len(filepaths)} file paths from Azure Storage...")

        # create folder if it doesnt already exist
        self.create(output_dir)

        download_count = 0

        # paths that fail to download
        fail_paths: List[Tuple[str, str]] = []
        # paths that were renamed
        renamed_paths: List[Tuple[str, str]] = []

        for fpath in filepaths:
            
            # i think azure only accepts forward slashes
            tpath = fpath.replace('\\', '/')

            try:
                
                output_path, renamed = self.get_output_path(fpath, output_dir)
                if renamed:
                    # if rename is not allowed, skip this file
                    if not RENAME:
                        continue

                if not default_container_name:
                    # get container name from path
                    container_name, *rest = tpath.split('/')
                    tpath = os.path.join("", *rest).replace('\\', '/')


                container_client = self.get_container_client(container_name)

                try:
                    # download file
                    stream = container_client.download_blob(tpath)
                except ResourceNotFoundError:
                    fail_paths.append((tpath, container_name))
                    print(f"WARN blob does not exist on container ({container_name}): '{tpath}'")
                    continue
                except HttpResponseError as e:
                    fail_paths.append((tpath, container_name))
                    print(f"WARN error when downloading blob on container ({container_name}) --\n\t{str(e)}: '{tpath}'")
                    continue

                if not DRY:
                    with open(output_path, 'wb') as f:
                        for chunk in stream.chunks():
                            f.write(chunk)

                if renamed:
                    renamed_paths.append((fpath, output_path))
                    
                download_count += 1
                self._fetched_paths.append(fpath)
            except Exception as e:
                log('')
                traceback.print_exc()
                raise SystemExit(f"ERROR: above error occurred when attempting to download the following file from container ({container_name})", fpath)

        log(f"Downloaded {download_count} file paths")

        if fail_paths:
            log(f"Failed to download one more paths - (container) path")
            for p, c in fail_paths:
                log(f"- ({c}) '{p}'")

        if renamed_paths:
            log(f"Renamed one or more files because the name already exists")
            for a, b in renamed_paths:
                log(f"- {a}\n\t-> {b}")

def main():
    global log, DRY, RENAME
    args = parser.parse_args()

    DRY = args.dry
    RENAME = not args.no_rename

    if args.silent:
        log = lambda *a, **kw: None

    connect_str = args.key or os.getenv(ENV_KEY, '')

    filepaths = []
    if args.paths:
        assert isinstance(args.paths, list)
        filepaths.extend(args.paths)

    if args.path:
        assert isinstance(args.paths, list)
        filepaths.extend(args.path)

    # read from files
    if args.file:
        if args.file.lower().endswith(".csv"):
            with open(args.file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=args.delimiter)
                for row in reader:
                    filepaths.append(row[0].strip())
        else:
            with open(args.file, 'r', encoding='utf-8') as f:
                for p in f.readlines():
                    filepaths.append(p.strip())

    downloader = AzureDownloader(connect_str)
    downloader.download(args.output, filepaths, container_name=args.container)

if __name__ == '__main__':
    main()
