import urllib.request
import os
import shutil

def dowload_file (url:str,dir_path:str,local_apth:str):
    """
    Downloads a file from a given URL to a specified local directory.
    """
    os.makedirs(os.path.dirname(dir_path), exist_ok=True)
    
    with urllib.request.urlopen(url) as response, open(local_apth, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)
        
    