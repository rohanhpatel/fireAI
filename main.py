import io
import os
import requests
import zipfile

def fetch_and_unpack(url, root_folder="data"):
    """
    Downloads a ZIP file from a URL and extracts it directly 
    into a structured root folder without leaving the .zip behind.
    """
    # 1. Create a clean folder name based on the URL (e.g., 'usfa_nfirs_2000')
    folder_name = url.split('/')[-1].replace('.zip', '')
    target_path = os.path.join(root_folder, folder_name)
    os.makedirs(target_path, exist_ok=True)

    print(f"Connecting to: {url}...")
    
    # 2. GET the data. We still use stream=True for stability.
    response = requests.get(url, stream=True)
    response.raise_for_status()

    # 3. Use BytesIO to capture the stream in memory
    print(f"Streaming data and extracting to {target_path}...")
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        zip_ref.extractall(target_path)
        
    print(f"Successfully unpacked {len(zip_ref.namelist())} files.")
    return target_path

zip_files = [
    
]

# Example Usage
if __name__ == "__main__":
    fema_url = "https://fema.gov/about/reports-and-data/openfema/nfirs/usfa_nfirs_2000.zip"
    fetch_and_unpack(fema_url)