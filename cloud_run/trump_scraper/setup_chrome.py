import os
import zipfile
import urllib.request

def download_headless_shell():
    output_dir = "./chrome_binary"
    # Official Google URL for the lightweight headless engine core
    url = "https://storage.googleapis.com/chrome-for-testing-public/124.0.6367.60/linux64/chrome-headless-shell-linux64.zip"
    zip_path = os.path.join(output_dir, "headless-shell.zip")
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    print("[1/3] Downloading unprivileged Chrome Headless Shell binary...")
    urllib.request.urlretrieve(url, zip_path)
    
    print("[2/3] Extracting core components...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)
        
    os.remove(zip_path)
    
    # Apply execution permissions to the binary process
    binary_path = os.path.abspath(os.path.join(output_dir, "chrome-headless-shell-linux64/chrome-headless-shell"))
    os.chmod(binary_path, 0o755)
    
    print(f"[3/3] Done! Headless engine established at: {binary_path}")

if __name__ == "__main__":
    download_headless_shell()