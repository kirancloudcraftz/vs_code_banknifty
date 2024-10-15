
from google.cloud import storage
import os
import re
import tkinter as tk
from tkinter import filedialog

def upload_to_gcs(bucket_name, source_file_path, gcs_destination_path):
    
    # Uploads csv files to Google Cloud Storage with the selection folder.
    
    # Initialize a storage client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Create a blob (GCS object) using the GCS path provided
    blob = bucket.blob(gcs_destination_path)

    # Upload the file from the local path
    blob.upload_from_filename(source_file_path)

    print(f"File {source_file_path} uploaded to {gcs_destination_path}.")

def select_folder():
  
  root = tk.Tk()
  root.withdraw()  # Hide the main window

  folder_path = filedialog.askdirectory()

  return folder_path + "/"

# Get the user selected folder path
source_folder_path = select_folder()

# Verify selected folder
print("You selected the folder:", source_folder_path)

# List down all the sources files in a list
list1 = os.listdir(source_folder_path)

for i in list1:
    
    source_file_path = source_folder_path + i # generate the each file path for processing

    # created logic for destination path using source file
    # Extract the date string from the file name
    date_string = re.search(r"\d{8}", source_file_path).group()

    # Extract the year, month, and day separately
    year = date_string[:4]
    month = date_string[4:6]
    day = date_string[6:]
    
    # Define the GCS path with the same folder hierarchy
    gcs_destination_path = f"BANKNIFTY/{year}/{month}/{i}"
    
    # Define the bucket
    bucket_name = "testingbanknifty"
    
    upload_to_gcs(bucket_name, source_file_path, gcs_destination_path)
    
