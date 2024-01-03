import os

import requests


def save_list(my_list, path):
    with open(path, "w") as file:
        for item in my_list:
            file.write("%s\n" % item)


def read_list(path):
    if path is None:
        return None
    elif not check_if_exist(path):
        return None
    else:
        with open(path, "r") as file:
            relist = file.readlines()

        # Removing newline characters from each item
        relist = [item.strip() for item in relist]
        return relist


def check_if_exist(path):
    return os.path.exists(path)


def upload_graph_db(graphdb_url, repository_id, data_file, contenttype):
    """
    Import the data in graphdb based on the tupe specified by contetype(json/ttl tested)

    @param graphdb_url:
    @param repository_id:
    @param contenttype:
    @param data_file:
    @return:
    """

    # Define the URL for the GraphDB REST API endpoint to upload data
    upload_url = f"{graphdb_url}/repositories/{repository_id}/statements"

    # Load JSON-LD data from a file

    with open(data_file, "r") as file:
        data = file.read()
        data = data.encode('utf-8')

    # Set headers for the HTTP request
    headers = {
        "Content-Type": contenttype,
    }

    # Send a POST request to upload the data to GraphDB
    response = requests.post(upload_url, data=data, headers=headers)

    # Check the response
    if response.status_code in (200, 204):
        print("Data imported successfully.")
    else:
        print("Error importing data. Status code:", response.status_code)
        print("Response content:", response.content)

# Reading the list back from the plain text file
