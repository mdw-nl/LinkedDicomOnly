#!/usr/bin/env python

from LinkedDicom import LinkedDicom
from LinkedDicom.rt import dvh
import os
import click
import requests


def upload_graph_db(graphdb_url, repository_id, data_file, contenttype):
    """

    @param graphdb_url:
    @param repository_id:
    @param json_data_path:
    @return:
    """

    # Define the URL for the GraphDB REST API endpoint to upload data
    upload_url = f"{graphdb_url}/repositories/{repository_id}/statements"

    # Load JSON-LD data from a file

    with open(data_file, "r") as file:
        data = file.read()

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


@click.command()
@click.argument('dicom-input-folder', type=click.Path(exists=True))
@click.option('-o', '--ontology-file', help='Location of ontology file to use for override.')
@click.option('-fp', '--file-persistent', is_flag=True, default=False, help='Store file path while parsing metadata.')
@click.option('-ol', '--output_location', default=None, help='Store file path while parsing metadata.')
def main_parse(dicom_input_folder, ontology_file, file_persistent, output_location=None):
    """
    Search the DICOM_INPUT_FOLDER for dicom files, and process these files.
    The resulting turtle file will be stored in linkeddicom.ttl within this folder
    """
    ldcm = LinkedDicom.LinkedDicom(ontology_file)

    print(f"Start processing folder {dicom_input_folder}. Depending on the folder size this might take a while.")

    ldcm.processFolder(dicom_input_folder, persistentStorage=file_persistent)
    if output_location is None:
        output_location = os.path.join(dicom_input_folder, "linkeddicom.ttl")
        ldcm.saveResults(output_location)
        print("Stored results in " + output_location)
    else:
        ldcm.saveResults(output_location)
        print("Stored results in " + output_location)


@click.command()
@click.argument('ldcm-rdf-location', type=click.Path(exists=True))
@click.argument('output_location', type=click.Path(exists=False))
def calc_dvh(ldcm_rdf_location, output_location):
    dvh_factory = dvh.DVH_dicompyler(ldcm_rdf_location)
    dvh_factory.calculate_dvh(output_location)


@click.command()
@click.argument('db_host', type=str)
@click.argument('repo_db', type=str)
@click.argument('file', type=click.Path(exists=True))
def upload_graph(db_host, repo_db, file):
    upload_graph_db(db_host, repo_db, file, contenttype="application/x-turtle")


if __name__ == "__main__":
    main_parse()