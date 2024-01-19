#!/usr/bin/env python

from LinkedDicomTe import LinkedDicom
from LinkedDicomTe.rt import dvh
from LinkedDicomTe.rt.dvh import calculate_dvh_folder
import os
import click
import pandas as pd
from .util import upload_graph_db
from uuid import uuid4
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


@click.command()
@click.argument('dicom-input-folder', type=click.Path(exists=True))
@click.option('-o', '--ontology-file', help='Location of ontology file to use for override.')
@click.option('-fp', '--file-persistent', is_flag=True, default=False, help='Store file path while parsing metadata.')
@click.option('-ol', '--output_location', default=None, help='Store file path while parsing metadata.')
def main_parse(dicom_input_folder, ontology_file, file_persistent,
               output_location=None):
    """
    Search the DICOM_INPUT_FOLDER for dicom files, and process these files.
    The resulting turtle file can be stored in linkeddicom.ttl within this folder or in other location
    if the output_location has been provided.
    """
    ldcm = LinkedDicom.LinkedDicom(ontology_file)

    logging.info(f"Start processing folder {dicom_input_folder}. Depending on the folder size this might take a while.")

    ldcm.process_folder_exe(dicom_input_folder, persistent_storage=file_persistent)
    if output_location is None:
        output_location = os.path.join(dicom_input_folder, "linkeddicom.ttl")
        ldcm.saveResults(output_location)
        logging.info("Stored results in " + output_location)
    else:
        ldcm.saveResults(output_location)
        logging.info("Stored results in " + output_location)


@click.command()
@click.argument('dicom-input-folder', type=click.Path(exists=True))
@click.option('-o', '--ontology-file', help='Location of ontology file to use for override.')
@click.option('-fp', '--file-persistent', is_flag=True, default=False, help='Store file path while parsing metadata.')
@click.option('-ol', '--output_location', default=None, help='output file locaiton.')
@click.option('-ks', '--list_saved', default=None, help='save location')
@click.option('-nf', '--number_file', type=int, default=None, help='number file to process')
def main_parse_test(dicom_input_folder, ontology_file, file_persistent,
                    list_saved, number_file, output_location=None):
    """
    Search the DICOM_INPUT_FOLDER for dicom files, and process these files.
    The resulting turtle file can be stored in linkeddicom.ttl within this folder or in other location
    if the output_location has been provided.
    """
    ldcm = LinkedDicom.LinkedDicom(ontology_file)

    logging.info(f"Start processing folder {dicom_input_folder}. Depending on the folder size this might take a while.")

    ldcm.process_folder_exe(dicom_input_folder, file_persistent, list_saved, number_file)
    uuid_for_calculation_str = str(uuid4())
    logging.info(f"Calculation completed saving file....")
    if output_location is None:
        output_location = os.path.join(dicom_input_folder, uuid_for_calculation_str + "_linkeddicom.ttl")
        ldcm.saveResults(output_location)
        logging.info("Stored results in " + output_location)
    else:
        ldcm.saveResults(output_location + uuid_for_calculation_str + "_linkeddicom.ttl")
        logging.info("Stored results in " + output_location)


@click.command()
@click.argument('output_location', type=click.Path(exists=False))
@click.option('-fl', '--ldcm_rdf_location', default=None, type=click.Path(exists=True))
@click.option('-ep', '--db_endpoint', default=None, type=str)
def calc_dvh(output_location, ldcm_rdf_location=None, db_endpoint=None):
    logging.info('Starting DVH Extraction')
    logging.info(str(ldcm_rdf_location))
    logging.info(str(output_location))
    if db_endpoint is not None and ldcm_rdf_location is None:
        dvh_factory = dvh.DVH_dicompyler(ldcm_rdf_location, urls=db_endpoint)
        dvh_factory.calculate_dvh(output_location)

    elif db_endpoint is None and ldcm_rdf_location is not None:
        dvh_factory = dvh.DVH_dicompyler(ldcm_rdf_location)
        dvh_factory.calculate_dvh(output_location)
    else:
        raise Exception("Missing ttl file location or graphdb address")


@click.command()
@click.argument('db_host', type=str)
@click.argument('repo_db', type=str)
@click.argument('file', type=click.Path(exists=True))
def upload_graph(db_host, repo_db, file):
    upload_graph_db(db_host, repo_db, file, contenttype="application/x-turtle")


@click.command()
@click.argument('path_file', type=click.Path(exists=False))
@click.argument('output_folder', type=click.Path(exists=False))
def DVH_from_folder_file(path_file, output_folder):
    csv_data: pd.DataFrame = pd.read_csv(path_file)
    for i, r in csv_data.iterrows():
        logging.info("working on patient: " + r["patientID"])
        try:
            calculate_dvh_folder(rtStructPath=r["pathRT"], patientID=r["patientID"],
                                 rtDosePath=r["rtDosePath"], rtPlanPath=r["rtPlanPath"],
                                 folder_to_store_results=output_folder)
        except:
            continue


if __name__ == "__main__":
    main_parse()
    # DVH_from_folder_file("/Users/alessioromita/PRE_ACT/HypogListStructureconnect.csv", None)
