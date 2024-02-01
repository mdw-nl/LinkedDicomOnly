#!/usr/bin/env python

from LinkedDicomTe import LinkedDicom
from LinkedDicomTe.rt import dvh
from LinkedDicomTe.rt.dvh import calculate_dvh_folder, dose_summation_process
import os
import click
import pandas as pd
from util import upload_graph_db
from uuid import uuid4
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

query_example= """
            PREFIX ldcm: <https://johanvansoest.nl/ontologies/LinkedDicom/>
SELECT  distinct ?patientID ?rtDose ?rtStruct ?rtDosePath ?rtStructPath ?rtPlanPath ?fgn
                WHERE {
    				?data ldcm:has_study ?dcmStudy.
    				?data ldcm:T00100010 ?patientID.
                    ?rtPlan rdf:type ldcm:Radiotherapy_Plan_Object.
                    
                    ?dcmSerieRtPlan ldcm:has_image ?rtPlan.
    				?rtPlan schema:contentUrl ?rtPlanPath.
                    ?dcmStudy ldcm:has_series ?dcmSerieRtPlan.
                    
                    ?dcmStudy ldcm:has_series ?dcmSerieRtStruct.
                    ?dcmSerieRtStruct ldcm:has_image ?rtStruct.
                    ?rtStruct rdf:type ldcm:Radiotherapy_Structure_Object.
                    ?rtStruct schema:contentUrl ?rtStructPath.
                    
                    ?dcmStudy ldcm:has_series ?dcmSerieRtDose.
                    ?dcmSerieRtDose ldcm:has_image ?rtDose.
                    ?rtDose rdf:type ldcm:Radiotherapy_Dose_Object.
                    ?rtDose schema:contentUrl ?rtDosePath.
    				?rtPlan ldcm:T300A0070 ?fg.
					?fg ldcm:has_sequence_item ?fgg.
					?fgg ldcm:T300A0078 ?fgn.
					
                }
                
                
                """
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
@click.argument('query', type=str)
@click.option('-fl', '--ldcm_rdf_location', default=None, type=click.Path(exists=True))
@click.option('-ep', '--db_endpoint', default=None, type=str)
def calc_dvh(output_location, query=query_example, ldcm_rdf_location=None, db_endpoint=None):
    logging.info('Starting DVH Extraction')
    logging.info("File location: ", str(ldcm_rdf_location))
    logging.info("The output is saved in: ",str(output_location))
    if db_endpoint is not None and ldcm_rdf_location is None:
        dvh_factory = dvh.DVH_dicompyler(ldcm_rdf_location, urls=db_endpoint, query=query)
        dvh_factory.calculate_dvh(output_location)

    elif db_endpoint is None and ldcm_rdf_location is not None:
        dvh_factory = dvh.DVH_dicompyler(ldcm_rdf_location, query=query)
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
    for row in csv_data.itertuples():
        logging.info("working on patient: " + row.patientID)
        try:
            calculate_dvh_folder(row.pathRT,row.rtDosePath, patient_id=row.patientID,
                                 rt_plan_path=row.rtPlanPath,
                                 folder_to_store_results=output_folder)
        # TODO identify exception to catch
        except:
            continue


if __name__ == "__main__":
    main_parse()
