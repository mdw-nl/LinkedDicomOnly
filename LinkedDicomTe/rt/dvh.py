from LinkedDicomTe import RDFService
import datetime
from abc import ABC, abstractmethod
from dicompylercore import dicomparser, dvh, dvhcalc  # TODO do not load this module if dicompyler is not used
import dicompylercore
from uuid import uuid4
import rdflib
import json
import os
import numpy as np
import logging
from rdflib.plugins.stores.sparqlstore import SPARQLStore


def get_dvh_v(structure,
              dose,
              roi,
              rtplan,
              limit=None,
              calculate_full_volume=True,
              use_structure_extents=False,
              interpolation_resolution=None,
              interpolation_segments_between_planes=0,
              thickness=None,
              memmap_rtdose=False,
              callback=None):
    """Calculate a cumulative DVH in Gy from a DICOM RT Structure Set & Dose.
        Take as input the RTplan to calculate the Vx (v10,20 etc..)

    Parameters
    ----------
    structure : pydicom Dataset or filename
        DICOM RT Structure Set used to determine the structure data.
    dose : pydicom Dataset or filename
        DICOM RT Dose used to determine the dose grid.
    roi : int
        The ROI number used to uniquely identify the structure in the structure
        set.
    rtplan : pydicom Dataset or filename
        DICOM RT plan path

    limit : int, optional
        Dose limit in cGy as a maximum bin for the histogram.
    calculate_full_volume : bool, optional
        Calculate the full structure volume including contours outside of the
        dose grid.
    use_structure_extents : bool, optional
        Limit the DVH calculation to the in-plane structure boundaries.
    interpolation_resolution : tuple or float, optional
        Resolution in mm (row, col) to interpolate structure and dose data to.
        If float is provided, original dose grid pixel spacing must be square.
    interpolation_segments_between_planes : integer, optional
        Number of segments to interpolate between structure slices.
    thickness : float, optional
        Structure thickness used to calculate volume of a voxel.
    memmap_rtdose : bool, optional
        Use memory mapping to access the pixel array of the DICOM RT Dose.
        This reduces memory usage at the expense of increased calculation time.
    callback : function, optional
        A function that will be called at every iteration of the calculation.

    Returns
    -------
    dvh.DVH
        An instance of dvh.DVH in cumulative dose. This can be converted to
        different formats using the attributes and properties of the DVH class.
    """

    #rtplan = rtplan.replace("/data/pre-act/mnt/", "/Volumes/research/Projects/cds/p0630-pre-act-dm/")
    rtss = dicomparser.DicomParser(structure)
    rtdose = dicomparser.DicomParser(dose, memmap_pixel_array=memmap_rtdose)
    structures = rtss.GetStructures()
    s = structures[roi]
    s['planes'] = rtss.GetStructureCoordinates(roi)
    s['thickness'] = thickness if thickness else rtss.CalculatePlaneThickness(
        s['planes'])
    rt_plan = dicomparser.DicomParser(rtplan)

    plan = rt_plan.GetPlan()

    calcdvh = dicompylercore.dvhcalc._calculate_dvh(s, rtdose, limit, calculate_full_volume,
                                                    use_structure_extents, interpolation_resolution,
                                                    interpolation_segments_between_planes,
                                                    callback)
    if plan['rxdose'] is not None:

        return dvh.DVH(counts=calcdvh.histogram,
                       bins=(np.arange(0, 2) if (calcdvh.histogram.size == 1) else
                             np.arange(0, calcdvh.histogram.size + 1) / 100),
                       dvh_type='differential',
                       dose_units='Gy',
                       notes=calcdvh.notes,
                       name=s['name'],
                       rx_dose=plan['rxdose'] / 100).cumulative
    else:
        return dvh.DVH(counts=calcdvh.histogram,
                       bins=(np.arange(0, 2) if (calcdvh.histogram.size == 1) else
                             np.arange(0, calcdvh.histogram.size + 1) / 100),
                       dvh_type='differential',
                       dose_units='Gy',
                       notes=calcdvh.notes,
                       name=s['name']).cumulative


class DVH_factory(ABC):
    def __init__(self, file_path, urls=None):
        """
        :param file_path:
        :param urls:
        """
        if file_path is not None:
            self.__ldcm_graph = RDFService.GraphService(file_path)
        else:
            store = SPARQLStore(urls)
            self.__ldcm_graph = RDFService.GraphService(file_path, store)

    def get_ldcm_graph(self):
        """
        :return:
        """
        return self.__ldcm_graph

    @abstractmethod
    def calculate_dvh(self, folder_to_store_results):
        pass


class DVH_dicompyler(DVH_factory):

    def __find_complete_packages(self):
        """
        :return:
        """
        """
        Execute SPARQL query on the ttl file to get the RtDose, RtStruct and patientId
        :return:
        """
        logging.info("Execution Query...")
        query = """
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
        ldcm = self.get_ldcm_graph()
        dose_objects = ldcm.runSparqlQuery(query)
        return dose_objects

    def calculate_dvh(self, folder_to_store_results):
        """

        :param folder_to_store_results:
        :return:
        """
        logging.info('Retrieving data from ttl file...')
        # if db_add is None:
        dcmDosePackages = self.__find_complete_packages()
        # else:
        #     dcmDosePackages = self.__find_complete_packages_gdb(db_add)

        logging.info("Data retrieve completed.")
        logging.info('Reading the data...')

        for dosePackage in dcmDosePackages:
            logging.info(
                f"Processing  {dosePackage.patientID} | {dosePackage.rtDosePath} | {dosePackage.rtStructPath} |"
                f"{dosePackage.rtPlanPath} | {dosePackage.fgn}...")
            logging.info("Starting Calculation...")
            try:
                calculatedDose = self.__get_dvh_for_structures(dosePackage.rtStructPath, dosePackage.rtDosePath,
                                                               dosePackage.rtPlanPath)
            except Exception as ex:
                print(ex)
                logging.warning(ex)
                logging.info("Error skipping")
                continue
            logging.info("Calculation Complete ")
            uuid_for_calculation = uuid4()
            resultDict = {
                "@context": {
                    "CalculationResult": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/CalculationResult",
                    "PatientID": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/PatientIdentifier",
                    "doseFraction": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/DoseFractionNumbers",
                    "references": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/references",
                        "@type": "@id"
                    },
                    "software": {
                        "@id": "https://schema.org/SoftwareApplication",
                        "@type": "@id"
                    },
                    "version": "https://schema.org/version",
                    "dateCreated": "https://schema.org/dateCreated",
                    "containsStructureDose": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/containsStructureDose",
                        "@type": "@id"
                    },
                    "structureName": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/structureName",
                    "min": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/min",
                        "@type": "@id"
                    },
                    "mean": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/mean",
                        "@type": "@id"
                    },
                    "max": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/max",
                        "@type": "@id"
                    },
                    "volume": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/volume",
                        "@type": "@id"
                    },
                    "D10": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/D10",
                        "@type": "@id"
                    },
                    "D20": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/D20",
                        "@type": "@id"
                    },
                    "D30": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/D30",
                        "@type": "@id"
                    },
                    "D40": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/D40",
                        "@type": "@id"
                    },
                    "D50": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/D50",
                        "@type": "@id"
                    },
                    "D60": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/D60",
                        "@type": "@id"
                    },
                    "V5": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/V5",
                        "@type": "@id"
                    },
                    "V10": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/V10",
                        "@type": "@id"
                    },
                    "V20": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/V20",
                        "@type": "@id"
                    },
                    "dvh_points": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/dvh_point",
                        "@type": "@id"
                    },
                    "4": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/dvh_curve",
                        "@type": "@id"
                    },
                    "d_point": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/dvh_d_point",
                    "v_point": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/dvh_v_point",
                    "Gray": "http://purl.obolibrary.org/obo/UO_0000134",
                    "cc": "http://purl.obolibrary.org/obo/UO_0000097",
                    "unit": "@type",
                    "value": "https://schema.org/value",
                    "has_color": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/has_color"
                },
                "@type": "CalculationResult",
                "@id": "http://data.local/ldcm-rt/" + str(uuid_for_calculation),
                "PatientID": dosePackage.patientID,
                "doseFraction": dosePackage.fgn,
                "references": [dosePackage.rtDose, dosePackage.rtStruct],
                "software": {
                    "@id": "https://github.com/dicompyler/dicompyler-core",
                    "version": dicompylercore.__version__
                },
                "dateCreated": datetime.datetime.now().isoformat(),
                "containsStructureDose": [calculatedDose]
            }

            filename = os.path.join(folder_to_store_results, f"{uuid_for_calculation}.jsonld")
            logging.info("Saving in" + str(filename))
            with open(filename, "w") as f:
                json.dump(resultDict, f)
                logging.info("Saving done")


    def __get_dvh_for_structures(self, rtStructPath, rtDosePath, rtPlan):
        """
            Calculate DVH parameters for all structures available in the RTSTRUCT file.
            Input:
                - rtStructPath: an URIRef or string containing the file path of the RTSTRUCT file
                - rtDosePath: an URIRef or string containing the file path of the RTDOSE file
                - rtPlan
            Output:
                - A python list containing a dictionaries with the following items:
                    - structureName: name of the structure as given in the RTSTRUCT file
                    - min: minimum dose to the structure
                    - mean: mean dose for this structure
                    - max: maximum dose for this structure
                    - volume: volume of the structure
                    - color: color (Red Green Blue) for the structure on a scale of 0-255
                    - dvh_d: list of dose values on the DVH curve
                    - dvh_v: list of volume values on the DVH curve
            """

        if type(rtStructPath) == rdflib.term.URIRef:
            rtStructPath = str(rtStructPath).replace("file://", "")
        #rtStructPath = rtStructPath.replace("/data/pre-act/mnt/", "/Volumes/research/Projects/cds/p0630-pre-act-dm/")
        structObj = dicomparser.DicomParser(rtStructPath)

        if type(rtDosePath) == rdflib.term.URIRef:
            rtDosePath = str(rtDosePath).replace("file://", "")
        #rtDosePath = rtDosePath.replace("/data/pre-act/mnt/", "/Volumes/research/Projects/cds/p0630-pre-act-dm/")

        structures = structObj.GetStructures()
        dvh_list = []
        for index in structures:
            logging.info("Calculating structures " + str(structures[index]))
            structure = structures[index]
            try:
                calcdvh = get_dvh_v(rtStructPath, rtDosePath, index, rtPlan)
            except Exception as exeppp:
                logging.warning("Skipping...")
                continue
            dvh_d = calcdvh.bincenters.tolist()

            dvh_v = calcdvh.counts.tolist()
            dvh_points = []
            for i in range(0, len(dvh_d)):
                dvh_points.append({
                    "d_point": dvh_d[i],
                    "v_point": dvh_v[i]
                })

            try:
                V5value = float(calcdvh.V5.value)
                V10value = float(calcdvh.V10.value)
                V20value = float(calcdvh.V20.value)

            except Exception as e:
                logging.warning("Value not available exception =")
                logging.error(e)
                V5value = None
                V10value = None
                V20value = None

            id = "http://data.local/ldcm-rt/" + str(uuid4())
            try:
                structOut = {
                    "@id": id,
                    "structureName": structure["name"],
                    "min": {"@id": f"{id}/min", "unit": "Gray", "value": calcdvh.min},
                    "mean": {"@id": f"{id}/mean", "unit": "Gray", "value": calcdvh.mean},
                    "max": {"@id": f"{id}/max", "unit": "Gray", "value": calcdvh.max},
                    "volume": {"@id": f"{id}/volume", "unit": "cc", "value": int(calcdvh.volume)},
                    "D10": {"@id": f"{id}/D10", "unit": "Gray", "value": float(calcdvh.D10.value)},
                    "D20": {"@id": f"{id}/D20", "unit": "Gray", "value": float(calcdvh.D20.value)},
                    "D30": {"@id": f"{id}/D30", "unit": "Gray", "value": float(calcdvh.D30.value)},
                    "D40": {"@id": f"{id}/D40", "unit": "Gray", "value": float(calcdvh.D40.value)},
                    "D50": {"@id": f"{id}/D50", "unit": "Gray", "value": float(calcdvh.D50.value)},
                    "D60": {"@id": f"{id}/D60", "unit": "Gray", "value": float(calcdvh.D60.value)},
                    "V5": {"@id": f"{id}/V5", "unit": "Gray", "value": V5value},
                    "V10": {"@id": f"{id}/V10", "unit": "Gray", "value": V10value},
                    "V20": {"@id": f"{id}/V20", "unit": "Gray", "value": V20value},
                    "color": ','.join(str(e) for e in structure.get("color", np.array([])).tolist()),

                    "dvh_curve": {
                        "@id": f"{id}/dvh_curve",
                        "dvh_points": dvh_points
                    }
                }
            except Exception as e:
                print(e)
                logging("error")
                logging.warning(e)
                continue
            dvh_list.append(structOut)
        return dvh_list
