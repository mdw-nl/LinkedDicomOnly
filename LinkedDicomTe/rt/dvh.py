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
import pydicom
from dicompylercore import dose


def get_dvh_for_structures(rt_struct_path, rt_dose_data, rt_plan_path=None):
    """
            Calculate DVH parameters for all structures available in the RT-STRUCT file.
            Input:
                - rtStructPath: an URIRef or string containing the file path of the RT-STRUCT file
                - rtDosePath: an URIRef or string containing the file path of the RT-DOSE file or the rt-dose itself
                - rtPlan
            Output:
                - A python list containing a dictionaries with the following items:
                    - structureName: name of the structure as given in the RT-STRUCT file
                    - min: minimum dose to the structure
                    - mean: mean dose for this structure
                    - max: maximum dose for this structure
                    - volume: volume of the structure
                    - color: color (Red Green Blue) for the structure on a scale of 0-255
                    - dvh_d: list of dose values on the DVH curve
                    - dvh_v: list of volume values on the DVH curve
            """
    dvh_list = []  # result dvh

    if type(rt_struct_path) == rdflib.term.URIRef:
        rt_struct_path = str(rt_struct_path).replace("file://", "")
    structObj = dicomparser.DicomParser(rt_struct_path)

    # RT-plan can be empty

    if type(rt_dose_data) == rdflib.term.URIRef:
        rt_dose_data = str(rt_dose_data).replace("file://", "")
    structures = structObj.GetStructures()

    for index in structures:
        logging.info("Calculating structures " + str(structures[index]))
        structure = structures[index]
        try:
            calc_dvh = get_dvh_v(rt_struct_path, rt_dose_data, index, rt_plan_path)
        except Exception as except_t:
            logging.warning(except_t)
            logging.warning("Skipping...")
            continue

        dvh_d = calc_dvh.bincenters.tolist()
        dvh_v = calc_dvh.counts.tolist()
        dvh_points = []

        for i in range(0, len(dvh_d)):
            dvh_points.append({
                "d_point": dvh_d[i],
                "v_point": dvh_v[i]
            })

        try:
            print(calc_dvh.V5)
            print(calc_dvh.D98)
            print(calc_dvh.V107)
            print(calc_dvh.V10)
            print(calc_dvh.D10)
            print(calc_dvh.volume, calc_dvh.volume_units)

            V5value = float(calc_dvh.V5.value)
            V10value = float(calc_dvh.V10.value)
            V20value = float(calc_dvh.V20.value)
            V30value = float(calc_dvh.V30.value)
            V40value = float(calc_dvh.V40.value)
            V50value = float(calc_dvh.V50.value)
            V60value = float(calc_dvh.V60.value)

        except Exception as e:
            logging.warning("Value not available exception =")
            logging.error(e)
            V5value = None
            V10value = None
            V20value = None
            V30value = None
            V40value = None
            V50value = None
            V60value = None

        id_data = "http://data.local/ldcm-rt/" + str(uuid4())
        try:
            structOut = {
                "@id": id_data,
                "structureName": structure["name"],
                "min": {"@id": f"{id_data}/min", "unit": "Gray", "value": calc_dvh.min},
                "mean": {"@id": f"{id_data}/mean", "unit": "Gray", "value": calc_dvh.mean},
                "max": {"@id": f"{id_data}/max", "unit": "Gray", "value": calc_dvh.max},
                "volume": {"@id": f"{id_data}/volume", "unit": "cc", "value": int(calc_dvh.volume)},
                "D10": {"@id": f"{id_data}/D10", "unit": "Gray", "value": float(calc_dvh.D10.value)},
                "D20": {"@id": f"{id_data}/D20", "unit": "Gray", "value": float(calc_dvh.D20.value)},
                "D30": {"@id": f"{id_data}/D30", "unit": "Gray", "value": float(calc_dvh.D30.value)},
                "D40": {"@id": f"{id_data}/D40", "unit": "Gray", "value": float(calc_dvh.D40.value)},
                "D50": {"@id": f"{id_data}/D50", "unit": "Gray", "value": float(calc_dvh.D50.value)},
                "D60": {"@id": f"{id_data}/D60", "unit": "Gray", "value": float(calc_dvh.D60.value)},
                "V5": {"@id": f"{id_data}/V5", "unit": "Gray", "value": V5value},
                "V10": {"@id": f"{id_data}/V10", "unit": "Gray", "value": V10value},
                "V20": {"@id": f"{id_data}/V20", "unit": "Gray", "value": V20value},
                "V30": {"@id": f"{id_data}/V5", "unit": "Gray", "value": V30value},
                "V40": {"@id": f"{id_data}/V10", "unit": "Gray", "value": V40value},
                "V50": {"@id": f"{id_data}/V20", "unit": "Gray", "value": V50value},
                "V60": {"@id": f"{id_data}/V20", "unit": "Gray", "value": V60value},
                "color": ','.join(str(e) for e in structure.get("color", np.array([])).tolist()),

                "dvh_curve": {
                    "@id": f"{id_data}/dvh_curve",
                    "dvh_points": dvh_points
                }
            }
        except Exception as e:
            logging.info("error")
            logging.warning(e)
            continue

        dvh_list.append(structOut)
    return dvh_list


def get_dvh_v(structure,
              dose_data,
              roi,
              rt_plan_p=None,
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
    dose_data : pydicom Dataset or filename
        DICOM RT Dose used to determine the dose grid.
    roi : int
        The ROI number used to uniquely identify the structure in the structure
        set.
    rt_plan_p : pydicom Dataset or filename
        DICOM RT plan path

    limit : int, optional
        Dose limit in cGy as a maximum bin for the histogram.
    calculate_full_volume : bool, optional
        Calculate the full structure volume including contours outside the
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

    # rtplan = rtplan.replace("/data/pre-act/mnt/", "/Volumes/research/Projects/cds/p0630-pre-act-dm/")
    rt_str = dicomparser.DicomParser(structure)
    if type(dose_data) is str:
        rt_dose = dicomparser.DicomParser(dose_data, memmap_pixel_array=memmap_rtdose)
    else:
        rt_dose = dose_data
    structures = rt_str.GetStructures()
    s = structures[roi]
    s['planes'] = rt_str.GetStructureCoordinates(roi)
    s['thickness'] = thickness if thickness else rt_str.CalculatePlaneThickness(
        s['planes'])

    calc_dvh = dvhcalc._calculate_dvh(s, rt_dose, limit, calculate_full_volume,
                                      use_structure_extents, interpolation_resolution,
                                      interpolation_segments_between_planes,
                                      callback)
    if rt_plan_p is not None:
        rt_plan = dicomparser.DicomParser(rt_plan_p)

        plan = rt_plan.GetPlan()
        if plan['rxdose'] is not None:

            return dvh.DVH(counts=calc_dvh.histogram,
                           bins=(np.arange(0, 2) if (calc_dvh.histogram.size == 1) else
                                 np.arange(0, calc_dvh.histogram.size + 1) / 100),
                           dvh_type='differential',
                           dose_units='Gy',
                           notes=calc_dvh.notes,
                           name=s['name'],
                           rx_dose=plan['rxdose'] / 100).cumulative
        else:
            return dvh.DVH(counts=calc_dvh.histogram,
                           bins=(np.arange(0, 2) if (calc_dvh.histogram.size == 1) else
                                 np.arange(0, calc_dvh.histogram.size + 1) / 100),
                           dvh_type='differential',
                           dose_units='Gy',
                           notes=calc_dvh.notes,
                           name=s['name']).cumulative
    else:
        return dvh.DVH(counts=calc_dvh.histogram,
                       bins=(np.arange(0, 2) if (calc_dvh.histogram.size == 1) else
                             np.arange(0, calc_dvh.histogram.size + 1) / 100),
                       dvh_type='differential',
                       dose_units='Gy',
                       notes=calc_dvh.notes,
                       name=s['name']).cumulative


class DVH_factory(ABC):
    """
    Starting point for the dvh calculation.
    Base on the arguments that you provide you will query the data from a ttl file or from Graph service.
    Tested only on GraphDB
    """

    def __init__(self, file_path, query, urls=None, ):
        """
        :param file_path:
        :param urls:
        """
        self.query = query
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


def check_dose_summ(dose_path):
    """
    :param dose_path:
    :return:
    """
    dose_to_sum = []
    for e in dose_path:
        data = pydicom.dcmread(e)
        dose_summ = data.DoseSummationType
        if dose_summ == "BEAM":
            dose_to_sum.append(e)
    return dose_to_sum


def dose_summation(dose_file0, dose_file1):
    """

    :param dose_file0:
    :param dose_file1:
    :return:
    """
    if type(dose_file0) is not dicompylercore.dose.DoseGrid:
        grid_1 = dose.DoseGrid(dose_file0)
    else:
        grid_1 = dose_file0
    grid_2 = dose.DoseGrid(dose_file1)
    grid_sum = grid_1 + grid_2
    return grid_sum


def dose_summation_process(dose_to_sum_list):
    """

    :param dose_to_sum_list: list of string path to the
    :return:
    """

    result_dose = dose_to_sum_list[0]
    data = pydicom.read_file(result_dose)

    dose_summ = data.DoseSummationType
    print(dose_summ)
    print(data[0x3004, 0x000E].value)
    print(data[0x0020, 0x0032])
    print(data[0x0020, 0x0037])
    print(data[0x0020, 0x000D])
    print(data[0x0028, 0x0030])
    print(data[0x3004, 0x000E])
    print(data[0x3004, 0x000C])
    for i in range(1, len(dose_to_sum_list)):
        data = pydicom.read_file(dose_to_sum_list[i])

        dose_summ = data.DoseSummationType
        print(dose_summ)
        print(data[0x3004, 0x000E].value)
        print(data[0x0020, 0x0032])
        print(data[0x0020, 0x0037])
        print(data[0x0020, 0x000D])
        print(data[0x0028, 0x0030])
        print(data[0x3004, 0x000E])
        print(data[0x3004, 0x000C])
        print('fin')

        result_dose = dose_summation(result_dose, dose_to_sum_list[i])
    return result_dose


def calculate_dvh_folder(rt_struct_path, *rt_dose_path, rt_plan_path=None, patient_id, folder_to_store_results):
    """


    :param rt_struct_path:
    :param rt_dose_path:
    :param rt_plan_path:
    :param patient_id:
    :param folder_to_store_results:
    :return:
    """
    if type(rt_dose_path) is not tuple or len(rt_dose_path) == 1:

        try:
            calculatedDose = get_dvh_for_structures(rt_struct_path, rt_dose_path,
                                                    rt_plan_path)
        except Exception as ex:
            print(ex)
            logging.warning(ex)
            logging.info("Error skipping")
            return
    else:
        list_to_sum = check_dose_summ(rt_dose_path)
        grid_sum = dose_summation_process(list_to_sum)
        calculatedDose = get_dvh_for_structures(rt_struct_path, grid_sum,
                                                rt_plan_path)

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
            "dvh_curve": {
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
        "PatientID": patient_id,
        "doseFraction": 0,
        "references": ["", ""],
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
        query = self.query

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

                calculatedDose = get_dvh_for_structures(dosePackage.rtStructPath, dosePackage.rtDosePath,
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
                    "V30": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/V30",
                        "@type": "@id"
                    },
                    "V40": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/V40",
                        "@type": "@id"
                    },
                    "V50": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/V50",
                        "@type": "@id"
                    },
                    "V60": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/V60",
                        "@type": "@id"
                    },
                    "dvh_points": {
                        "@id": "https://johanvansoest.nl/ontologies/LinkedDicom-dvh/dvh_point",
                        "@type": "@id"
                    },
                    "dvh_curve": {
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
