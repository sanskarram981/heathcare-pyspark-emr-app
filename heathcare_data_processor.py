from pyspark.sql import SparkSession

from claim_insurance_fact import ClaimInsuranceFact
from constants import APP_NAME, PHARMACY_DF, KEEP_DF, PRESCRIPTION_DF, CONTAIN_DF, CLAIM_DF, INSURANCE_PLAN_DF, \
    INSURANCE_COMPANY_DF, TREATMENT_DF, DISEASE_DF, PATIENT_DF
from healthcare_data_factory import getDatasets
from patient_treatment_fact import PatientTreatmentFact
from pharmacy_inventory_fact import PharmacyInventoryFact
from prescription_details_fact import PrescriptionDetailsFact


def process(inputPath, outputPath):
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()

    datasets = getDatasets(spark)

    pif = PharmacyInventoryFact(datasets[PHARMACY_DF], datasets[KEEP_DF])
    pif.create()
    pif.persist()

    pdf = PrescriptionDetailsFact(datasets[PRESCRIPTION_DF], datasets[CONTAIN_DF])
    pdf.create()
    pdf.persist()

    cif = ClaimInsuranceFact(datasets[CLAIM_DF], datasets[INSURANCE_PLAN_DF], datasets[INSURANCE_COMPANY_DF])
    cif.create()
    cif.persist()

    ptf = PatientTreatmentFact(datasets[TREATMENT_DF], datasets[DISEASE_DF], datasets[PATIENT_DF])
    ptf.create()
    ptf.persist()

    spark.stop()
