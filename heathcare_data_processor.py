import os

from pyspark.sql import SparkSession
from dotenv import load_dotenv
from claim_insurance_fact import ClaimInsuranceFact
from constants import APP_NAME, PHARMACY_DF, KEEP_DF, PRESCRIPTION_DF, CONTAIN_DF, CLAIM_DF, INSURANCE_PLAN_DF, \
    INSURANCE_COMPANY_DF, TREATMENT_DF, DISEASE_DF, PATIENT_DF
from healthcare_data_factory import getDatasets
from patient_treatment_fact import PatientTreatmentFact
from pharmacy_inventory_fact import PharmacyInventoryFact
from prescription_details_fact import PrescriptionDetailsFact
from application_logger import get_logger

load_dotenv()
logger = get_logger("heathcare_data_processor")


#  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.681 main.py --input 's3a://heathcare-emr-rasans-bucket/input' --output 's3a://heathcare-emr-rasans-bucket/output'
def process(inputPath, outputPath):
    logger.info("process has been started")
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_KEY')) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com") \
        .getOrCreate()

    datasets = getDatasets(spark, inputPath)

    pif = PharmacyInventoryFact(datasets[PHARMACY_DF], datasets[KEEP_DF])
    pif.create()
    pif.persist(outputPath)

    pdf = PrescriptionDetailsFact(datasets[PRESCRIPTION_DF], datasets[CONTAIN_DF])
    pdf.create()
    pdf.persist(outputPath)

    cif = ClaimInsuranceFact(datasets[CLAIM_DF], datasets[INSURANCE_PLAN_DF], datasets[INSURANCE_COMPANY_DF])
    cif.create()
    cif.persist(outputPath)

    ptf = PatientTreatmentFact(datasets[TREATMENT_DF], datasets[DISEASE_DF], datasets[PATIENT_DF])
    ptf.create()
    ptf.persist(outputPath)

    logger.info("process has been completed")
    spark.stop()
