from abc import ABC, abstractmethod
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, LongType, DecimalType, DateType

from constants import ADDRESS_DF, CLAIM_DF, CONTAIN_DF, INSURANCE_COMPANY_DF, INSURANCE_PLAN_DF, KEEP_DF, MEDICINE_DF, \
    PATIENT_DF, PERSON_DF, PHARMACY_DF, PRESCRIPTION_DF, TREATMENT_DF, DISEASE_DF


class DatasetHandler(ABC):

    @abstractmethod
    def readSourceDataset(self):
        pass


class AddressHandler(DatasetHandler):
    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("addressID", IntegerType(), True),
                                  StructField("address1", StringType(), True),
                                  StructField("city", StringType(), True),
                                  StructField("state", StringType(), True),
                                  StructField("zip", IntegerType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


class ClaimHandler(DatasetHandler):
    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("claimID", LongType(), True),
                                  StructField("balance", IntegerType(), True),
                                  StructField("uin", StringType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


class ContainHandler(DatasetHandler):
    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("prescriptionID", LongType(), True),
                                  StructField("medicineID", IntegerType(), True),
                                  StructField("quantity", IntegerType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


class DiseaseHandler(DatasetHandler):
    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("diseaseID", IntegerType(), True),
                                  StructField("diseaseName", StringType(), True),
                                  StructField("description", StringType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


class InsuranceCompanyHandler(DatasetHandler):
    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("companyID", IntegerType(), True),
                                  StructField("companyName", StringType(), True),
                                  StructField("addressID", IntegerType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


class InsurancePlanHandler(DatasetHandler):
    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("uin", StringType(), True),
                                  StructField("planName", StringType(), True),
                                  StructField("companyID", IntegerType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


class KeepHandler(DatasetHandler):
    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("pharmacyID", IntegerType(), True),
                                  StructField("medicineID", IntegerType(), True),
                                  StructField("quantity", IntegerType(), True),
                                  StructField("discount", IntegerType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


class MedicineHandler(DatasetHandler):
    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("medicineID", IntegerType(), True),
                                  StructField("companyName", StringType(), True),
                                  StructField("productName", StringType(), True),
                                  StructField("description", StringType(), True),
                                  StructField("substanceName", StringType(), True),
                                  StructField("productType", IntegerType(), True),
                                  StructField("taxCriteria", StringType(), True),
                                  StructField("hospitalExclusive", StringType(), True),
                                  StructField("governmentDiscount", StringType(), True),
                                  StructField("taxImmunity", StringType(), True),
                                  StructField("maxPrice", DecimalType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


class PatientHandler(DatasetHandler):
    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("patientID", IntegerType(), True),
                                  StructField("ssn", LongType(), True),
                                  StructField("dob", DateType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


class PersonHandler(DatasetHandler):

    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("personID", IntegerType(), True),
                                  StructField("personName", StringType(), True),
                                  StructField("phoneNumber", LongType(), True),
                                  StructField("gender", StringType(), True),
                                  StructField("addressID", IntegerType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


class PharmacyHandler(DatasetHandler):
    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("pharmacyID", IntegerType(), True),
                                  StructField("pharmacyName", StringType(), True),
                                  StructField("phone", LongType(), True),
                                  StructField("addressID", IntegerType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


class PrescriptionHandler(DatasetHandler):
    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("prescriptionID", LongType(), True),
                                  StructField("pharmacyID", IntegerType(), True),
                                  StructField("treatmentID", IntegerType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


class TreatmentHandler(DatasetHandler):
    def __init__(self, sparkSession, filePath):
        self.sparkSession = sparkSession
        self.filePath = filePath
        self.df = None
        self.schema = StructType([StructField("treatmentID", IntegerType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("patientID", IntegerType(), True),
                                  StructField("diseaseID", IntegerType(), True),
                                  StructField("claimID", LongType(), True)])

    def readSourceDataset(self):
        self.df = self.sparkSession.read.format("csv") \
            .option("inferSchema", False) \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("mode", "PERMISSIVE") \
            .schema(self.schema) \
            .load(self.filePath)
        return self.df


def getDatasets(spark):

    addressDf = AddressHandler(spark, "datalake/HealthcareTables/Address/Address.csv").readSourceDataset()
    claimDf = ClaimHandler(spark, "datalake/HealthcareTables/Claim/Claim.csv").readSourceDataset()
    containDf = ContainHandler(spark, "datalake/HealthcareTables/Contain/Contain.csv").readSourceDataset()
    diseaseDf = DiseaseHandler(spark, "datalake/HealthcareTables/Disease/Disease.csv").readSourceDataset()
    insuranceCompanyDf = InsuranceCompanyHandler(spark,
                                                 "datalake/HealthcareTables/InsuranceCompany/InsuranceCompany.csv") \
        .readSourceDataset()
    insurancePlanDf = InsurancePlanHandler(spark, "datalake/HealthcareTables/InsurancePlan/InsurancePlan.csv") \
        .readSourceDataset()
    keepDf = KeepHandler(spark, "datalake/HealthcareTables/Keep/Keep.csv").readSourceDataset()
    medicineDf = MedicineHandler(spark, "datalake/HealthcareTables/Medicine/Medicine.csv").readSourceDataset()
    patientDf = PatientHandler(spark, "datalake/HealthcareTables/Patient/Patient.csv").readSourceDataset()
    personDf = PersonHandler(spark, "datalake/HealthcareTables/Person/Person.csv").readSourceDataset()
    pharmacyDf = PharmacyHandler(spark, "datalake/HealthcareTables/Pharmacy/Pharmacy.csv").readSourceDataset()
    prescriptionDf = PrescriptionHandler(spark,
                                         "datalake/HealthcareTables/Prescription/Prescription.csv").readSourceDataset()
    treatmentDf = TreatmentHandler(spark, "datalake/HealthcareTables/Treatment/Treatment.csv").readSourceDataset()

    datasets = {
        ADDRESS_DF: addressDf,
        CLAIM_DF: claimDf,
        CONTAIN_DF: containDf,
        INSURANCE_COMPANY_DF: insuranceCompanyDf,
        INSURANCE_PLAN_DF: insurancePlanDf,
        KEEP_DF: keepDf,
        MEDICINE_DF: medicineDf,
        PATIENT_DF: patientDf,
        PERSON_DF: personDf,
        PHARMACY_DF: pharmacyDf,
        PRESCRIPTION_DF: prescriptionDf,
        TREATMENT_DF: treatmentDf,
        DISEASE_DF: diseaseDf
    }

    return datasets



