from pyspark.sql import SparkSession

from handler import *

spark = SparkSession.builder \
    .appName("healthCareSparkApp") \
    .getOrCreate()

addressDf = AddressHandler(spark,"datalake/HealthcareTables/Address/Address.csv").readSourceDataset()

claimDf = ClaimHandler(spark,"datalake/HealthcareTables/Claim/Claim.csv").readSourceDataset()

containDf = ContainHandler(spark,"datalake/HealthcareTables/Contain/Contain.csv").readSourceDataset()

diseaseDf = DiseaseHandler(spark,"datalake/HealthcareTables/Disease/Disease.csv").readSourceDataset()
diseaseDf.show()

insuranceCompanyDf = InsuranceCompanyHandler(spark,"datalake/HealthcareTables/InsuranceCompany/InsuranceCompany.csv") \
                           .readSourceDataset()
insuranceCompanyDf.show()

insurancePlanDf = InsurancePlanHandler(spark,"datalake/HealthcareTables/InsurancePlan/InsurancePlan.csv")\
                           .readSourceDataset()
insurancePlanDf.show()

keepDf = KeepHandler(spark,"datalake/HealthcareTables/Keep/Keep.csv").readSourceDataset()
keepDf.show()

medicineDf = MedicineHandler(spark,"datalake/HealthcareTables/Medicine/Medicine.csv").readSourceDataset()
medicineDf.show()

patientDf = PatientHandler(spark,"datalake/HealthcareTables/Patient/Patient.csv").readSourceDataset()
patientDf.show()

personDf = PersonHandler(spark, "datalake/HealthcareTables/Person/Person.csv").readSourceDataset()
personDf.show()

pharmacyDf = PharmacyHandler(spark,"datalake/HealthcareTables/Pharmacy/Pharmacy.csv").readSourceDataset()
pharmacyDf.show()

prescriptionDf = PrescriptionHandler(spark,"datalake/HealthcareTables/Prescription/Prescription.csv").readSourceDataset()
prescriptionDf.show()

treatmentDf = TreatmentHandler(spark,"datalake/HealthcareTables/Treatment/Treatment.csv").readSourceDataset()
treatmentDf.show()

spark.stop()
