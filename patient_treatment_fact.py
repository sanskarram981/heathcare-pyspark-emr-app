from pyspark.sql.functions import col


class PatientTreatmentFact:
    def __init__(self, treatmentDf, diseaseDf, patientDf):
        self.patientDf = patientDf
        self.treatmentDf = treatmentDf
        self.diseaseDf = diseaseDf
        self.sourceDf = None
        self.persistedDf = None

    def create(self):
        self.sourceDf = self.treatmentDf.alias("df1").join(self.diseaseDf.alias("df2"),
                                                           col("df1.diseaseID") == col("df2.diseaseID"), "left") \
            .join(self.patientDf.alias("df3"), col("df1.patientID") == col("df3.patientID"), "inner") \
            .select(col("df1.treatmentID"), col("df1.date"), col("df1.patientID"), col("df1.diseaseID"),
                    col("df1.claimID"),
                    col("df2.diseaseName"), col("df2.description"), col("df3.ssn"), col("df3.dob"))

    def reconcile(self):
        pass

    def persist(self,outputPath):
        self.sourceDf = self.sourceDf.coalesce(1)
        self.sourceDf.write.format("csv") \
            .option("header", True) \
            .option("delimiter", ",") \
            .mode("overwrite") \
            .save("{}/HealthcareTables/PatientTreatmentFact".format(outputPath))
