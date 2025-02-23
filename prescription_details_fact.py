from pyspark.sql.functions import col


class PrescriptionDetailsFact:
    def __init__(self, prescriptionDf, containDf):
        self.prescriptionDf = prescriptionDf
        self.containDf = containDf
        self.sourceDf = None
        self.persistedDf = None

    def create(self):
        self.sourceDf = self.prescriptionDf.alias("df1").join(self.containDf.alias("df2"),
                                                              col("df1.prescriptionID") == col("df2.prescriptionID"),
                                                              "inner").select(col("df1.prescriptionID"),
                                                                              col("df1.pharmacyID"),
                                                                              col("df1.treatmentID"),
                                                                              col("df2.medicineID"),
                                                                              col("df2.quantity"))

    def reconcile(self):
        pass

    def persist(self,outputPath):
        self.sourceDf = self.sourceDf.coalesce(1)
        self.sourceDf.write.format("csv") \
            .option("header", True) \
            .option("delimiter", ",") \
            .mode("overwrite") \
            .save("{}/HealthcareTables/PrescriptionDetailsFact".format(outputPath))
