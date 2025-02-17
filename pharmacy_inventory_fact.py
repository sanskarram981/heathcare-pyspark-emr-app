from pyspark.sql.functions import col


class PharmacyInventoryFact:
    def __init__(self, pharmacyDf, keepDf):
        self.pharmacyDf = pharmacyDf
        self.keepDf = keepDf
        self.sourceDf = None
        self.persistedDf = None

    def create(self):
        self.sourceDf = self.pharmacyDf.alias("df1").join(self.keepDf.alias("df2"),
                                                          col("df1.pharmacyID") == col("df2.pharmacyID"), "left") \
            .select(col("df1.pharmacyID"), col("df1.pharmacyName"), col("df1.phone"), col("df1.addressID"),
                    col("df2.medicineID"),
                    col("df2.quantity"), col("df2.discount"))

    def reconcile(self):
        pass

    def persist(self):
        self.sourceDf = self.sourceDf.coalesce(1)
        self.sourceDf.write.format("csv") \
            .option("header", True) \
            .option("delimiter", ",") \
            .mode("overwrite") \
            .save("datawarehouse/HealthcareTables/PharmacyInventoryFact")
