from pyspark.sql.functions import col


class ClaimInsuranceFact:
    def __init__(self, claimDf, insurancePlanDf, insuranceCompanyDf):
        self.claimDf = claimDf
        self.insurancePlanDf = insurancePlanDf
        self.insuranceCompanyDf = insuranceCompanyDf
        self.sourceDf = None
        self.persistedDf = None

    def create(self):
        self.sourceDf = self.claimDf.alias("df1").join(self.insurancePlanDf.alias("df2"),
                                                       col("df1.uin") == col("df2.uin"),"inner") \
            .join(self.insuranceCompanyDf.alias("df3"), col("df2.companyID") == col("df3.companyID"),"inner") \
            .select(col("df1.claimID"), col("df1.balance"), col("df1.uin"), col("df2.planName"), col("df2.companyID"),
                   col("df3.companyName"), col("df3.addressID"))

    def reconcile(self):
        pass

    def persist(self):
        self.sourceDf = self.sourceDf.coalesce(1)
        self.sourceDf.write.format("csv") \
            .option("header", True) \
            .option("delimiter", ",") \
            .mode("overwrite") \
            .save("datawarehouse/HealthcareTables/ClaimInsuranceFact")
