# ===================================================================
# Configuration
# ===================================================================
segment_list = [10000, 11000, 11100, 11300, 12000, 11200, 11400, 11900, 
                50053, 50063, 50065, 80053, 80063, 80065]

#segment_list = (
#    spark.read.table("mpe.mapping_segmen_baru2")
#    .filter(col("produk").isin("Kupedes", "Briguna-Mikro", "KUR-Mikro", "KUR-Small"))
#    .select("fksegmen1")
#    .distinct()  # Add this if you want unique values
#    .toPandas()["fksegmen1"]
#    .tolist()
#)

# ===================================================================
# Product Mapping Functions
# ===================================================================
def segmen():
      return F.when(col("fksegmen").isin(50053, 50063, 50065, 80053, 80063, 80065), "Small")\
              .when(col("fksegmen").isin(10000, 11000, 11100, 11300, 12000, 11200, 11400, 11900), "Micro")\
              .otherwise(col("fksegmen").cast("string")) \
              .alias("segmen")

def get_product_column():
    return F.when(F.col("fksegmen").isin(10000, 11000, 11100, 11300), "Kupedes") \
            .when(F.col("fksegmen") == 12000, "Briguna-Mikro") \
            .when(F.col("fksegmen").isin(11200, 11400, 11900), "KUR-Mikro") \
            .when(F.col("fksegmen").isin(50053, 50063, 50065, 80053, 80063, 80065), "KUR-Small") \
            .otherwise(F.col("fksegmen").cast("string")) \
            .alias("produk")

def get_sub_product():
    return F.when(
              (F.col("produk") == "Kupedes") & (F.col("loan_type").isin("1B", "1D", "3B", "3D")),
              "KECE"
          ).when(
              (F.col("produk") == "Kupedes") & (F.col("loan_type").isin(
                  "1K", "1L", "3Z", "S1", "S2", "S3", "S4", "S5", "S6", "S9",
                  "T1", "T2", "T3", "T4", "T5", "4U", "1R", "3O", "4V", "3Q", "3R", "3S", "3T"
              )),
              "KUPRA"
          ).when(
              (F.col("produk").like("%KUR%")) & (F.col("loan_type").isin(
                  "2F", "2G", "2H", "2I", "2J", "2K", "2T", "2U", "4O", "4W",
                  "4Q", "4R", "4S", "4T"
              )),
              "KUR SUPER MIKRO"
          ).otherwise(F.col("produk")).alias("SUBPRODUK")
def tiering(x):
     return F.when(col(x) <= 10000000, lit("sd10jt"))\
          .when(col(x) <= 25000000, lit("10sd25jt"))\
          .when(col(x) <= 50000000, lit("25sd50jt"))\
          .when(col(x) <= 100000000, lit("50sd100jt"))\
          .when(col(x) <= 250000000, lit("100sd250jt"))\
          .when(col(x) <= 500000000, lit("250sd500jt"))\
          .otherwise(lit(">500jt")).alias("tiering")
  
def get_strata_plafond(x, y):
    ratio = F.col(x) / F.col(y)

    return (
        F.when(F.col(y) <= 0, F.lit(None))
         .when(ratio <= 1.0, F.lit("<=100%"))
         .when(ratio <= 1.1, F.lit("<=110%"))
         .when(ratio <= 1.2, F.lit("<=120%"))
         .when(ratio <= 1.3, F.lit("<=130%"))
         .when(ratio <= 1.4, F.lit("<=140%"))
         .when(ratio <= 1.5, F.lit("<=150%"))
         .when(ratio <= 1.6, F.lit("<=160%"))
         .when(ratio <= 1.7, F.lit("<=170%"))
         .when(ratio <= 1.8, F.lit("<=180%"))
         .when(ratio <= 1.9, F.lit("<=190%"))
         .when(ratio <= 2.0, F.lit("<=200%"))
         .otherwise(">200%")
    )
  
def get_strata_os(x, y):
    ratio = F.col(x) / F.col(y)

    return (
        F.when(F.col(y) <= 0, F.lit(None))
         .when(ratio <= 0.1, F.lit("<=10%"))
         .when(ratio <= 0.2, F.lit("<=20%"))
         .when(ratio <= 0.3, F.lit("<=30%"))
         .when(ratio <= 0.4, F.lit("<=40%"))
         .when(ratio <= 0.5, F.lit("<=50%"))
         .when(ratio <= 0.6, F.lit("<=60%"))
         .when(ratio <= 0.7, F.lit("<=70%"))
         .when(ratio <= 0.8, F.lit("<=80%"))
         .when(ratio <= 0.9, F.lit("<=90%"))
         .when(ratio <= 1.0, F.lit("<=100%"))
         .otherwise(">100%")
    )

# ============================================================
# DATA FILTER PARTITIONS
# ============================================================
partition = spark.sql("SHOW PARTITIONS datalake.2021_cras_datamart_pinjaman").collect()

# Extract ds values and filter out default partitions
ds_values = [
    p.partition.split('=')[1] 
    for p in partition 
    if '__HIVE_DEFAULT_PARTITION__' not in p.partition
]

# Find max
mds = max(ds_values)
#ds_list = ["20251231"]
#ds_list = ["20250131","20250228","20250331",
#    "20250430","20250531","20250630",
#    "20250731","20250831","20250930",
#    "20251031","20251130"]
#
#
#for mds in ds_list:
mds_date = datetime.strptime(mds, '%Y%m%d')

# Get first day of current month, subtract 1 day to get last day of previous month
first_day_current_month = mds_date.replace(day=1)
lds_date = first_day_current_month - relativedelta(days=1)

lds = lds_date.strftime('%Y%m%d')
print(f"mds: {mds}, lds: {lds}")

dsa = mds[:6]
dsb = lds[:6]

print(f"dsa: {dsa} dsb:{dsb}")
# ===================================================================
# STEP 1: Read Loan Data (Only Realizations)
# ===================================================================
df_customer = spark.table("datalake.6969_crm_edw_fact_dly_customer_info")\
                   .select(
                       F.upper(F.trim(F.col("cfcif#"))).alias("cifno"), 
                       F.trim(F.col("id_number")).alias("id_number")
                   )\
                   .distinct()

df_keluarga = spark.table("mpe.mapping_keluarga_debitur")\
                   .select(
                       F.trim(F.col("no_kk")).alias("no_kk"),
                       F.md5(F.trim(F.col("nik"))).alias("id_number")
                   )\
                   .distinct()


df_loan = spark.table("mpe.mrm_cras_master_dibrusment")\
               .filter((F.col("ds") == dsa)&
                        (F.col("flag_restruk") !="Y")& (F.col("kol_adk") == 1), 
                   )\
               .select(
                   F.upper(F.trim(F.col("cifno"))).alias("cifno")                
               )\
               .distinct()
df_info = df_loan.join(df_customer, "cifno", "left")


#print(f"STEP 1 - Base CIFNOs: {df_loan.count():,}")

window_spec = Window.partitionBy("cifno").orderBy(
    F.when(F.col("no_kk").isNull(), 1).otherwise(0),  # NULL last
    F.col("no_kk")  # Then alphabetically/numerically
)

df_loan_info = df_info.join(df_keluarga, "id_number", "left").withColumn("rank", F.row_number().over(window_spec))\
    .filter(F.col("rank") == 1)\
    .drop("rank")

#print(f"STEP 1.1 - Base CIFNOs: {df_loan_info.count():,}")

#df_loan = df_loan_info.join(df_keluarga, "id_number", "left")

df_loan = df_loan_info

#hiveDB = "temp"
#hiveTable = "last_balance_suplesi_check"
#df_loan_info\
#  .write.format("parquet") \
#  .mode("overwrite") \
#  .saveAsTable("{}.{}".format(hiveDB, hiveTable))

# ===================================================================
# STEP 2: Get Previous Loan Info (FILTERED by Current Month CIFNOs)
# ===================================================================
df_historical = spark.table("datalake.8989_bisdwh_fact_loanmaster")\
    .filter(
        (F.col("ds").cast("int").between(201501, 202112)) & 
        (F.col("fksegmen").isin(segment_list)) &
        ((F.year("freldt_dt") * 100 + F.month("freldt_dt")) == F.col("ds").cast("int"))
    )\
    .select(
        F.col("freldt_dt").alias("real_date"),
        F.col("cifno"),
        F.col("acctno"),
        F.col("orgamt_base").alias("plafond"),
        F.col("type").alias("loan_type"),
        F.col("fksegmen"),
        F.col("ds")
    )
#print(f"Historical records (2015-2020): {df_historical.count():,}")

df_recent = spark.table("datalake.2021_cras_datamart_pinjaman")\
    .filter(
        (F.col("ds").between("20220131", mds)) &
        (F.col("fksegmen").isin(segment_list)) &
        (F.substring(F.col("ds").cast("string"), 1, 6) == 
         F.date_format(F.to_date(F.col("tgl_realisasi"), "dd-MM-yyyy"), "yyyyMM"))
    )\
    .select(
        F.to_timestamp(F.col("tgl_realisasi"), "dd-MM-yyyy").alias("real_date"),
        F.col("cifno"),
        F.col("acctno"),
        F.col("plafond"),
        F.col("loan_type"),
        F.col("fksegmen"),
        F.col("ds")
    )

#print(f"Recent records (2021-present): {df_recent.count():,}")

#print("Combining historical and recent data...")

df_all_loans_history = df_historical.unionByName(df_recent)

#print(f"STEP 2A - Total combined records: {df_all_loans_history.count():,}")

df_all_loans_history = df_all_loans_history.join(
    df_loan, 
    on="cifno",
    how="inner"
).select(
   "real_date",
    "no_kk",
    F.col("cifno"),
    F.col("acctno"),
    F.col("plafond"),
    F.col("loan_type"),
    F.col("fksegmen"),
    get_product_column()
).select(
    "real_date",
    "no_kk",
    "cifno",
    "acctno",
    "plafond",
    tiering("plafond"),
    segmen(),
    get_sub_product()
)

#print(f"STEP 2B - History after join: {df_all_loans_history.count():,}")

df_all_loans_history = df_all_loans_history\
  .withColumn("group_key", 
      F.when(F.col("no_kk").isNotNull(), F.col("no_kk"))
       .otherwise(F.col("cifno"))
  )
# Apply window function using the group_key
window_spec = Window.partitionBy("group_key").orderBy("real_date")

df_all_with_prev = df_all_loans_history \
    .withColumn("realisasi_ke", F.row_number().over(window_spec))\
    .withColumn("real_before", F.lag("real_date", 1).over(window_spec))\
    .withColumn("acctno_before", F.lag("acctno", 1).over(window_spec)) \
    .withColumn("prod_before", F.lag("SUBPRODUK", 1).over(window_spec)) \
    .withColumn("plafond_before", F.lag("plafond", 1).over(window_spec))\
    .withColumn("tiering_before", F.lag("tiering", 1).over(window_spec))

#print(f"STEP 2C - With prev loan info: {df_all_with_prev.count():,}")

df_with_prev = df_all_with_prev.filter(
    F.date_format(F.col("real_date"), "yyyyMM") == F.lit(dsa)
)

#print(f"STEP 2C - With prev loan info: {df_with_prev.count():,}")
df_lunas = (spark.table("datalake.`8989_bisdwh_fact_loanmaster`")
    .filter(
        (F.col("ds").between("201501", dsa)) &
        (F.col("status") == 2) &
        (F.col("ds") == F.concat(
            F.year(F.col("stsdt_dt") + F.expr("INTERVAL 7 HOURS")).cast("string"),
            F.lpad(
                F.month(F.col("stsdt_dt") + F.expr("INTERVAL 7 HOURS")).cast("string"),
                2,
                "0"
            )
        ))
    )
    .select(col("ds").alias("ds_lunas"), col("cifno").alias("cifno_lunas"), col("acctno").alias("acctno_lunas"), col("cbal_base").alias("os_lunas"))

)
df_lunas_check = df_with_prev.join(
    df_lunas, 
    (df_with_prev.cifno == df_lunas.cifno_lunas) & 
    (df_with_prev.acctno_before == df_lunas.acctno_lunas), 
    "left"
)
window_spec_l = Window.partitionBy("acctno").orderBy(F.col("ds_lunas").desc())
df_fil_lunas = df_lunas_check.withColumn("rank", F.row_number().over(window_spec_l))\
                              .filter(F.col("rank") == 1).drop("rank")
#print(f"STEP 2D - Current month only: {df_lunas_check.count():,}")

hiveDB = "temp"
hiveTable = "last_balance_suplesi_temp"
df_fil_lunas\
  .write.format("parquet") \
  .mode("overwrite") \
  .saveAsTable("{}.{}".format(hiveDB, hiveTable))

# ===================================================================
# STEP 3: Get Only Accounts and DS Needed for Balance Lookup
# ===================================================================

df_sup = spark.read.table("temp.last_balance_suplesi_temp")
#print(f"STEP 3A - Read from temp table: {df_sup.count():,}")

accounts_needed = df_sup \
  .filter(F.col("acctno_before").isNotNull()) \
  .select(
      F.col("cifno").alias("cfino_lookup"),
      F.col("acctno_before").alias("acctno_lookup")
  ) \
  .distinct()

#print(f"STEP 3B - Accounts needed for lookup: {accounts_needed.count():,}")

# ===================================================================
# STEP 4: Read Balance Data ONLY for Required Accounts & Dates
# ===================================================================
df_balance = spark.table("datalake.2021_cras_datamart_pinjaman")

df_balance_filtered = df_balance \
  .filter(
      (F.col("ds").isin(lds, mds))&
      (F.col("fksegmen").isin(segment_list))
  ).withColumn("ds_bfr_real", F.substring(F.col("ds"), 1, 6))

#print(f"STEP 4A - Balance before join: {df_balance_filtered.count():,}")

df_balance_filtered = df_balance_filtered \
  .join(
      accounts_needed,
      (F.col("acctno") == F.col("acctno_lookup")) &
      (F.col("cifno") == F.col("cfino_lookup")),
      "inner"
  ) \
  .select(
      F.col("cifno"),
      F.col("acctno"),
      F.col("cbal_base"),
      F.col("tgl_realisasi").alias("real_date_bfr"),
      "ds_bfr_real"
  ) \
  .select(
      F.col("cifno").alias("cifno_bal"),
      F.col("acctno").alias("acctno_bal"),
      F.col("cbal_base"),
      "real_date_bfr",
      "ds_bfr_real"
  )

#print(f"STEP 4B - Balance after join: {df_balance_filtered.count():,}")

window_balance = Window.partitionBy("cifno_bal", "acctno_bal").orderBy(F.desc("ds_bfr_real"))

df_balance_filtered = df_balance_filtered \
  .withColumn("rn", F.row_number().over(window_balance)) \
  .filter(F.col("rn") == 1) \
  .drop("rn").select(
      F.col("cifno_bal").alias("cifno_bal"),
      F.col("acctno_bal").alias("acctno_bal"),
      F.col("cbal_base"),
      "real_date_bfr", "ds_bfr_real"
  )

#print(f"STEP 4C - Balance after dedup: {df_balance_filtered.count():,}")

# ===================================================================
# STEP 5: Final Join to Get Complete Dataset
# ===================================================================
df_final = df_sup \
  .join(
      df_balance_filtered,
      (df_sup.acctno_before == df_balance_filtered.acctno_bal) &
      (df_sup.cifno == df_balance_filtered.cifno_bal),
      how="left"
  )


#print(f"STEP 5A - After balance join: {df_final.count():,}")
#print(f"CIFNOs with multiple accounts: {df_final.groupBy('cifno').count().filter(F.col('count') > 1).count():,}")

df_final = df_final.select(
      F.col("cifno"),
      F.col("real_date"),
      F.col("realisasi_ke"),
      F.col("acctno"),
      F.col("SUBPRODUK").alias("prod_now"), 
      F.col("plafond").alias("plafond_now"),
      F.col("tiering"),
      F.col("real_before"),
      F.coalesce(F.col("ds_bfr_real"), F.col("ds_lunas")).alias("ds_bfr_real"),
      F.col("acctno_before"),
      F.col("prod_before"),
      F.coalesce(F.col("cbal_base"), F.col("os_lunas"), F.lit(0)).alias("cbal_bfr_real"),
      F.col("plafond_before"),
      F.col("tiering_before")
  )\
  .withColumn("strata_plafond", get_strata_plafond("plafond_now","plafond_before"))\
  .withColumn("strata_os", get_strata_os("cbal_bfr_real","plafond_before"))


#hiveDB = "temp"
#hiveTable = "last_balance_suplesi_check"
#df_final\
#  .write.format("parquet") \
#  .mode("overwrite") \
#  .saveAsTable("{}.{}".format(hiveDB, hiveTable))
#print(f"STEP 5B - After strata columns: {df_final.count():,}")

window_spec = Window.partitionBy("cifno","acctno").orderBy("ds_bfr_real")

# Define product tier hierarchy (lower number = lower tier)
product_tiers = {
    "KUR-Mikro": 1,
    "KUR SUPER MIKRO": 2,
    "Kupedes": 3,
    "KUPRA": 4,
    "KECE": 5,
    "Briguna-Mikro": 6,
    "KUR-Small": 7
}

# Create mapping expression for product tiers
tier_mapping = F.create_map([F.lit(x) for pair in product_tiers.items() for x in pair])

df_final = (
    df_final
    .withColumn("ranking", F.row_number().over(window_spec))
    .filter(F.col("ranking") == 1)
    .drop("ranking")
    .withColumn("ds", F.lit(dsa))
    .withColumn("status_debitur", 
    F.when(F.col("realisasi_ke") == 1, "debitur_baru")
     .otherwise("debitur_lama")
)\
.withColumn("tier_before", tier_mapping[F.col("prod_before")])\
.withColumn("tier_now", tier_mapping[F.col("prod_now")])\
.withColumn("status_suplesi",
    F.when(
        (F.col("status_debitur") == "debitur_lama") & 
        (F.months_between(
            F.to_date(F.col("ds"), "yyyyMM"),
            F.to_date(F.col("ds_bfr_real"), "yyyyMM")
        ) > 6),
        "kawan_lama"
    )
    .when(
        (F.col("status_debitur") == "debitur_lama") &
        (F.months_between(
            F.to_date(F.col("ds"), "yyyyMM"),
            F.to_date(F.col("ds_bfr_real"), "yyyyMM")
        ) <= 6),
        "kawan_dekat"
    )
     .when(
        (F.col("status_debitur") == "debitur_lama"),
        "debitur_lama"
    )
    .otherwise("debitur_baru")
)
)\
.withColumn("plafond_nett",
        F.when(
            (F.col("status_suplesi") == "kawan_dekat") | (F.col("cbal_bfr_real") != 0),
            F.col("plafond_now") - F.col("cbal_bfr_real")
        )
        .otherwise(F.col("plafond_now"))
    ).drop("tier_now", "tier_before")

#print(f"STEP 5C - FINAL OUTPUT: {df_final.count():,}")
#print(f"FINAL - Distinct CIFNOs: {df_final.select('cifno').distinct().count():,}")
#print(f"FINAL - Distinct ACCTNOs: {df_final.select('acctno').distinct().count():,}")

hiveDB = "mpe"
hiveTable = "mrm_data_suplesi"
spark.sql(f"ALTER TABLE {hiveDB}.{hiveTable} DROP IF EXISTS PARTITION (ds='{dsa}')")


df_final \
      .write \
      .format("parquet") \
      .mode("append") \
      .partitionBy("ds") \
      .saveAsTable(f"{hiveDB}.{hiveTable}")

df_loan.unpersist()
accounts_needed.unpersist()
print(f"Done DS: {dsa}")
