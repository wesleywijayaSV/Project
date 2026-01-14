partition = spark.sql("SHOW PARTITIONS datalake.2021_cras_datamart_pinjaman").collect()

# Extract ds values and filter out default partitions
ds_values = [
    p.partition.split('=')[1] 
    for p in partition 
    if '__HIVE_DEFAULT_PARTITION__' not in p.partition
]

# Find max
mds = max(ds_values)
mds_date = datetime.strptime(mds, '%Y%m%d')
lds_date = mds_date - relativedelta(months=1)
lds = lds_date.strftime('%Y%m%d')
ds = mds[:6]
start_month = "201901"
print(f"start: {start_month} mds : {mds} lds : {lds} ds: {ds}")


segmen_list = [10000, 11000, 11100, 11300, 12000, 
               11200, 11400, 11900,50053, 50063, 50065, 80053, 80063, 80065]

# ========================================
# 1️⃣ ACCOUNT MAPPING (Load once, outside loop)
# ========================================
acctno_mapping = spark.read.table("datamart.cras_vintage_acctno_tob_mapping").select(
    F.col("olacct").alias("old_acctno"),
    F.col("nwacct").alias("new_acctno")
)

# ========================================
# 5️⃣ SEVERITY MAPPING & CONVERSION (Define once)
# ========================================
severity_map = {
    "Lancar": 1,
    "Lancar Restruk": 2,
    "SML1": 3,
    "SML2": 4,
    "SML3": 5,
    "KL": 6,
    "D1": 7,
    "D2": 8,
    "M": 9
}
reverse_map = {v: k for k, v in severity_map.items()}

@F.udf("int")
def severity_lookup(k):
    return severity_map.get(k, None)

@F.udf("string")
def kualitas_lookup(sev):
    return reverse_map.get(sev, None)



# ========================================
# MAIN LOOP - PROCESS EACH PRODUCT
# ========================================
#for fksegmen_list, product_name in product_fksegmen_map:
#    print("=" * 80)
#    print(f"Processing product: {product_name}")
#    print(f"fksegmen: {fksegmen_list}")
#    print("=" * 80)
#    
# ========================================
# 2️⃣ GET ACCOUNT METADATA
# ========================================
df_account_loan = (
    spark.read.table("datalake.`8989_bisdwh_fact_loanmaster`")
    .filter(F.col("ds").between(start_month[:6], "202112"))
    .filter(~F.col("status").isin(2, 4, 8))
    .filter(F.col("fksegmen").isin(segmen_list))
    .join(acctno_mapping, F.col("acctno") == F.col("old_acctno"), "left")
    .withColumn("new_acctno", F.coalesce(F.col("new_acctno"), F.col("acctno")))
    .drop("old_acctno", "acctno")

    # Extract realization date info
    .withColumn("tahun_realisasi", F.year(F.col("freldt_dt")))
    .withColumn("bulan_realisasi", F.month(F.col("freldt_dt")))
    .withColumn(
        "tanggal_realisasi",
        F.concat(
            F.lpad(F.col("tahun_realisasi").cast("string"), 4, "0"),
            F.lpad(F.col("bulan_realisasi").cast("string"), 2, "0")
        )
    )

    # Map segmen
    .withColumn(
        "segmen",
        F.when(F.col("fksegmen").isin(10000, 11000, 11100, 11200, 11300, 11400, 11900, 12000, 13000), "Micro")
         .when(F.col("fksegmen").isin(50053, 50063, 50065, 80053, 80063, 80065), "Small")
        .otherwise("Unknown")
    )

    .withColumn(
        "produk",
        F.when(F.col("fksegmen").isin(10000, 11000, 11100, 11300), "Kupedes")
        .when(F.col("fksegmen") == 12000, "Briguna-Mikro")
        .when(F.col("fksegmen").isin(11200, 11400, 11900), "KUR-Mikro")
        .when(F.col("fksegmen").isin(50053, 50063, 50065, 80053, 80063, 80065), "KUR-Small")
        .otherwise(F.col("fksegmen").cast("string"))
    )
    .select("cifno", "new_acctno", "tanggal_realisasi", "tahun_realisasi", 
            "bulan_realisasi", "segmen", "produk")
    .distinct()
)


df_account_cras = (
    spark.read.table("datalake.2021_cras_datamart_pinjaman")
    .filter(F.col("ds").between("20220131", mds))
    .filter(F.col("fksegmen").isin(segmen_list))
    .join(acctno_mapping, F.col("acctno") == F.col("old_acctno"), "left")
    .withColumn("new_acctno", F.coalesce(F.col("new_acctno"), F.col("acctno")))
    .drop("old_acctno", "acctno")
    # Extract realization date info
    .withColumn(
        "tanggal_realisasi_parsed", 
        F.to_date(F.col("tgl_realisasi"), "dd-MM-yyyy")
    )
    .withColumn(
        "tahun_realisasi", 
        F.year(F.col("tanggal_realisasi_parsed"))
    )
    .withColumn(
        "bulan_realisasi", 
        F.month(F.col("tanggal_realisasi_parsed"))
    )
    .withColumn(
        "tanggal_realisasi_formatted",
        F.concat(
            F.lpad(F.col("tahun_realisasi").cast("string"), 4, "0"),
            F.lpad(F.col("bulan_realisasi").cast("string"), 2, "0")
        )
    )
    # Map segmen
    .withColumn(
        "segmen",
        F.when(F.col("fksegmen").isin(10000, 11000, 11100, 11200, 11300, 11400, 11900, 12000, 13000), "Micro")
         .when(F.col("fksegmen").isin(50053, 50063, 50065, 80053, 80063, 80065), "Small")
        .otherwise("Unknown")
    )
    .withColumn(
        "produk",
        F.when(F.col("fksegmen").isin(10000, 11000, 11100, 11300), "Kupedes")
        .when(F.col("fksegmen") == 12000, "Briguna-Mikro")
        .when(F.col("fksegmen").isin(11200, 11400, 11900), "KUR-Mikro")
        .when(F.col("fksegmen").isin(50053, 50063, 50065, 80053, 80063, 80065), "KUR-Small")
        .otherwise(F.col("fksegmen").cast("string"))
    )
    .select(
        "cifno", 
        "new_acctno", 
        F.col("tanggal_realisasi_formatted").alias("tanggal_realisasi"),  # Changed this line
        "tahun_realisasi", 
        "bulan_realisasi", 
        "segmen", 
        "produk"
    )
    .distinct()
)



df_account_meta = df_account_cras.union(df_account_loan)\
    .filter(F.col("tahun_realisasi") >= 2019)\
    .filter(F.col("tanggal_realisasi") <= ds).distinct()

#print(f"Account metadata rows: {df_account_meta.count()}")

# ========================================
# 3️⃣ GET ACTUAL LOAN DATA (ACTUAL DATA ONLY)
# ========================================

df_loan_actual_cras = (
    spark.read.table("datalake.`2021_cras_datamart_pinjaman`")
    .filter((F.col("ds") >= "20220131") & (F.col("ds") <= mds))
    .join(acctno_mapping, F.col("acctno") == F.col("old_acctno"), "left")
    .withColumn("new_acctno", F.coalesce(F.col("new_acctno"), F.col("acctno")))
    .drop("old_acctno", "acctno")
    .select(
        "cifno",
        "new_acctno",
        F.substring(F.col("ds").cast("string"), 1, 6).alias("ds"),
        "kol_adk",
        F.col("restruk").alias("flag_restruk"),
        "cbal_base",
        "jumlah_tunggakan_hari"
    )
)

# Loan master data (2019-2021)
df_loan_actual_master = (
    spark.read.table("datalake.`8989_bisdwh_fact_loanmaster`")
    .filter(F.col("ds").between(start_month[:6], "202112"))
    .filter(~F.col("status").isin(2, 4, 8))
    .join(acctno_mapping, F.col("acctno") == F.col("old_acctno"), "left")
    .withColumn("new_acctno", F.coalesce(F.col("new_acctno"), F.col("acctno")))
    .drop("old_acctno", "acctno")
    .withColumn(
        "jumlah_tunggakan_hari",
        F.datediff(
            F.to_date(F.col("ds"), "yyyyMM"),
            F.expr(
                "date_add(to_date(concat(substring(cast(npdt as string), 1, 4), '-01-01'), 'yyyy-MM-dd'), cast(substring(cast(npdt as string), 5) as int) - 1)"
            )
        )
    )
    .select(
        "cifno",
        "new_acctno",
        F.col("ds"),  # Already in YYYYMM format
        "kol_adk",
        F.col("lrflag").alias("flag_restruk"),
        "cbal_base",
        "jumlah_tunggakan_hari"
    )
)

# Union both sources
df_loan_actual = df_loan_actual_cras.union(df_loan_actual_master).distinct()

# ========================================
# 4️⃣ JOIN WITH METADATA & CALCULATE MOB
# ========================================
df_base = (
    df_loan_actual
    .join(df_account_meta, ["cifno", "new_acctno"], "inner")
    .filter(F.col("ds") >= F.col("tanggal_realisasi"))

    # Calculate MOB
    .withColumn(
        "mob",
        F.months_between(
            F.to_date(F.col("ds"), "yyyyMM"),
            F.to_date(F.col("tanggal_realisasi"), "yyyyMM")
        ).cast("int")
    )

    # Add posisi_data
    .withColumn("posisi_data", F.to_date(F.col("ds"), "yyyyMM"))

    # Apply kualitas classification
    .withColumn(
        "kualitas_new",
        F.expr("""
            CASE
                WHEN kol_adk = 1 AND flag_restruk = 'Y' THEN 'Lancar Restruk'
                WHEN kol_adk = 1 THEN 'Lancar'
                WHEN kol_adk = 2 AND jumlah_tunggakan_hari <= 30 THEN 'SML1'
                WHEN kol_adk = 2 AND jumlah_tunggakan_hari <= 60 THEN 'SML2'
                WHEN kol_adk = 2 AND jumlah_tunggakan_hari > 60 THEN 'SML3'
                WHEN kol_adk = 3 THEN 'KL'
                WHEN kol_adk = 4 AND jumlah_tunggakan_hari <= 150 THEN 'D1'
                WHEN kol_adk = 4 AND jumlah_tunggakan_hari > 150 THEN 'D2'
                WHEN kol_adk = 5 THEN 'M'
                ELSE 'Unknown'
            END
        """)
    )
    .filter(F.col("kualitas_new") != "Unknown")
)

#print(f"Base data rows: {df_base.count()}")

# ========================================
# 5️⃣ SEVERITY MAPPING
# ========================================
df_with_severity = df_base.withColumn(
    "severity_current", 
    severity_lookup(F.col("kualitas_new"))
)

# ========================================
# 6️⃣ DETECT FIRST APPEARANCE OF EACH KUALITAS
# ========================================
w_first_appearance = (
    Window.partitionBy("cifno", "new_acctno", "kualitas_new")
    .orderBy(F.col("ds").cast("int"))
)

df_with_first = df_with_severity.withColumn(
    "first_appearance_rank", F.row_number().over(w_first_appearance)
).withColumn(
    "default_date",
    F.when(F.col("first_appearance_rank") == 1, F.col("ds")).otherwise(None)
).withColumn(
    "baki_debet_pemburukan",
    F.when(F.col("first_appearance_rank") == 1, F.col("cbal_base")).otherwise(None)
)

# ========================================
# 7️⃣ PROPAGATE DEFAULT_DATE & BALANCE
# ========================================
w_propagate = (
    Window.partitionBy("cifno", "new_acctno", "kualitas_new")
    .orderBy(F.col("ds").cast("int"))
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

df_propagated = (
    df_with_first
    .withColumn("default_date", F.first("default_date", ignorenulls=True).over(w_propagate))
    .withColumn("baki_debet_pemburukan", F.first("baki_debet_pemburukan", ignorenulls=True).over(w_propagate))
    .drop("first_appearance_rank")
)

# ========================================
# 8️⃣ STICKY DOWNGRADE (WORST SO FAR)
# ========================================
w = (
    Window.partitionBy("cifno", "new_acctno")
    .orderBy(F.col("ds").cast("int"))
    .rowsBetween(Window.unboundedPreceding, 0)
)

df_sticky = df_propagated.withColumn(
    "worst_so_far", F.max("severity_current").over(w)
).withColumn(
    "current_kualitas", F.col("kualitas_new")
)

# ========================================
# 9️⃣ COLLECT BURDEN HISTORY
# ========================================
df_history = df_sticky.withColumn(
    "all_worst_reached",
    F.array_distinct(F.collect_set("worst_so_far").over(w))
)

# ========================================
# 🔟 CREATE KUALITAS METADATA MAP
# ========================================
w_kualitas_meta = (
    Window.partitionBy("cifno", "new_acctno", "kualitas_new")
    .orderBy(F.col("ds").cast("int"))
)

df_kualitas_metadata = (
    df_sticky
    .withColumn("rn", F.row_number().over(w_kualitas_meta))
    .filter(F.col("rn") == 1)
    .select(
        "cifno",
        "new_acctno",
        F.col("severity_current").alias("severity_key"),
        "default_date",
        "baki_debet_pemburukan"
    )
)

# ========================================
# 1️⃣1️⃣ EXPLODE & JOIN METADATA
# ========================================
df_history_clean = df_history.drop("default_date", "baki_debet_pemburukan")

df_kualitas_metadata_aliased = df_kualitas_metadata.selectExpr(
    "cifno as cifno_meta",
    "new_acctno as new_acctno_meta",
    "severity_key",
    "default_date",
    "baki_debet_pemburukan"
)

df_final_actual_only = (
    df_history_clean
    .withColumn("severity_final", F.explode("all_worst_reached"))
    .join(
        df_kualitas_metadata_aliased,
        (df_history_clean["cifno"] == df_kualitas_metadata_aliased["cifno_meta"]) &
        (df_history_clean["new_acctno"] == df_kualitas_metadata_aliased["new_acctno_meta"]) &
        (F.col("severity_final") == df_kualitas_metadata_aliased["severity_key"]),
        "left"
    )
    .drop("cifno_meta", "new_acctno_meta", "severity_key")
    .withColumn("kualitas_final", kualitas_lookup(F.col("severity_final")))
    .withColumn(
        "row_type",
        F.when(F.col("severity_final") == F.col("worst_so_far"), "current").otherwise("burden")
    )
)

# ========================================
# 1️⃣2️⃣ PREPARE FINAL OUTPUT (ACTUAL DATA)
# ========================================
data_vintage_actual = df_final_actual_only.select(
    'cifno',
    'new_acctno',
    'posisi_data',
    'ds',
    'tahun_realisasi',
    'bulan_realisasi',
    'mob',
    'segmen',
    'produk',
    'kualitas_final',
    'flag_restruk',
    'baki_debet_pemburukan',
    'default_date'
).distinct()

# ========================================
# SETUP OUTPUT TABLE
# ========================================
hiveDB = "temp"
hiveTable = "cras_detail_final"

data_vintage_actual\
    .write.format("parquet") \
    .mode("overwrite") \
    .saveAsTable(f"{hiveDB}.{hiveTable}")

#print(f"Vintage actual data rows: {data_vintage_actual.count()}")

# ========================================
# 1️⃣3️⃣ NOW FILL THE GAPS - FIND CLOSED ACCOUNTS
# ========================================
data_vintage_base = spark.table("temp.cras_detail_fin19")
# First, find the last month for each account
df_last_month = (
    data_vintage_base
    .groupBy("cifno", "new_acctno")
    .agg(F.max("ds").alias("last_ds"))
)

# Get ALL rows from that last month (including all burden rows)
df_last_state = (
    data_vintage_base
    .join(df_last_month, ["cifno", "new_acctno"], "inner")
    .filter(F.col("ds") == F.col("last_ds"))
    .drop("last_ds")
)

# Identify accounts that ended before current month
df_to_extend = (
    df_last_state
    .filter(F.col("ds") < ds)
    .withColumn(
        "months_to_add",
        F.months_between(
            F.to_date(F.lit(ds), "yyyyMM"),
            F.to_date(F.col("ds"), "yyyyMM")
        ).cast("int")
    )
    .filter(F.col("months_to_add") > 0)
)

#print(f"Accounts needing extension: {df_to_extend.select('cifno', 'new_acctno').distinct().count()}")

# ========================================
# 1️⃣4️⃣ GENERATE EXTENDED MONTHS
# ========================================
df_extended = (
    df_to_extend
    .withColumn("month_offset", F.explode(F.sequence(F.lit(1), F.col("months_to_add"))))
    .withColumn(
        "ds_new",
        F.date_format(
            F.expr("add_months(to_date(ds, 'yyyyMM'), month_offset)"),
            "yyyyMM"
        )
    )
    .withColumn("mob_new", F.col("mob") + F.col("month_offset"))
    .withColumn("posisi_data_new", F.to_date(F.col("ds_new"), "yyyyMMdd"))

    # Keep all columns, replace ds/mob/posisi_data
    .drop("ds", "mob", "posisi_data", "month_offset", "months_to_add")
    .withColumnRenamed("ds_new", "ds")
    .withColumnRenamed("mob_new", "mob")
    .withColumnRenamed("posisi_data_new", "posisi_data")
)

#print(f"Extended rows generated: {df_extended.count()}")

# ========================================
# 1️⃣5️⃣ UNION ACTUAL + EXTENDED
# ========================================
data_vintage_complete = data_vintage_actual.unionByName(df_extended)

#print(f"Total rows for {product_name}: {data_vintage_complete.count()}")

# ========================================
# 1️⃣6️⃣ WRITE TO FINAL TABLE
# ========================================
# ========================================
# SETUP OUTPUT TABLE
# ========================================
hiveDB = "mpe"
hiveTable = "cras_detail_final"

data_vintage_complete\
    .write.format("parquet") \
    .mode("overwrite") \
    .saveAsTable(f"{hiveDB}.{hiveTable}")
