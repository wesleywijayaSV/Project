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

current_month = mds
#datetime.now().strftime("%Y%m")
start_month = "20210131"

# Calculate max MOB dynamically
start_date = datetime.strptime(start_month, '%Y%m%d')
current_date = datetime.strptime(current_month, '%Y%m%d')
max_mob = (current_date.year - start_date.year) * 12 + (current_date.month - start_date.month)
print(f"Max MOB: {max_mob} (from {start_month} to {current_month}) mds:{mds} lds:{lds}")


# ========================================
# PART 1
# ========================================


# Read these ONCE outside the loop (they don't change)
acctno_mapping = spark.read.table("datamart.cras_vintage_acctno_tob_mapping").select(
    F.col("olacct").alias("old_acctno"),
    F.col("nwacct").alias("new_acctno")
)

df_branch = spark.read.table("datalake.`6969_crm_dim_dwh_branch`")\
    .select("branch", 
            col("rgdesc").alias("kanwil"),
            col("mbdesc").alias("kanca"), 
            col("brdesc").alias("unit_kerja"))
    
# 1. Filter data_vintage by current product
data_vintage = (
    spark.read.table('mpe.cras_detail_fin19_v2')
    .filter(
        (F.col("mob").cast("int").between(0, max_mob)) &
        (F.col("tahun_realisasi") >= 2021) &
        (F.col("ds") >= "202101")   
    )
)

# ========================================
# 2. LOAN MASTER - UNION OF TWO SOURCES
# ========================================

# CRAS loan master (2022 onwards) - Already has kanwil, kanca, unit_kerja
loan_master_real = (
    spark.read.table("mpe.mrm_cras_master_dibrusment")
    .filter(
        (F.col("ds").between(start_month[:6], mds[:6]))
    )
    .join(acctno_mapping, F.col("acctno") == F.col("old_acctno"), "left")
    .withColumn("acctno", F.coalesce(F.col("new_acctno"), F.col("acctno")))
    .drop("old_acctno", "new_acctno")
    .select(
        "cifno", 
        "ds",
        "acctno",
        "branch",
        col("plafond").alias("orgamt_base"),
        "cbal_base",
        "PERIODE_REALISASI",
        col("ket_kanwil").alias("kanwil"),
        col("ket_kanca").alias("kanca"),
        col("ket_unit_kerja").alias("unit_kerja"),
        "SUBPRODUK",
        "fksegmen",
        "produk",
        "tiering"
    )
)


loan_master_branch = loan_master_real  # Keep your original variable name

# 3. Join
df_joined = (
    data_vintage.alias("a")
    .join(loan_master_branch.alias("b"),
          on=(F.col("a.new_acctno") == F.col("b.acctno")) & 
             (F.col("a.cifno") == F.col("b.cifno")),
          how="left")
    .select(
        "a.*",
        "b.branch",
        "b.tiering",
        "b.kanwil",
        "b.kanca",
        "b.unit_kerja",
        "b.SUBPRODUK",
				F.coalesce(F.col("a.flag_restruk"), F.lit("N")).alias("restruk"),
        F.coalesce(F.col("b.orgamt_base"), F.lit(0)).alias("orgamt_base"),
        F.coalesce(F.col("b.cbal_base"), F.lit(0)).alias("cbal_base")
    )
)

# 4. Write
hiveDB = "mpe"
hiveTable = "mrm_detail_vintage_analysis_unit"

df_joined \
    .write.format("parquet") \
    .mode("overwrite") \
    .saveAsTable(f"{hiveDB}.{hiveTable}")




df_data_vintage_join_2 = spark.read.table('mpe.mrm_detail_vintage_analysis_unit')
# -----------------------------
# 2. Agg part
# -----------------------------
df_npl = (
    df_data_vintage_join_2
    .filter(
        (F.col('SUBPRODUK').isin("Kupedes","KUR-Small","KUR-Mikro","KUR SUPER MIKRO","KUPRA","Briguna-Mikro"))&
        (F.col("kualitas_final").isin("SML2","KL","D1","SML1")) &
        (F.col("tahun_realisasi") >= 2021) &
        (
            F.concat(
                F.col("tahun_realisasi").cast("string"),
                F.lpad(F.col("bulan_realisasi").cast("string"), 2, "00")
            ) != F.col("default_date")
        )
    )
    .groupBy(
        "posisi_data", "tahun_realisasi", "bulan_realisasi",
        "mob", "segmen", "branch","kanwil","kanca","unit_kerja",
        "produk","SUBPRODUK",
        col("kualitas_final").alias("kualitas"), col("restruk").alias("flag_restruk")
    )
    .agg(
        F.sum(F.col("baki_debet_pemburukan")).alias("JUMLAH_OS"),
        F.countDistinct( F.col("new_acctno")).alias("JUMLAH_REKENING"),
        F.sum(F.col("orgamt_base")).alias("orgamt_base")
    )
    .withColumnRenamed("ds", "POSISI_TERAKHIR")
    .withColumnRenamed("tahun_realisasi", "TAHUN_REALISASI")
    .withColumnRenamed("bulan_realisasi", "BULAN_REALISASI")
    .withColumnRenamed("mob", "MOB")
    .withColumnRenamed("segmen", "SEGMEN")
    .withColumnRenamed("produk", "PRODUK")
    .withColumnRenamed("kualitas", "KUALITAS")
    .withColumnRenamed("flag_restruk", "FLAG_RESTRUK")
)


hiveDB = "mpe"
hiveTable = "mrm_summary_vintage_analysis_unit" 
df_npl\
  .write.format("parquet") \
  .mode("overwrite") \
  .saveAsTable("{}.{}".format(hiveDB, hiveTable))
