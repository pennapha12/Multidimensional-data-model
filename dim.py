import polars as pl
import duckdb as dd
from datetime import datetime 
import dlt
import pandas as pd
import sqlite3

# สร้างการเชื่อมต่อกับฐานข้อมูล DuckDB
db_path_op=r'C:\Users\pwb\Desktop\ปี 3\ปี3เทอม1\Multidimensional data model\stagingarea.duckdb'
con_op=dd.connect(db_path_op)


# ดึงข้อมูลจากตารางแต่ละตาราง
Al_f = pl.read_database(query="SELECT * FROM project_stg.stg_albums", connection=con_op)
Ar_f = pl.read_database(query="SELECT * FROM project_stg.stg_artists", connection=con_op)
Cus_f = pl.read_database(query="SELECT * FROM project_stg.stg_customers", connection=con_op)
Em_f = pl.read_database(query="SELECT * FROM project_stg.stg_employee", connection=con_op)
Ge_f = pl.read_database(query="SELECT * FROM project_stg.stg_genres", connection=con_op)
Inv_f = pl.read_database(query="SELECT * FROM project_stg.stg_invoices", connection=con_op)
Invt_f = pl.read_database(query="SELECT * FROM project_stg.stg_invoices_items", connection=con_op)
Med_f = pl.read_database(query="SELECT * FROM project_stg.stg_media_Types", connection=con_op)
Pl_f = pl.read_database(query="SELECT * FROM project_stg.stg_playlist", connection=con_op)
PlT_f = pl.read_database(query="SELECT * FROM project_stg.stg_playlist_Tracks", connection=con_op)
Tr_f = pl.read_database(query="SELECT * FROM project_stg.stg_tracks", connection=con_op)


# ปิดการเชื่อมต่อกับฐานข้อมูล DuckDB
con_op.close()

print(Al_f) 
print(Ar_f) 
print(Cus_f)
print(Em_f)
print(Inv_f)
print(Invt_f)
print(Med_f)
print(Pl_f)
print(PlT_f)
print(Tr_f)

# ตรวจสอบ Columns ของแต่ละ DataFrame
print(Tr_f.columns)
print(Al_f.columns)
print(Med_f.columns)
print(Ge_f.columns)
print(Ar_f.columns)
print(Em_f.columns)
print(Pl_f.head())
print(PlT_f.head())

#create Function
def rename_col(df, colname_dict):
        for old_name, new_name in colname_dict.items():
            df = df.rename({old_name: new_name})
        return df

def add_timestamp(df, colname):
        current_timestamp = datetime.now()
        df_n = df.with_columns(pl.lit(current_timestamp).alias(colname))
        return df_n

def unique(df, colname):
        return df.unique(subset=[colname])

def sort(df, colname):
        return df.sort(by=colname)

def rename_col(df, colname_dict):
        for old_name, new_name in colname_dict.items():
            df = df.rename({old_name: new_name})
        return df

def exclude(df, colname):
        return df.select(pl.col('*').exclude(colname))


def convert_str_to_datetime(df, column_name, datetime_format='%Y-%m-%d %H:%M:%S'):
    return df.with_columns(
        pl.col(column_name).str.strptime(pl.Datetime, format=datetime_format).alias('Date_time'))


def group_by_and_sum(df: pl.DataFrame, groupby_col: str, sum_col: str) -> pl.DataFrame:
    return df.group_by(groupby_col).agg(pl.col(sum_col).sum().alias(f"{sum_col}_sum"))

def group_by_and_count(df: pl.DataFrame, groupby_col: str) -> pl.DataFrame:
    return df.group_by(groupby_col).agg(pl.count().alias('count'))


def split_month_year(df, datetime_col):
    # Ensure the datetime_col is in datetime format
    df = df.with_columns(
        pl.col(datetime_col).cast(pl.Datetime).alias(datetime_col))
    
    # Add new columns for month and year
    return df.with_columns([
        pl.col(datetime_col).dt.month().alias(f"{datetime_col}_Month"),
        pl.col(datetime_col).dt.year().alias(f"{datetime_col}_Year")])

#-------------------------------------------
# สร้าง dim_tracks
Tr_select = Tr_f.select(['tr_id', 'tr_name', 'al_id', 'med_id', 'ge_id', 'composer', 'milliseconds', 'bytes', 'unit_price'])
Al_select = Al_f.select(['al_id', 'al_ti', 'ar_id'])
Med_select = Med_f.select(['med_id', 'me_name'])
Ge_select = Ge_f.select(['ge_id', 'ge_name'])
Ar_select = Ar_f.select(['ar_id', 'ar_name'])
Pl_select = Pl_f.select(['pl_id', 'pl_name'])
PlT_select = PlT_f.select(['pl_id', 'tr_id'])
Plj = Pl_select.join(PlT_select, on='pl_id', how='left')

dim_tracks = (
      Tr_select.join(Al_select, on='al_id', how='left')
      .join(Med_select, on='med_id', how='left')
      .join(Ge_select, on='ge_id', how='left')
      .join(Ar_select, on='ar_id', how='left')
      .join(Plj, on='tr_id', how='left')
      .pipe(add_timestamp,'Time_Stamp')
)
print(dim_tracks)
dim_tracks.columns

#--------------------------------------------------------------------------------------

# ตรวจสอบว่า address ใน Invoice และ Customer เหมือนกันหรือไม่
Inv_f_select = Inv_f.select(['cus_id', 'b_ci'])
Cus_f_select = Cus_f.select(['cus_id', 'cus_city'])

ch = Inv_f_select.join(Cus_f_select, on='cus_id', how='right')
print(ch)

#---------------------------------------------------------------------------------------

# สร้าง dim_date จากข้อมูล Invoice
Inv_select = Inv_f.select(['inv_id', 'inv_d', 'date_time', 'date_time_month', 'date_time_year'])
date_inv_id = Inv_f.select(['inv_id'])

#transfrom มีการ Add timestamp
dim_date = (
    Inv_select
    .join(date_inv_id, on='inv_id', how='inner')
    .select(['inv_id', 'date_time', 'date_time_month', 'date_time_year'])
    .with_columns([
        pl.when(pl.col('date_time_month').is_between(1, 3)).then(1)
        .when(pl.col('date_time_month').is_between(4, 6)).then(2)
        .when(pl.col('date_time_month').is_between(7, 9)).then(3)
        .otherwise(4).alias('quarter')  # เพิ่มคอลัมน์ไตรมาสตามเงื่อนไข
    ])
    .pipe(add_timestamp,'Time_Stamp')
)
print(dim_date)

#---------------------------------------------------------------------------
# สร้าง dim_customers
dim_customers = Cus_f
print(dim_customers.columns)

#---------------------------------------------------------------------
# สร้าง dim_employees 
dim_employees = (
    Em_f
    .join(Cus_f.select(['cus_id', 'em_id']), on='em_id', how='left')  # เชื่อมกับ cus_id โดยใช้ em_id
)

dim_employees = dim_employees.with_columns(
    pl.when(pl.col('cus_id').is_null()).then(0).otherwise(pl.col('cus_id')).alias('cus_id')
)
dim_employees


#Fact
Inv_f.columns
Invt_f
Tr_f.columns
inv_se=Inv_f.select(['inv_id', 'cus_id', 'total'])
invt_se=Invt_f.select(['invt_id', 'inv_id', 'tr_id', 'unit_price', 'quantity'])
tr_se=Tr_f.select(['tr_id'])
date_sel=dim_date.select(['inv_id'])
cus=dim_customers.select(['cus_id'])
em_se=dim_employees.select(['cus_id','em_id'])

fact_orders=(inv_se.join(invt_se,on='inv_id',how='left')\
              .join(tr_se,on='tr_id',how='left')\
              .join(cus,on='cus_id',how='left')\
              .join(date_sel,on='inv_id',how='left')\
              .join(em_se,on='cus_id',how='left')
              .pipe(add_timestamp,'Time_Stamp')
)
fact_orders

dt=dim_tracks.to_pandas()
dc=dim_customers.to_pandas()
ddt=dim_date.to_pandas()
dem=dim_employees.to_pandas()
ftd=fact_orders.to_pandas()

pipeline1=dlt.pipeline(
    pipeline_name="DimFact",destination='duckdb',
    dataset_name="dimention_fact"
)

pipeline1.run(dc,table_name="dim_customers",write_disposition='append')
pipeline1.run(dem,table_name="dim_employees",write_disposition='append')
pipeline1.run(dt,table_name="dim_tracks",write_disposition='append')
pipeline1.run(ddt,table_name="dim_date",write_disposition='append')
pipeline1.run(ftd,table_name="fact_orders",write_disposition='append')

# สร้างการเชื่อมต่อกับฐานข้อมูล DuckDB
db_path_df = r'C:\Users\pwb\Desktop\ปี 3\ปี3เทอม1\Multidimensional data model\dimfact.duckdb'
conn1 = dd.connect(db_path_df)

date_dim = pl.read_database(query="SELECT * FROM dimention_fact.dim_date", connection=conn1)
date_dim
em_dim = pl.read_database(query="SELECT * FROM dimention_fact.dim_employees", connection=conn1)
em_dim
cus_dim = pl.read_database(query="SELECT * FROM dimention_fact.dim_customers", connection=conn1)
cus_dim
tr_dim= pl.read_database(query="SELECT * FROM dimention_fact.dim_tracks", connection=conn1)
tr_dim
fact_orders= pl.read_database(query="SELECT * FROM dimention_fact.fact_orders", connection=conn1)
fact_orders
dd.close()

print(date_dim.columns)
print(em_dim.columns)
print(cus_dim.columns)
print(tr_dim.columns)
print(fact_orders.columns)

# เปลี่ยนชื่อ time_stamp ในแต่ละตารางก่อนทำการรวม ยกเว้นตารางหนึ่งเพื่อหลีกเลี่ยงการซ้ำกัน
tr_dim1 = tr_dim.rename({"time_stamp": "time_stamp_tr"})
cus_dim2 = cus_dim.rename({"time_stamp": "time_stamp_cus"})
em_dim3= em_dim.rename({"time_stamp": "time_stamp_em"})
date_dim4 = date_dim.rename({"time_stamp": "time_stamp_date"})

# ทำการ join ตารางอีกครั้ง
datacube = fact_orders.join(tr_dim1, on='tr_id', how='inner')\
    .join(cus_dim2, on='cus_id', how='inner')\
    .join(em_dim3, on='em_id', how='inner')\
    .join(date_dim4, on='inv_id', how='inner')
datacube.columns

dc=datacube.to_pandas()

pipeline1.run(dc,table_name="datacube",write_disposition='append')