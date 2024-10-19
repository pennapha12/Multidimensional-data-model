import duckdb as dd
import polars as pl
import pandas as pd
import dlt
import sqlite3
from datetime import datetime

#path
db_path=r'C:\Users\pwb\Desktop\ปี 3\ปี3เทอม1\Multidimensional data model\DataWarehouse_Project\chinook.db'
try:
    conn = sqlite3.connect(db_path)
    print("Connection successful")
except Exception as e:
    print(f"Error: {e}")


#show all Table 
def show_tables(connection):
  """Shows all tables in the connected database."""
  cursor = connection.cursor()
  cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
  tables = cursor.fetchall()
  for table in tables:
      print(table[0])
      
show_tables(conn)

#Extract
albums = pl.read_database(
   query= "SELECT * FROM albums",
   connection=conn )
sqlite_sequence = pl.read_database(
   query= "SELECT * FROM sqlite_sequence",
   connection=conn )
sqlite_sequence
artists = pl.read_database(
   query= "SELECT * FROM artists",
   connection=conn )
artists
customers = pl.read_database(
   query= "SELECT * FROM customers",
   connection=conn )
customers
employees = pl.read_database(
   query= "SELECT * FROM employees",
   connection=conn )
employees
genres = pl.read_database(
   query= "SELECT * FROM genres",
   connection=conn )
genres
invoice_items = pl.read_database(
   query= "SELECT * FROM invoice_items",
   connection=conn )
invoice_items
invoices = pl.read_database(
   query= "SELECT * FROM invoices",
   connection=conn )
invoices
media_types = pl.read_database(
   query= "SELECT * FROM media_types",
   connection=conn )
media_types
playlist_track = pl.read_database(
   query= "SELECT * FROM playlist_track",
   connection=conn )
playlist_track
playlists = pl.read_database(
   query= "SELECT * FROM playlists",
   connection=conn )
playlists
tracks = pl.read_database(
   query= "SELECT * FROM tracks",
   connection=conn )
tracks
sqlite_stat1 = pl.read_database(
    query= "SELECT * FROM sqlite_stat1",
    connection=conn )
sqlite_stat1
conn.close()

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


#Tranform albums
albums
Al_f=(albums.pipe(rename_col,{"AlbumId":"Al_Id"})
      .pipe(rename_col,{"Title":"Al_Ti"})
      .pipe(rename_col,{"ArtistId":"Ar_Id"})
      .pipe(add_timestamp,'Time_Stamp')
).to_pandas()
Al_f
 
#Tranform artists
artists
Ar_f=(
    artists.pipe(rename_col,{"ArtistId":"Ar_Id"})
    .pipe(rename_col,{"Name":"Ar_Name"})
    .pipe(add_timestamp,'Time_Stamp')
).to_pandas()
Ar_f
 
#Tranform Customers 
customers.columns
Cus_f = (
    customers.pipe(rename_col, {"CustomerId": "Cus_Id"})  # เปลี่ยนชื่อคอลัมน์
      .pipe(rename_col, {"LastName": "Cus_L"})
      .pipe(rename_col, {"FirstName": "Cus_F"})
      .pipe(rename_col, {"Company": "Cus_Com"})
      .pipe(rename_col, {"Address": "Cus_Ad"})
      .pipe(rename_col, {"City": "Cus_City"})
      .pipe(rename_col, {"State": "Cus_State"})
      .pipe(rename_col, {"Country": "Cus_Contry"})
      .pipe(rename_col, {"PostalCode": "Cus_Pos"})
      .pipe(rename_col, {"Phone": "Cus_Phone"})
      .pipe(rename_col, {"Fax": "Cus_Fax"})
      .pipe(rename_col, {"Email": "Cus_Email"})
      .pipe(rename_col, {"SupportRepId": "Em_Id"})
      .pipe(add_timestamp,'Time_Stamp')
).to_pandas()
Cus_f

# Transformation process using .pipe
Em_f = (
    employees.pipe(rename_col, {"EmployeeId": "Em_Id"})  # เปลี่ยนชื่อคอลัมน์
      .pipe(rename_col, {"LastName": "Em_L"})
      .pipe(rename_col, {"FirstName": "Em_F"})
      .pipe(rename_col, {"Title": "Em_Ti"})
      .pipe(rename_col, {"BirthDate": "Em_Birth"})
      .pipe(rename_col, {"HireDate": "Em_Hire"})
      .pipe(rename_col, {"Address": "Em_Ad"})
      .pipe(rename_col, {"City": "Em_City"})
      .pipe(rename_col, {"State": "Em_State"})
      .pipe(rename_col, {"Country": "Em_Contry"})
      .pipe(rename_col, {"PostalCode": "Em_Pos"})
      .pipe(rename_col, {"Phone": "Em_Phone"})
      .pipe(rename_col, {"Fax": "Em_Fax"})
      .pipe(rename_col, {"Email": "Em_Email"})
      .pipe(add_timestamp,'Time_Stamp')
).to_pandas()
Em_f

#Tranform genres
genres
Ge_f=(
      genres.pipe(rename_col,{"GenreId":"Ge_Id"})
      .pipe(rename_col,{"Name":"Ge_Name"})
      .pipe(add_timestamp,'Time_Stamp')
).to_pandas()
Ge_f

#Tranform invoices
invoices.columns
Inv_f=(
    invoices.pipe(rename_col, {"InvoiceId": "Inv_Id"})
    .pipe(rename_col, {"CustomerId": "Cus_Id"})
    .pipe(rename_col, {"InvoiceDate": "Inv_D"})
    .pipe(rename_col, {"BillingAddress": "B_Ad"})
    .pipe(rename_col, {"BillingCity": "B_Ci"})
    .pipe(rename_col, {"BillingState": "B_St"})
    .pipe(rename_col, {"BillingCountry": "B_Coun"})
    .pipe(rename_col, {"BillingPostalCode": "B_Pos"})
    .pipe(convert_str_to_datetime, 'Inv_D')  # Convert to datetime format
    .pipe(split_month_year,'Date_time')          # Split into month and year
    .pipe(add_timestamp,'Time_Stamp')
   ).to_pandas()
Inv_f

#Tranform invoices_items
invoice_items
Invt_f=(
      invoice_items.pipe(rename_col,{"InvoiceLineId":"Invt_Id"})
      .pipe(rename_col,{"InvoiceId":"Inv_Id"})
      .pipe(rename_col,{"TrackId":"Tr_Id"})
      .pipe(add_timestamp,'Time_Stamp')
).to_pandas()
Invt_f

#Tranform media_types
media_types
Med_f=(
      media_types.pipe(rename_col,{"MediaTypeId":"Med_Id"})
      .pipe(rename_col,{"Name":"Me_Name"})
      .pipe(add_timestamp,'Time_Stamp')
).to_pandas()
Med_f

#Tranform playlists
playlists
Pl_f=(
      playlists.pipe(rename_col,{"PlaylistId":"Pl_Id"})
      .pipe(rename_col,{"Name":"Pl_Name"})
      .pipe(add_timestamp,'Time_Stamp')
).to_pandas()
Pl_f

#Tranform playlist_tracks
playlist_track
PlT_f=(
      playlist_track.pipe(rename_col,{"PlaylistId":"Pl_Id"})
      .pipe(rename_col,{"TrackId":"Tr_Id"})
      .pipe(add_timestamp,'Time_Stamp')
).to_pandas()
PlT_f

#Tranform Track
tracks
tracks.columns
Tr_f=(
      tracks.pipe(rename_col,{"TrackId":"Tr_Id"})
      .pipe(rename_col,{"Name":"Tr_Name"})
      .pipe(rename_col,{"AlbumId":"Al_Id"})
      .pipe(rename_col,{"MediaTypeId":"Med_Id"})
      .pipe(rename_col,{"GenreId":"Ge_Id"})   
      .pipe(add_timestamp,'Time_Stamp')
).to_pandas()
Tr_f

pipeline=dlt.pipeline(
    pipeline_name="StagingArea",destination='duckdb',
    dataset_name="Project_stg"
)

pipeline.run(Al_f,table_name="stg_Albums",write_disposition='append')
pipeline.run(Ar_f,table_name="stg_Artists",write_disposition='append')
pipeline.run(Cus_f,table_name="stg_customers",write_disposition='append')
pipeline.run(Em_f,table_name="stg_employee",write_disposition='append')
pipeline.run(Ge_f,table_name="stg_Genres",write_disposition='append')
pipeline.run(Inv_f,table_name="stg_Invoices",write_disposition='append')
pipeline.run(Invt_f,table_name="stg_Invoices_items",write_disposition='append')
pipeline.run(Med_f,table_name="stg_Media_Types",write_disposition='append')
pipeline.run(Pl_f,table_name="stg_Playlist",write_disposition='append')
pipeline.run(PlT_f,table_name="stg_Playlist_Tracks",write_disposition='append')
pipeline.run(Tr_f,table_name="stg_Tracks",write_disposition='append')
 
#----------------------
path_ch=r'C:\Users\pwb\Desktop\ปี 3\ปี3เทอม1\Multidimensional data model\stagingarea.duckdb'
con_ch=dd.connect(path_ch)

stg_Invoices_items=pl.read_database(query="SELECT * FROM Project_stg.stg_Invoices_items", connection=con_ch)
stg_Invoices_items.columns

stg_Media_Types=pl.read_database(query="SELECT * FROM Project_stg.stg_Media_Types", connection=con_ch)
stg_Media_Types.columns

stg_Playlist=pl.read_database(query="SELECT * FROM Project_stg.stg_Playlist", connection=con_ch)
stg_Playlist.columns

stg_Playlist_Tracks=pl.read_database(query="SELECT * FROM Project_stg.stg_Playlist_Tracks", connection=con_ch)
stg_Playlist_Tracks.columns

stg_Tracks=pl.read_database(query="SELECT * FROM Project_stg.stg_Tracks", connection=con_ch)
stg_Tracks.columns

stg_Invoices=pl.read_database(query="SELECT * FROM Project_stg.stg_Invoices", connection=con_ch)
stg_Invoices.columns

stg_Genres=pl.read_database(query="SELECT * FROM Project_stg.stg_Genres", connection=con_ch)
stg_Genres.columns

stg_Albums=pl.read_database(query="SELECT * FROM Project_stg.stg_Albums", connection=con_ch)
stg_Albums.columns

stg_Artists=pl.read_database(query="SELECT * FROM Project_stg.stg_Artists", connection=con_ch)
stg_Artists.columns

stg_customers=pl.read_database(query="SELECT * FROM Project_stg.stg_customers", connection=con_ch)
stg_customers.columns

stg_employee=pl.read_database(query="SELECT * FROM Project_stg.stg_employee", connection=con_ch)
stg_employee.columns

con_ch.close()