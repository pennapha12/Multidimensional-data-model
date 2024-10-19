import polars as pl 
import streamlit as st
import pandas as pd
import duckdb as dd 
import plotly.express as px
from streamlit_echarts import st_echarts

# ดึงข้อมูลจาก database ``
conn = dd.connect(r'C:\Users\pwb\Desktop\ปี 3\ปี3เทอม1\Multidimensional data model\dimfact.duckdb')

datacube = pl.read_database(
    query= "SELECT * FROM dimention_fact.datacube",
    connection=conn )
conn.close()

def rename_col(df, colname_dict):
    for old_name, new_name in colname_dict.items():
        df = df.rename({old_name: new_name})
    return df

st.title('Music Stores Data Cube Viewer Dashboard')

#สร้าง sidebar สำหรับเลือก page
page = st.sidebar.selectbox("Select Page", ["Sales Dashboard", "Heat Map", "Employee Sales"])


if page == "Sales Dashboard":
    # ทำ aggregation ของข้อมูลของยอดขายของแต่ละประเทศ
    country_sales = datacube.group_by(['cus_contry']).agg([
        pl.col('total').sum().alias('total_sales')
    ]).sort(['total_sales'], descending=[True]).pipe(rename_col, {'cus_contry': 'Country', 'total_sales': 'Total Sales'}).to_pandas()
    df = pd.read_csv(r'C:\DataWarehouse_Project\all.csv')
    country_sales = country_sales.merge(df[['name', 'region']], left_on='Country', right_on='name', how='left') 
    country_sales = country_sales.drop(columns=['name'])
    regions = {
        'USA': 'Americas',
        'United Kingdom': 'Europe',
        'Czech Republic': 'Europe',
        'Netherlands': 'Europe'
    }
    country_sales['region'] = country_sales['region'].fillna(country_sales['Country'].map(regions))

    st.sidebar.header('Filter Options')
    region_filter = st.sidebar.multiselect('Select Region(s):', options=country_sales['region'].unique(), default=country_sales['region'].unique())

    filtered_countries = country_sales[country_sales['region'].isin(region_filter)]['Country'].unique()

    country_filter = st.sidebar.multiselect('Select Country(s):', options=filtered_countries, default=filtered_countries)

    filtered_data = country_sales[
    (country_sales['region'].isin(region_filter)) &
    (country_sales['Country'].isin(country_filter)) 
    ]

    data = filtered_data.groupby(['region', 'Country']).sum().reset_index()

    sunburst_sales = px.sunburst(
    data,
    path=['region', 'Country'],
    values='Total Sales',
    title='Sales by Region, Country'
    )
# แยก 2 ฝั้งของ layout ให้เป็น 2 คอลัมน์
    col1, col2 = st.columns(2)
# แสดง sunburst chart ในคอลัมน์แรก
    with col1:
        st.subheader('Sales by Region, Country (Sunburst Chart)')
        st.plotly_chart(sunburst_sales, use_container_width=True)
# แสดง bar chart ในคอลัมน์ที่สอง
    with col2:
        st.subheader('Total Sales by Region and Country (Stacked Bar Chart)')
        stacked_bar_chart = px.bar(
            filtered_data,
            x='Country',
            y='Total Sales',
            color='region',
            title='Total Sales by Region and Country',
            labels={'Total Sales': 'Total Sales ($)', 'Country': 'Country'},
            barmode='stack'
        )
        st.plotly_chart(stacked_bar_chart, use_container_width=True)

    # เลือกเฉพาะข้อมูลที่มี region และ country ตามที่เลือก
    filtered_datacube = datacube.filter(pl.col('cus_contry').is_in(country_filter))
    # ทำ aggregation ของข้อมูลของยอดขายของแต่ละประเทศ
    yearly_country_sales = filtered_datacube.group_by(['date_time_year', 'cus_contry']).agg([
        pl.col('total').sum().alias('total_sales')
    ]).sort(['date_time_year', 'total_sales'], descending=[False, True]).pipe(rename_col, {'date_time_year': 'Year', 'cus_contry': 'Country', 'total_sales': 'Total Sales'})

    yearly_country_sales_df = yearly_country_sales.to_pandas()

    # สร้าง line chart
    yearly_line_chart = px.line(
        yearly_country_sales_df,
        x='Year',
        y='Total Sales',
        color='Country',
        title='Yearly Sales by Country',
        labels={'Total Sales': 'Total Sales ($)', 'Year': 'Year'},
        line_group='Country'
    )

    month_sales = filtered_datacube.group_by(['date_time_month', 'cus_contry']).agg([
        pl.col('total').sum().alias('total_sales')
    ]).sort(['date_time_month', 'total_sales'], descending=[False,True]).pipe(rename_col,
    {'date_time_month': 'Month', 'cus_contry': 'Country', 'total_sales': 'Total Sales'})

    monthly_sales_df = month_sales.to_pandas()

    #สร้าง line chart สำหรับยอดขายรายเดือน
    stacked_line_chart = px.line(
        monthly_sales_df,
        x='Month',
        y='Total Sales',
        color='Country',
        title='Monthly Sales by Country',
        labels={'Total Sales': 'Total Sales ($)', 'Month': 'Month'},
        line_group='Country'
    )

    col3, col4 = st.columns(2)
# แสดง line chart สำหรับยอดขายรายเดือนในคอลัมน์แรก
    with col3:
        st.subheader('Monthly Sales by Country (Stacked Line Chart)')
        st.plotly_chart(stacked_line_chart, use_container_width=True)
# แสดง line chart สำหรับยอดขายรายปีในคอลัมน์ที่สอง
    with col4:
        st.subheader('Yearly Sales by Country (Line Chart)')
        st.plotly_chart(yearly_line_chart, use_container_width=True)
        
        
#สร้าง Heat map จากข้อมูลของยอดขายของแต่ละประเภท 
elif page == "Heat Map":
    genre_sales = datacube.group_by(['date_time_year','ge_name']).agg([
        pl.col('total').sum().alias('total_sales')
    ]).sort(['date_time_year','total_sales'], descending=[False,True]).pipe(rename_col, {'date_time_year': 'Year', 'ge_name': 'Genre', 'total_sales': 'Total Sales'})
    st.header('Heat Map Genre Sales')
    heatmap_data = []
    years = genre_sales['Year'].unique().to_list()
    genres = genre_sales['Genre'].unique().to_list()

    for year in years:
        for genre in genres:
            total_sales = genre_sales.filter((pl.col('Year') == year) & (pl.col('Genre') == genre))['Total Sales']
            if total_sales.is_empty():
                value = 0
            else:
                value = total_sales[0]
            heatmap_data.append([genres.index(genre), years.index(year), value])

    option_heatmap = {
        "title": {
            "text": 'Genre Sales Heatmap',
            "left": 'center'
        },
        "tooltip": {
            "position": 'top'
        },
        "grid": {
            "height": '50%',
            "top": '10%'
        },
        "xAxis": {
            "type": 'category',
            "data": genres,
            "splitArea": {
                "show": True
            },
            "axisLabel": {
                "interval": 0,
                "rotate": 75
            }
        },
        "yAxis": {
            "type": 'category',
            "data": years,
            "splitArea": {
                "show": True
            }
        },
        "visualMap": {
            "min": 0,
            "max": max([item[2] for item in heatmap_data]),
            "calculable": True,
            "orient": 'horizontal',
            "left": 'center',
            "bottom": '15%'
        },
        "series": [{
            "name": 'Total Sales',
            "type": 'heatmap',
            "data": heatmap_data,
            "label": {
                "show": False  
            },
            "emphasis": {
                "itemStyle": {
                    "shadowBlur": 10,
                    "shadowColor": 'rgba(0, 0, 0, 0.5)'
                }
            }
        }]
    }

    st_echarts(option_heatmap, height="800px", width="100%")
    
    
# สร้าง bar chart สำหรับยอดขายของพนักงาน 
elif page == "Employee Sales":
    top_employee_sales = datacube.group_by(['date_time_year','em_f','em_l']).agg([
        pl.col('total').sum().alias('total_sales')
    ]).sort(['date_time_year','total_sales'], descending=[False,True]).pipe(rename_col, {'date_time_year': 'Year', 'em_f': 'First', 'em_l': 'Last', 'total_sales': 'Total Sales'}).to_pandas()
    st.header('Top Employee Sales')
    steve_sales = top_employee_sales[top_employee_sales['First'] == 'Steve']['Total Sales'].to_list()
    jane_sales = top_employee_sales[top_employee_sales['First'] == 'Jane']['Total Sales'].to_list()
    margaret_sales = top_employee_sales[top_employee_sales['First'] == 'Margaret']['Total Sales'].to_list()

    options_mix = {
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "cross", "crossStyle": {"color": "#999"}},
        },
        "toolbox": {
            "feature": {
                "dataView": {"show": True, "readOnly": False},
                "magicType": {"show": True, "type": ["line", "bar"]},
                "restore": {"show": True},
                "saveAsImage": {"show": True},
            }
        },
        "legend": {"data": ["Steve Johnson", "Jane Peacock", "Margaret Park", "Total Sales"]},
        "xAxis": [
            {
                "type": "category",
                "data": top_employee_sales['Year'].unique().tolist(),
                "axisPointer": {"type": "shadow"},
            }
        ],
        "yAxis": [
            {
                "type": "value",
                "name": "Sales",
                "min": 0,
                "axisLabel": {"formatter": "{value} $"},
            },
        ],
        "series": [
            {
                "name": "Steve Johnson",
                "type": "bar",
                "data": steve_sales,
            },
            {
                "name": "Jane Peacock",
                "type": "bar",
                "data": jane_sales,
            },
            {
                "name": "Margaret Park",
                "type": "bar",
                "data": margaret_sales,
            },
            {
                "name": "Total Sales",
                "type": "line",
                "data": top_employee_sales.groupby('Year')['Total Sales'].sum().tolist(),
            },
        ],
    }
    st_echarts(options_mix, height="400px", width="100%")
