# Multidimensional-data-model
Mini Project for Data Warehouse and Big Data Analytics

# Overview Project

## Project การสร้าง Multidimensional data model และแดชบอร์ดนําเสนอข้อมูลของฐานข้อมูล Chinook ผ่านมุมมองทางธุรกิจ 
*โปรเจคนี้มุ่งเน้นการสร้าง Multidimensional Data Model และการออกแบบแดชบอร์ดสำหรับฐานข้อมูล Chinook ผ่านมุมมองเชิงธุรกิจ โดยมีวัตถุประสงค์หลักคือ:
*การพัฒนาโมเดลข้อมูลหลายมิติ (Multidimensional Data Model) และการจัดสร้าง Data Cube เพื่อวิเคราะห์ข้อมูลเชิงลึกและตอบโจทย์ทางธุรกิจได้อย่างมีประสิทธิภาพ
*สร้าง Dashboard เพื่อแสดงผลข้อมูลที่ผ่านการวิเคราะห์แล้วในรูปแบบที่เข้าใจง่ายและช่วยให้การตัดสินใจเชิงธุรกิจมีความรวดเร็วและแม่นยำมากขึ้น

# File details

1. ETL.py ไฟลล์นี้เป็นไฟล์ที่ใช้ในกระบวนการ ETL
   1.1 Extract ข้อมูลมาจาก Operational database (Chinook.db)
   1.2 Transform Attribute ต่างๆที่อยู่ใน Table
   1.3 Load data ที่มีการ Transform แล้ว เก็บไว้ใน stagingarea.duckdb
2. dim.py ไฟลล์นี้เป็นไฟลล์ที่ใช้ในการสร้าง data cube
   2.1 ทำการ Extract ข้อมูล มาจาก stagingarea.duckdb
   2.2 ทำการ join ตารางต่างๆ เข้าด้วยกัน เพื่อสร้างเป็น Dimension Table and Fact Table
   2.3 Load ข้อมูลไปเก็บไว้ใน dimfact.duckdb
   2.4 ทำการ join dimension ต่างๆ และ fact เข้าด้วยกันเป็นตารางเดียว ชื่อ datacube (เพื่อให้ Query ข้อมูลจากตารางเดียว ตอนทำ dashboard)
   2.5 Load datacube ไปเก็บไว้ใน dimfact.duckdb
3. dashboard.py ไฟลล์นี้ใช้ในการสร้าง Web Application
   3.1 Etract datacube มาจาก dimfact.duckdb
   3.2 สร้าง กราฟและตารางต่างๆ 
   3.2 สร้าง หน้า Dashboard เพื่อตอบคำถามมุมมองธุรกิจต่างๆที่ตั้งไว้ 
