# AWS Glue ETL Project 🚀

This project demonstrates an **ETL pipeline in AWS Glue** using Visual ETL + custom PySpark code.  
We join two CSV files (`employee.csv` and `department.csv`), perform transformations, rearrange columns, apply **data quality checks**, and write the results back to Amazon S3.

---

## 🔹 Features
- Read CSV files from Amazon S3  
- Join **Employee** and **Department** tables on `dept_id`  
- Drop unwanted fields (`location`, `manager`, `hire_date`, etc.)  
- Rearrange columns using PySpark DataFrame conversion  
- Convert back to **DynamicFrame** for Glue compatibility  
- Control output files by setting `repartition(1)`  
- Perform basic **data quality checks** with `EvaluateDataQuality`  
- Write final output to S3 in **JSON format**

---

## 📂 Data Flow
1. **Source** → Employee & Department CSVs in S3  
2. **Transform** → Join, drop fields, reorder columns  
3. **Data Quality** → Validate record count & schema  
4. **Target** → JSON output written to S3  

![AWS Glue Architecture](./images/glue-architecture.png)

---

## 🛠️ Technologies Used
- **AWS Glue (ETL)**  
- **PySpark**  
- **Amazon S3**  
- **AWS Glue Data Quality**  

---
## Project Structure
``bash
aws-glue-etl-project/
│── README.md                # Project documentation  
│── glue_job.py              # Your Glue script  
│── /data/                   # Sample CSVs (emp.csv, dept.csv)  
│── /images/                 # Architecture diagram/screenshots  
│── LICENSE                  # (Optional)  
``
