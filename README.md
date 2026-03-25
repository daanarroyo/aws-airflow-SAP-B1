# 📊 SAP Business One → AWS MWAA → S3 (Parquet) Pipeline

This project implements a data pipeline to extract data from SAP Business One (Service Layer), orchestrate it using Apache Airflow (Amazon MWAA), and store it in Amazon S3 in Parquet format, ready for analytics with AWS Glue and Athena.

## 🚀 Architecture

SAP B1 (Service Layer API)
        ↓
Amazon MWAA (Airflow DAGs)
        ↓
Transformation (Pandas + PyArrow)
        ↓
Amazon S3 (Parquet - Data Lake)
        ↓
AWS Glue Catalog
        ↓
Amazon Athena / BI Tools

## 🎯 Objective

- Extract SAP B1 data in an automated and scalable  
- Implement incremental loads (CDC based on timestamps)  
- Store data in columnar format (Parquet + Snappy)  
- Enable a data lake ready for analytics and BI  

## 📦 Core Components

### Orchestration
- Amazon MWAA (Managed Airflow)

### Data Source
- SAP Business One Service Layer (REST/OData API)

### Security
- AWS Secrets Manager

### Processing
- Python + Pandas + PyArrow

### Storage
- Amazon S3 (Data Lake)

### Catalog (Optional)
- AWS Glue Data Catalog

## 📁 Project Structure

.
├── dags/
│   └── sap_b1_orders_to_parquet.py
├── requirements.txt
└── README.md

## ⚙️ Configuration

### AWS Secrets Manager

{
  "company_db": "DemoCompany",
  "username": "datasync",
  "password": "********",
  "base_url": "https://sapb1.example.com:50000/b1s/v1"
}

### Airflow Variables

sap_b1_orders_last_watermark = 2026-01-01

### S3 Structure

s3://my-data-lake/sap-b1/orders/load_date=YYYY-MM-DD/orders.parquet

## 🔄 DAG Workflow

1. Retrieve credentials from Secrets Manager  
2. Login to SAP B1  
3. Extract data (incremental + paginated)  
4. Transform JSON → DataFrame  
5. Convert to Parquet  
6. Upload to S3  
7. Update watermark  

## 📊 Output

- Format: Parquet  
- Compression: Snappy  
- Partition: load_date  

## 🧱 Best Practices

- Incremental loads  
- Pagination  
- No hardcoded credentials  
- Optimized for analytics  

## ⚠️ Considerations

- Use secure network (VPN / VPC)  
- Avoid full loads  
- Validate data types  

## 🏁 Outcome

- Scalable Data Lake  
- Queryable with Athena  
- Ready for BI / ML  
