# 🚀 Financial Data Analysis with PySpark (Cloudera)

> An end-to-end financial data analysis project built on Cloudera using Apache Spark (PySpark), focused on loan risk assessment and loan score calculation.

![PySpark](https://img.shields.io/badge/PySpark-3.x-orange?style=for-the-badge&logo=apache-spark&logoColor=white)
![Platform](https://img.shields.io/badge/Platform-Cloudera-blue?style=for-the-badge)
![Domain](https://img.shields.io/badge/Domain-Financial%20Analytics-green?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-In%20Progress-orange?style=for-the-badge)


## 📋 Table of Contents
- 🎯 Project Overview
- 🏗️ Architecture & Data Flow
- 📂 Repository Structure
- 📊 Data Processing & Cleaning
- 🧮 Loan Score Calculation Logic
- 🛠️ How to Run
- 🧪 Testing & Validation
- 🪵 Logging
- 📈 Use Cases
- 🧠 Key Learnings
- 🔮 Future Enhancements
- 📞 Connect

## 🎯 Project Overview

This repository demonstrates a real-world **financial data analysis pipeline** developed using **Apache Spark (PySpark)** on the **Cloudera platform**.

The project focuses on calculating a **Loan Score** for customers based on multiple financial and behavioral factors. Raw financial datasets are cleaned, processed, and stored in HDFS, followed by analytical transformations to support downstream reporting and decision-making.

The solution follows industry best practices such as:
- Modular PySpark development
- External table creation for multi-team access
- Views and pre-aggregated datasets
- Scalable financial analytics on big data

## 🏗️ Architecture & Data Flow
<img width="1536" height="1024" alt="project_2_architecture" src="https://github.com/user-attachments/assets/5c62beb0-2f9d-4f97-991e-009f895e5b89" />

Raw Financial Data  
↓  
HDFS Landing Zone  
↓  
Cleaned / Processed Data  
↓  
External Tables & Views  
↓  
Loan Score Calculation  
↓  
Analytics & Reporting

## 📂 Repository Structure

<img width="406" height="317" alt="Screenshot 2026-02-05 at 10 33 53 AM" src="https://github.com/user-attachments/assets/84a8975d-ee97-45af-9f10-17bc4d58ba10" />


## 📊 Data Processing & Cleaning

The project processes multiple financial datasets related to customer credit behavior. Key datasets include:
- Loan repayment history
- Loan defaulters history
- Public records, bankruptcies, and credit enquiries

Processing steps include:
- Schema enforcement and type casting
- Handling null values and invalid records
- Removing duplicate and bad records (repeating member IDs)
- Creating cleaned and processed datasets for downstream use

Cleaned datasets are written back to HDFS in both **CSV** and **Parquet** formats.

## 🧮 Loan Score Calculation Logic

Analytical scoring is derived from multiple contributing factors, including:

1. **Payment Behavior**
   - Historical payment patterns
   - Consistency of repayments

2. **Risk Indicators**
   - Delinquency trends
   - Adverse credit-related events
   - Frequency of recent credit activities

3. **Financial Profile**
   - Customer financial attributes
   - Account and funding characteristics
   - Overall credit standing


## 🛠️ How to Run

### Prerequisites
- Python 3.x
- Apache Spark (Cloudera distribution)
- PySpark
- HDFS access
- Pipenv for dependency management

### Execution Steps
1. Ingest raw financial data into HDFS landing directories
2. Run PySpark cleaning and processing scripts
3. Create external tables on cleaned datasets
4. Generate views for consolidated reporting
5. Execute loan score calculation jobs
6. Store final results in processed HDFS directories

## 🧪 Testing & Validation

The project can be used to test through **Pytest** for unit testing:
- Validates record counts after transformations
- Tests configuration loading
- Ensures correctness of business logic

Fixtures are used to create and manage SparkSession objects during testing, ensuring isolated and repeatable test execution.

## 🪵 Logging

Application-level logging can be implemented using **Log4j**, aligned with Spark’s native logging framework.

Logging benefits include:
- Configurable log levels (INFO, WARN, ERROR)
- Console and file-based logging
- Improved debugging and production readiness

## 📈 Use Cases

- Loan approval and risk assessment
- Credit behavior analysis
- Financial health evaluation
- Regulatory and compliance reporting
- BI and analytics consumption

## 🧠 Key Learnings

- Building scalable financial analytics using PySpark
- Processing large datasets on Cloudera
- Implementing external tables and views
- Applying weighted scoring models
- Writing testable and maintainable Spark code
- Managing dependencies with Pipenv

## 🔮 Future Enhancements

- Logging to implement
- Pytest
- Workflow orchestration
- Data quality validation frameworks


## 📞 Connect

Author: Syed Nazish Haque  
Role: Tech Lead / Data Engineer  
Email: sn.haque136@gmail.com  

⭐ Star this repository if you find it useful  
🚀 Building scalable financial data platforms with PySpark
