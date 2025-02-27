# **Insurance Data Ingestion Pipeline**

## **Overview**
This project demonstrates a **data ingestion pipeline** for processing multiple categories of **insurance data files** (XLSX and CSV) using **Snowflake Snowpark**. The pipeline follows the **Medallion Architecture** to ensure structured data processing through **Raw, Clean, and Curated layers**.

The pipeline extracts data from **staged files**, processes them, and loads them into a **Snowflake Data Warehouse**, enabling analytics and reporting.

## **Pipeline Architecture**
The project follows the **Medallion Architecture**:
1. **Bronze Layer (Raw Data)**
   - Extracts files from Snowflake Stages (`@DEV.RAW.FILE_TYPE_2`).
   - Loads raw data into **temporary staging tables** (`nn_raw_temp_f2`).
   
2. **Silver Layer (Cleaned Data)**
   - Performs transformations: handling missing values, standardizing formats.
   - Stores processed data in refined tables.

3. **Gold Layer (Curated Data)**
   - Aggregates data for analytics.
   - Joins datasets for business intelligence use cases.

---

## **Installation and Setup**
### **1. Prerequisites**
Ensure the following are installed:
- **Python 3.10+**
- **Poetry (Dependency Manager)**
- **Snowflake Snowpark Python SDK**
- **Dotenv** for managing credentials
- **OpenPyxl** for handling Excel files

### **2. Installation**
Clone the repository and install dependencies using Poetry:

```sh
git clone <repo-url>
cd <repo-folder>
poetry install
