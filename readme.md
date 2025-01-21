# **Soda Programmatic Checks**

## **Introduction**

### **Key Benefits of Soda’s Approach**
- 🚀 **Automatic Enforcement**: Apply standard quality checks across your entire data stack without manual intervention.
- 🔄 **Effortless Schema Evolution**: Handle changes in schema seamlessly.
- ✅ **Empty Dataset Verification**: Ensure datasets are not empty with automated checks.
- 📊 **Anomaly Detection**: Identify irregularities in row counts to maintain data accuracy.
- 🔍 **Missing Data Identification**: Pinpoint and address issues with missing data effectively.
- ⚙️ **Scalability**: Automatically generate SodaCL for every dataset to scale effortlessly.
- 📚 **Programmatic Integration**: Leverage the Soda Library for large-scale operations across your organization.

### **Results**
Soda’s approach has enabled customers to **rapidly and successfully adopt** data quality checks across their organizations.

---

## **How Does It Work?**

### **1. Automated Dataset Discovery**
- Automatically detect and discover tables in the configured schema of your data sources, including:
  - **PostgreSQL**
  - **Snowflake**
  - **Databricks**
  - **BigQuery**
  - **Redshift**

### **2. Auto-Generate SodaCL**
- Apply basic quality check coverage across all tables and columns:
  - Schema evolution tracking.
  - Verify row count > 0.
  - Detect anomalies in row counts.
  - Null checks for each column.

### **3. Automatically Run Soda Scans**
- Run scans automatically on your data sources.
- Push the results to **Soda Cloud** for easy monitoring and insights.

---

## **How to Deploy**

1. **Pull the GitHub Repository**  
   Clone the Soda Programmatic Checks repo:  
   [GitHub Repository](https://github.com/sodadata/soda-programmatic-checks)
   
2. **Install Python Requirements**  
   Run the following command to install dependencies:  
   ```bash
   pip install -r /path/to/requirements.txt
   
3. **Provide Data Source Connections**
   Add connections to your data sources using the Soda data source YAML format. Here is an example configuration:
   ```yaml
   # Please find all supported data sources on the Soda Docs: https://docs.soda.io/soda/connect-athena.html

    data_source XXXX:
      type: postgres
      connection:
        host: XXXX
        port: XXXX
        username: XXXX
        password: XXXX
        database: XXXX
      schema: XXXX
    
    soda_cloud:
      host: cloud.soda.io # or cloud.us.soda.io
      api_key_id: XXXX
      api_key_secret: XXXX

4. **Add Soda Cloud API Keys**
   Include your Soda Cloud API keys in every data source configuration:
   ```yaml
    soda_cloud:
      host: cloud.soda.io # or cloud.us.soda.io
      api_key_id: XXXX
      api_key_secret: XXXX
   
5. **Run the Scripts**
   Execute the main script to start the programmatic checks:
   ```bash
   python main.py

6. **Schedule the Script**
   Use a cron job (Linux/macOS) or Task Scheduler (Windows) to automate the script execution at a desired frequency.
   Example cron job for running the script daily at midnight:
   ```bash
   0 0 * * * python /path/to/main.py

## **Supported Data Sources**

- 🟦 **Databricks SQL**  
- ❄️ **Snowflake**  
- 🐘 **PostgreSQL**  
- 📊 **BigQuery**  
- 🏢 **SQL Server**


## **Need Help?**

If you encounter any issues, please:  
- **Log a ticket**: [support.soda.io](https://support.soda.io)  
- **Contact us**: [support@soda.io](mailto:support@soda.io)

