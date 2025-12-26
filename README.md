# databricks-lakehouse-medallion
End-to-end data pipeline built on Azure Databricks and ADLS following a Medallion Architecture.
The pipeline is structured into landing, bronze, quality_bronze, silver, quality_silver, and gold layers, where dedicated quality layers apply data quality rules and filters to ensure trusted and consistent data.

It is a fully parameterized pipeline supporting both historical and incremental loads on Delta tables: historical loads allow rebuilding datasets from scratch, while incremental loads keep data up to date without full reprocessing.

The pipeline is orchestrated using Databricks Workflows, demonstrating scalable, flexible, and production-oriented data engineering best practices.

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/0382a9f0-c9b6-48b2-b353-3df7a1b4a3c1" />
