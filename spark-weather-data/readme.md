## Analyzing Weather Data Using Apache Spark

This project uses [Apache Spark](https://spark.apache.org/)  to analyze weather data (years 2010-2019) hosted on the UC CEAS cluster, to answer several relevant information. Refer the attached [project document](./Project_3_FS2019.docs) for the complete requirements.

The final working code is in the [`results`](./results) folder. The python code first transforms the datasets and cleans noise (such as removing unncessary and incorrect information), using RDD - Data Frame conversion, and then uses Spark SQL to query the required information from the cleaned datasets. Results of the required information are also in the [`results`](./results) folder.
  
