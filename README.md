# Emergency Admission for Respiratory Related Diseases

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat&logo=Apache%20Airflow&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-4285F4?style=flat&logo=google-cloud&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![Terraform](https://img.shields.io/badge/terraform-%235835CC.svg?style=flat&logo=terraform&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=Streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=flat&logo=docker&logoColor=white)

## 📌 Overview
This project implements an end-to-end Data Engineering pipeline to monitor and visualize emergency department admission trends. By automating the flow from raw data ingestion to a live dashboard, the system provides healthcare analysts with up-to-date insights into emergency admission for respiratory related diseases.

The tech stack leverages **GCP (BigQuery)**, **Terraform** for infrastructure-as-code, **Airflow** for orchestration, **dbt** for transformations, **Spark/PySpark** for batch processing, **Docker** for containerization, and **Streamlit** for the final visualization.

---

## ❓ Problem Description
Healthcare facilities often store admission records. Manual monitoring of sudden increases in emergency admissions for specific illness types is slow and prone to human error, which can delay emergency resource allocation. 

This project solves the problem by providing a centralized, automated surveillance system. It ingests daily admission data, processes it through a robust warehouse (BigQuery), applies analytical transformations (7-day rolling averages), and visualizes the results. This allows healthcare professionals to identify potential outbreaks or hospital capacity issues at a glance.

---

## 📊 Dataset
The data on emergency admissions for respiratory diseases is collected daily as part of the AKTIN registration and published by the Robert Koch Institute (RKI). The dataset includes the date of the admission report, the amount of emergency departments that reported on that day, the average of the admissions per emergency department, the patient's age group and the diagnoses coded according to the International Classification of Diseases (ICD-10). The data is based on individual willingness to participate in emergency department surveillance reporting as part of the AKTIN emergency room register, and then made available to the RKI. 

---

## 🏗️ Data Pipeline Architecture

<img src="https://github.com/CarlosKim94/emergency_admission_surveillence/blob/main/images/de_architecture.png" width="900" height="500">

The table below defines the project's technology stack and the optimization strategy implemented.

| Pipeline Stage | Components | Primary Tool(s) | Architecture Details |
| :--- | :--- | :--- | :--- |
| **Ingestion** | Data Source | **Spark (PySpark)** | Ingests raw CSV data. **`PySpark`** reads the CSV, cleans the schema, and writes it to GCS (Data Lake) as Parquet. |
| **Orchestration** | Workflow | **Airflow** | Manages the End-to-End DAG. Tasks are orchestrated. It is essentially the "Brain." It triggers Spark, then triggers the BigQuery Load, then triggers dbt. |
| **Warehouse** | Staging Area | **BigQuery** | Served as the central repository. Raw data is loaded from GCS into "Staging" tables.|
| **Transformations** | Data Modeling | **dbt** | Runs SQL inside BigQuery to transform raw staging tables into optimized "Marts." **Staging models** handle type casting. **Mart models** handle critical analytical logic like the **7-day rolling average**. Partitioning and Clustering are applied here to the final Marts.|
| **Dashboard** | Visualization | **Streamlit** | Live presenting of time-series data using Plotly. Queries the final Marts in BigQuery to display the dashboard, which pulls directly from optimized tables for speed.|
| **Infrastructure** | Containerization, IaC | **Docker, Terraform** | The pipeline environment is containerized via Docker. Infrastructure deployment (BigQuery datasets, GCS buckets) is fully defined and automated using **Terraform**. |

---

## 📊 Dashboard
[The Streamlit dashboard](https://emergencyadmissionsurveillence.streamlit.app/)

<img src="https://github.com/CarlosKim94/emergency_admission_surveillence/blob/main/images/dashboard.png" width="900" height="1100">

---

## 📁 Repository Structure

```bash
├── dags
│   └── ingest_data.py
├── dbt
│   ├── emergency_dbt/
│   └── profiles.yml
├── docker-compose.yaml
├── Dockerfile
├── Dockerfile.streamlit
├── pyproject.toml
├── README.md
├── requirements.txt
├── scripts
│   └── transform.py
├── streamlit_app
│   └── app.py
├── terraform
│   ├── main.tf
│   ├── provider.tf
│   ├── terraform.tfstate
│   └── terraform.tfstate.backup
└── uv.lock
```

---

## ▶️ How to Run the Project

### 1. Clone the Repository

```bash
git clone https://github.com/CarlosKim94/emergency_admission_surveillence.git
cd pneumonia_detection
```

### 2. Create Virtual Environment

```bash
pip install uv
source .venv/bin/activate
```

To deactivate the virtual environment
```bash
deactivate
```

### 3. Google Cloud Authentication

For the pipeline to interact with Cloud environment, you must provide your own credentials:
- Generate a JSON key for your Service Account in the GCP Console.
- Create a folder named keys/ in the project root.
- Save your key as keys/service_account.json. (This folder is listed in .gitignore to prevent accidental credential leaks).

### 4. Infrastructure Setup (Terraform)

Before running the pipeline, initialize cloud resources:

```bash
cd terraform/
terraform init
terraform apply
```

### 5. Orchestration (Airflow & Spark)
Use Docker-Compose to manage the Airflow scheduler and worker nodes:

`docker-compose up -d`

Once the containers are running, you can access the Airflow UI at localhost:8080. Toggle the emergency_admission_dag to "On" to trigger the PySpark ingestion and BigQuery load.

### 6. Execute Transformations (dbt)
Once the raw data is loaded into BigQuery, run the dbt models to build the optimized analytics tables:

```bash
cd dbt/emergency_dbt
dbt run --profiles-dir .
```

### 7. View the Dashboard

`streamlit run streamlit_app/app.py`

### 8. Deploy on Cloud

https://github.com/user-attachments/assets/e08a64ba-eefa-4dc8-a764-730a089acb78

---

## Acknowledgments

- Data Source: [Robert Koch Institute (RKI) – Notaufnahmesurveillance](https://github.com/robert-koch-institut/Daten_der_Notaufnahmesurveillance)
- Libraries & Tools: Python, Plotly, Google Cloud, dbt, Apache Airflow, Apache Spark, Terraform, Streamlit, Docker
