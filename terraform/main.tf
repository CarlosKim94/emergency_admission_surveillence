# --- 1. STORAGE LAYER (Data Lake) ---

# Main Data Lake Bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "emergency-admission-data"
  location      = "europe-west1"
  force_destroy = true
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
}

# Temporary Bucket for Spark-to-BigQuery Transfers
# This is required by the Spark-BigQuery connector
resource "google_storage_bucket" "spark_temp_bucket" {
  name          = "emergency-admission-data-temp"
  location      = "europe-west1"
  force_destroy = true
  uniform_bucket_level_access = true
  
  # Auto-delete temp files after 1 day to keep costs at zero
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}

# --- 2. WAREHOUSE LAYER (BigQuery) ---

resource "google_bigquery_dataset" "dataset" {
  dataset_id = "emergency_admission_data"
  location   = "europe-west1"
}

# --- 3. COMPUTE LAYER (Spark/Dataproc) ---

resource "google_dataproc_cluster" "spark_cluster" {
  name     = "emergency-spark-cluster"
  region   = "europe-west1"
  project  = "emergency-admission-492214"

  cluster_config {
    # Master configuration (Single Node Setup)
    master_config {
      num_instances = 1
      machine_type  = "e2-standard-2" # 2 vCPU, 7.5GB RAM
      disk_config {
        boot_disk_size_gb = 30
      }
    }

    # Software configuration
    software_config {
      image_version = "2.1-debian11"
      override_properties = {
        # This property allows the cluster to run without separate worker nodes
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    # Security and Networking
    gce_cluster_config {
      # Grants the cluster permission to access other GCP services (GCS/BigQuery)
      internal_ip_only = false
      service_account_scopes = [
        "https://www.googleapis.com/auth/cloud-platform"
      ]
    }

    # Cost Management (Auto-shutdown)
    lifecycle_config {
      # Shutdown after 15 mins of inactivity
      idle_delete_ttl = "900s" 
    }
  }
}

  # --- 4. PERMISSIONS ---
resource "google_project_iam_member" "default_compute_worker" {
  project = "emergency-admission-492214"
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:405764081643-compute@developer.gserviceaccount.com"
}