# Create the Data Lake (GCS Bucket)
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "emergency-admission-data" # Must be unique across all of Google
  location      = "europe-west4"
  force_destroy = true
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
}

# Create the Data Warehouse (BigQuery Dataset)
resource "google_bigquery_dataset" "dataset" {
  dataset_id = "emergency_admission_data"
  location   = "europe-west4"
}