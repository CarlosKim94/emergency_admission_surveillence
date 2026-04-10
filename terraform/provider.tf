terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.10.0"
    }
  }
}

provider "google" {
  project     = "emergency-admission-492214" # Replace with your real Project ID
  region      = "europe-west1"
  credentials = file("../keys/emergency-admission-492214-c3756c77628d.json") # Path to your key
}