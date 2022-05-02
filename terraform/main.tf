terraform {
  required_version = ">= 1.0"
  backend "local" {}  
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
  credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/prkoviders/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}_project" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

# DWH
resource "google_bigquery_dataset" "dataset" {
  dataset_id = "crime_data"
  project    = var.project
  location   = var.region
}

# Dataproc
# resource "google_dataproc_cluster" "simplecluster" {
#   name   = "cluster-o7"
#   region = var.region

#   cluster_config {

#     master_config {
#       num_instances = 1
#       machine_type  = "e2-medium"
#       disk_config {
#         boot_disk_type    = "pd-ssd"
#         boot_disk_size_gb = 40
#       }
#     }

#   }
# }
