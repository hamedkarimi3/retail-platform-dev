# Tells Terraform which "provider" (cloud) we're using, and which project/region
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = "retail-platform-dev-hk3"
  region  = "europe-north1"
}

# A storage bucket — a place to land files before they go into BigQuery
resource "google_storage_bucket" "landing" {
  name          = "retail-platform-dev-hk3-landing"  # bucket names must be globally unique
  location      = "EU"
  force_destroy = true  # allows terraform destroy to delete it even if it has files, useful for a learning project
}

# Three BigQuery datasets = our Bronze/Silver/Gold layers
resource "google_bigquery_dataset" "bronze" {
  dataset_id = "bronze"
  location   = "EU"
}

resource "google_bigquery_dataset" "silver" {
  dataset_id = "silver"
  location   = "EU"
}

resource "google_bigquery_dataset" "gold" {
  dataset_id = "gold"
  location   = "EU"
}