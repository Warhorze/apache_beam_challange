provider "google" {
  project     = var.project_id
  region      = var.region 
}
#google requires a organisation to create a project via terraform.


resource "random_id" "bucket_prefix" {
  byte_length = 8
}

resource "google_storage_bucket" "static" {
  name          = "${random_id.bucket_prefix.hex}-bucket"
  location      = "EU"
  storage_class = "COLDLINE"
  labels = {
    environment = "development"
    creator = "wesley"
    project = "codingchallange"
  }
  uniform_bucket_level_access = true
}
output bucket_name {
  value       = google_storage_bucket.static.name
  description = "bucket name"
}

