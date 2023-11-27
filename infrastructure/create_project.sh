#!/bin/bash

# Set your GCP project details
PROJECT_ID="wraml6"
PROJECT_NAME="Coding-Challenge"


# Check if the project already exists
if gcloud projects describe $PROJECT_ID &> /dev/null; then
  echo "Project $PROJECT_ID already exists. Proceeding..."
else
  # If the project does not exist, create it
  echo "Creating project $PROJECT_ID..."
  gcloud projects create $PROJECT_ID --name="$PROJECT_NAME" --set-as-default

  
  read -p "Enter your billing account ID: " BILLING_ACCOUNT
  gcloud beta billing projects link $PROJECT_ID --billing-account=$BILLING_ACCOUNT

  # Set the default project
  gcloud config set project $PROJECT_ID

  # Enable required APIs (+
  gcloud services enable compute.googleapis.com
  gcloud services enable storage.googleapis.com
  gcloud services enable dataflow.googleapis.com


  echo "Project setup complete. Your new project ID is: $PROJECT_ID"
fi

export TF_VAR_project_id=$PROJECT_ID

# Run Terraform
cd terraform
terraform apply
