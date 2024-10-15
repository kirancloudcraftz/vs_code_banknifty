from google.cloud import dataproc_v1
from google.api_core.operation import Operation

# Set your configurations
project_id = "eis-global-dev"
region = "asia-south2"  # Example: "us-central1"
cluster_name = "cluster-b8ce"
bucket_name = "dataproc-cluster-metadata"
pyspark_file = "gs://testingbanknifty/scripts/BankniftyTransformationScript6.py"


################################
# Create a Dataproc client

client = dataproc_v1.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})

# Initialize request argument(s)
request = dataproc_v1.StartClusterRequest(
    project_id=project_id,
    region=region,
    cluster_name= cluster_name,
)

# Make the request
operation = client.start_cluster(request=request)

print("waiting for operation")


response = operation.result()
print(response)

#########################################


# # Initialize Dataproc Job Controller client
# job_client = dataproc_v1.JobControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})

# Configure the PySpark job details
job = {
    "placement": {"cluster_name": cluster_name},
    "pyspark_job": {"main_python_file_uri": pyspark_file}
}

# Submit the PySpark job
response = client.submit_job(project_id=project_id, region=region, job=job)
job_id = response.reference.job_id


# Optional: Wait for job to finish (you can remove this block if you don't need to wait)
job_result = client.get_job(project_id=project_id, region=region, job_id=job_id)

# Check job status
if job_result.status.state == dataproc_v1.JobStatus.State.ERROR:
    print(f"Job {job_id} failed with error: {job_result.status.details}")
else:
    print(f"Job {job_id} finished successfully.")
