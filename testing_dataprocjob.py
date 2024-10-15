from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.types import PySparkJob 

def sample_submit_job():
    
    region = "asia-south2"  # Example: "us-central1"
    pyspark_file = "gs://testingbanknifty/scripts/BankniftyTransformationScript6.py"
    cluster_name = "cluster-b8ce"
    # Create a client
    client = dataproc_v1.JobControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})

    # Initialize request argument(s)
    job = dataproc_v1.Job(pyspark_job=pyspark_file)

    request = dataproc_v1.SubmitJobRequest(
        project_id="eis-global-dev",
        region=region,
        clusters = cluster_name,
        job=job,
    )
    # Make the request
    response = client.submit_job(request=request)

    # Handle the response
    print(response)


# Set your configurations
# project_id = "eis-global-dev"
region = "asia-south2"  # Example: "us-central1"
cluster_name = "cluster-b8ce"
bucket_name = "dataproc-cluster-metadata"
pyspark_file = "gs://testingbanknifty/scripts/BankniftyTransformationScript6.py"
job_output_uri = f"gs://{bucket_name}/dataproc-output/"  # Output path for logs
sample_submit_job()