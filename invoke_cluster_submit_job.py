from google.cloud import dataproc_v1
import time
from google.api_core.exceptions import NotFound

# Function to start the Dataproc cluster if it's stopped
def start_dataproc_cluster(project_id, region, cluster_name):
    # Create a Dataproc ClusterController client
    cluster_client = dataproc_v1.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})

    # Get the cluster information
    cluster = cluster_client.get_cluster(project_id=project_id, region=region, cluster_name=cluster_name)
    
    # Check if the cluster is not already running
    if cluster.status.state != dataproc_v1.ClusterStatus.State.RUNNING:
        print(f"Starting Dataproc cluster: {cluster_name}...")
        # Issue a request to start the cluster

        # Initialize request argument(s)
        request = dataproc_v1.StartClusterRequest(
            project_id=project_id,
            region=region,
            cluster_name= cluster_name,
        )
        operation = cluster_client.start_cluster(request=request)

        while True:
            try:
                cluster = cluster_client.get_cluster(project_id=project_id, region=region, cluster_name=cluster_name)
                cluster_state = cluster.status.state
                if cluster_state == dataproc_v1.ClusterStatus.State.RUNNING:
                    print(f"Cluster {cluster_name} is now in RUNNING state.")
                    break
                elif cluster_state == dataproc_v1.ClusterStatus.State.ERROR:
                    print(f"Cluster {cluster_name} encountered an ERROR.")
                    break
                else:
                    print(f"Cluster {cluster_name} is in state: {cluster_state}. Waiting...")
                
                time.sleep(30)
            except NotFound:
                print(f"Cluster {cluster_name} not found. Make sure the cluster name, region, and project ID are correct.")
                break

        operation.result()  # Wait for the operation to complete
        print(f"Cluster {cluster_name} started successfully.")
    else:
        print(f"Cluster {cluster_name} is already running.")

# Function to submit the PySpark job to Dataproc
def submit_pyspark_job(project_id, region, cluster_name, pyspark_file_uri):
    # Create a Dataproc JobController client
    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Define the PySpark job
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": pyspark_file_uri
        }
    }

    # Submit the job to the cluster
    operation = job_client.submit_job(project_id=project_id, region=region, job=job)
    print(f"Submitted PySpark job to cluster: {cluster_name}. Job ID: {operation.reference.job_id}")

    # Wait for the job to complete
    job_response = job_client.get_job(project_id=project_id, region=region, job_id=operation.reference.job_id)
    print(f"Job finished with state: {job_response.status.state.name}")

# Main script to run the functions
if __name__ == "__main__":
    # Project and cluster details
    project_id = "eis-global-dev"
    region = "asia-south2"  
    cluster_name = "cluster-b8ce"
    # cluster_name = "cluster-fa66"

    # PySpark job details
    pyspark_file_uri = "gs://testingbanknifty/scripts/BankniftyTransformationScript6.py"  # PySpark script location in GCS

    # Start the cluster (if not running)
    start_dataproc_cluster(project_id, region, cluster_name)

    # Submit the PySpark job
    submit_pyspark_job(project_id, region, cluster_name, pyspark_file_uri)
