from google.cloud import dataproc_v1

def get_cluster_info(project_id, region, cluster_name):
    # Create a ClusterControllerClient
    cluster_client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Get the cluster information
    cluster = cluster_client.get_cluster(
        project_id=project_id, region=region, cluster_name=cluster_name
    )

    # Display cluster information
    print(f"Cluster Name: {cluster.cluster_name}")
    print(f"Cluster Status: {cluster.status.state.name}")
    print(f"Cluster Config: {cluster.config}")

# Example usage
project_id = "eis-global-dev"
region = "asia-south2"  # Example: "us-central1"
cluster_name = "cluster-b8ce"

get_cluster_info(project_id, region, cluster_name)
