from databricks.sdk import WorkspaceClient
from helpers.compute_analyzer import analyze_cluster_policies, analyze_jobs, analyze_dlt_pipelines
from helpers.compute_analyzer import _analyze_cluster_spec
from databricks.sdk.service.compute import ClusterSource

import pprint
import json
import os
from typing import Dict, Any


def save_results_to_file(results: Dict[str, Any], filename: str) -> None:
    """Save results to a JSON file with pretty printing."""
    with open(filename, "w") as f:
        json.dump(results, f, indent=4)
    print(f"Results saved to {filename}")


def analyze_clusters_in_batches(wc: WorkspaceClient, batch_size: int = 100) -> dict:
    """Analyzes Databricks clusters for DBFS libraries and file references in batches.
    
    Args:
        wc: The Databricks workspace client
        batch_size: Number of clusters to process before saving results
        
    Returns:
        A dictionary containing the results of the analysis.
    """
    res = {}
    i = 0
    batch_count = 0
    
    for cl in wc.clusters.list(page_size=100):
        if cl.cluster_source not in [ClusterSource.UI, ClusterSource.API]:
            continue
        i += 1
        
        # print("Analyzing cluster:", cl.cluster_name)
        finds = {}

        # check if we have any libraries pointing to DBFS
        for lib in wc.libraries.cluster_status(cl.cluster_id):
            dbfs_lib = ""
            if lib.library.jar and lib.library.jar.startswith("dbfs:/"):
                dbfs_lib = lib.library.jar
            if lib.library.whl and lib.library.whl.startswith("dbfs:/"):
                dbfs_lib = lib.library.whl
            if dbfs_lib:
                r = finds.get("libraries", [])
                r.append(dbfs_lib)
                finds["libraries"] = r

        finds = _analyze_cluster_spec(cl, finds)

        # if we found anything, add it to the results
        if finds:
            finds["cluster_name"] = cl.cluster_name
            res[cl.cluster_id] = finds
        
        # Save results every batch_size clusters
        if i % batch_size == 0:
            batch_count += 1
            print(f"Scanned {i} clusters - saving batch {batch_count}")
            if res:
                batch_filename = f"cluster_scan_results_batch_{batch_count}.json"
                save_results_to_file({"clusters": res}, batch_filename)
    
    print(f"Total {i} clusters")
    return res


if __name__ == '__main__':
    wc = WorkspaceClient()
    full_results: dict = {}
    
    # clusters - modified to save results every 100 clusters
    print("Starting scanning clusters")
    cluster_results = analyze_clusters_in_batches(wc, batch_size=100)
    if cluster_results:
        full_results["clusters"] = cluster_results
        # Save final cluster results
        save_results_to_file({"clusters": cluster_results}, "cluster_scan_results_final.json")
    
    # cluster policies
    print("Starting scanning cluster policies")
    cluster_policy_results = analyze_cluster_policies(wc)
    if cluster_policy_results:
        full_results["cluster_policies"] = cluster_policy_results
    
    # jobs
    print("Starting scanning jobs")
    jobs_results = analyze_jobs(wc)
    if jobs_results:
        full_results["jobs"] = jobs_results
    
    # DLT pipelines
    print("Starting scanning DLT pipelines")
    dlt_results = analyze_dlt_pipelines(wc)
    if dlt_results:
        full_results["dlt"] = dlt_results
    
    print("Final scan results: ")
    if full_results:
        pprint.pprint(full_results)
        save_results_to_file(full_results, "compute_scan_results.json")
    else:
        print("Nothing is found")
