from googleapiclient.discovery import build


def trigger_df_job(cloud_event):   
 
    service = build('dataflow', 'v1b3')
    project = "etl-project-lt-001"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bq-load",  # Provide a unique name for the job
        "parameters": {
        "javascriptTextTransformGcsPath": "gs://etl-crkt-api-metadata/udf.js",
        "JSONPath": "gs://etl-crkt-api-metadata/bq.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "etl-project-lt-001.cricket_dataset.crkt_data_rnkg",
        "inputFilePattern": "gs://etl-crkt-api/batsmen_rankings.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://etl-crkt-api-metadata"
        },
        # Agrega network y subnetwork aquí
        "environment": {
            "network": "lt-vpc-cloud-001",
            "subnetwork": "https://www.googleapis.com/compute/v1/projects/etl-project-lt-001/regions/us-central1/subnetworks/lt-sub-nt-001"
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)