from googleapiclient.discovery import build


def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "etl-project-lt-001"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
    "jobName": "bq-aut-load",  # Provide a unique name for the job
    "parameters": {
        "inputFilePattern": "gs://bkt-zn-etl/user.csv",
        "JSONPath": "gs://bkt-df-metadata-lt-004/bq.json",
        "outputTable": "etl-project-lt-001:user_data.users",
        "bigQueryLoadingTemporaryDirectory": "gs://bkt-df-metadata-lt-004",
        "javascriptTextTransformGcsPath": "gs://bkt-df-metadata-lt-004/udf.js",
        "javascriptTextTransformFunctionName": "transform"
    },
    "environment": {
    "network": "lt-vpc-cloud-001",
    "subnetwork": "https://www.googleapis.com/compute/v1/projects/etl-project-lt-001/regions/us-central1/subnetworks/lt-sub-nt-001"
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)

