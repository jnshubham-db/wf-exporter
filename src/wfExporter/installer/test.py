import databricks.sdk as sdk

client = sdk.WorkspaceClient()

j = client.jobs.list(name="[WF] Exporter")
for job in j:
    print(job)
