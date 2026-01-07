"""Drop the Salesforce Marketing Cloud pipeline state."""
import dlt

# Get the pipeline
pipeline = dlt.pipeline(
    pipeline_name='salesforce_marketing_cloud_pipeline',
    destination='snowflake',
    dataset_name='salesforce_marketing_cloud'
)

# Drop the pipeline (this removes local state and pending packages)
pipeline.drop()

print("Pipeline dropped successfully!")
print("You can now run the main script again with a fresh start.")
