"""Push workflow example: download, process, upload.

This example shows a complete workflow where you:
1. Fetch data from remote storage
2. Process it locally
3. Push results back to remote storage
"""

from pathlib import Path

from datacachalog import Catalog, Dataset, FileCache, FilesystemStorage


# Define a dataset for the report
report = Dataset(
    name="daily_report",
    source="s3://reports/daily/summary.csv",
    description="Daily summary report",
)

catalog = Catalog(
    datasets=[report],
    storage=FilesystemStorage(),  # Use create_router() for real S3
    cache=FileCache(Path("./data")),
    cache_dir=Path("./data"),
)

# Step 1: Fetch current data
result = catalog.fetch("daily_report")
assert isinstance(result, Path)  # Type narrowing: single-file dataset
input_path = result
print(f"Downloaded report from: {input_path}")

# Step 2: Process the data (your business logic here)
# For example, read with pandas, transform, and write to new file
output_path = Path("./output/updated_report.csv")
output_path.parent.mkdir(parents=True, exist_ok=True)

# Simulate processing
data = input_path.read_text()
processed = f"# Updated at 2024-01-15\n{data}"
output_path.write_text(processed)

# Step 3: Push back to remote storage
catalog.push("daily_report", local_path=output_path)
print(f"Uploaded processed report to: {report.source}")

# The cache is automatically updated after push
# Next fetch will return the cached version unless remote changes
