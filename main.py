import sys
from jobs.dim_load_job import DimLoadJob
from jobs.fact_streaming_job import FactLoadJob

if __name__ == "__main__":
    # Optional: Take config path from CLI args, or use default
    config_path = sys.argv[1] if len(sys.argv) > 1 else "conf/job.conf"
    job_dim_tables = DimLoadJob(conf_path=config_path)
    #job_dim_tables.execute()  # This calls the .execute() internally
    job_stream_orders = FactLoadJob(conf_path=config_path)
    job_stream_orders.execute()

