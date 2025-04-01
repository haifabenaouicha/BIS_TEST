import sys
from jobs.dim_load_job import DimLoadJob

if __name__ == "__main__":
    # Optional: Take config path from CLI args, or use default
    config_path = sys.argv[1] if len(sys.argv) > 1 else "conf/job.conf"

    job = DimLoadJob(conf_path=config_path)
    job.execute()  # This calls the .run() internally
