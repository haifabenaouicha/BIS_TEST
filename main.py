import argparse
import logging
from jobs.dim_load_job import DimLoadJob
from jobs.fact_job import FactLoadJob
import sys, os

def configure_logger(job_name: str):
    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Formatter with job name
    formatter = logging.Formatter(f"[%(asctime)s] [%(levelname)s] [{job_name}] %(message)s")
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    return logger

def main():
    zip_path = os.path.join(os.path.dirname(__file__), "deps.zip")
    if os.path.exists(zip_path) and zip_path not in sys.path:
        sys.path.insert(0, zip_path)
    parser = argparse.ArgumentParser(description="Run a specific Spark job.")

    parser.add_argument(
        "--job",
        choices=["dim", "fact"],
        help="Which job to run: 'dim' for dimension load, 'fact' for fact load",
    )

    parser.add_argument(
        "--config",
        default="conf/job.conf",
        help="Path to job configuration file (default: conf/job.conf)",
    )

    args = parser.parse_args()
    logger = configure_logger(args.job)

    logger.info(f"Starting job.")
    logger.info(f"Using config: {args.config}")

    try:
        if args.job == "dim":
            job = DimLoadJob(conf_path=args.config)
        elif args.job == "fact":
            job = FactLoadJob(conf_path=args.config)
        else:
            raise ValueError("Unsupported job type.")

        job.execute()
        logger.info("Job completed successfully.")

    except Exception as e:
        logger.exception(f"Job failed with error: {e}")
        raise

if __name__ == "__main__":
    main()
