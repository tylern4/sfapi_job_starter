from pathlib import Path
import schedule
from sfapi_client import Client
from sfapi_client.compute import Machine
from sfapi_client.jobs import JobCommand, JobState
import time
from uuid import uuid4 as uuid_gen
from database_connection import Database

import os
from authlib.jose import JsonWebKey
import json


try:
    from loguru import logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger()


job_template = """#!/bin/bash
#SBATCH --qos=debug
#SBATCH --account=nstaff
#SBATCH --time=01:00
#SBATCH --constraint=cpu
#SBATCH -N 2
#SBATCH -J {name}
#SBATCH -o {scratch_dir}/hostname-%j.out
#SBATCH -e {scratch_dir}/hostname-%j.err

srun -n 2 -c 2 --cpu_bind=cores hostname
sleep 4
"""

# Load in configs from environment variables
USER_NAME = os.getenv("USER_NAME", "dastest")
TOTAL_JOBS = int(os.getenv("TOTAL_JOBS", 0))
KEY_FILE = os.getenv("KEY_FILE", Path("/keys/superfacility/dev.pem"))

client_id = os.getenv("SFAPI_CLIENT_ID")
sfapi_secret = os.getenv("SFAPI_SECRET")
if sfapi_secret is not None:
    client_secret = JsonWebKey.import_key(json.loads(sfapi_secret))
else:
    client_secret = None

PARTITIONS = os.getenv("PARTITIONS", "regular_milan_ss11,gpu_ss11,shared_milan_ss11,shared_gpu_ss11")

DB_UPDATE_TIME = int(os.getenv("DB_UPDATE_TIME", 60))  # once per minutes
START_NEW_JOBS = int(os.getenv("START_NEW_JOBS", 600))  # once per hour


class PoolManagerDaemon:
    """
    Daemon that periodically checks on this site's active runs.
    """

    def __init__(self):
        logger.info("Initializing pool manager daemon")
        self.client = Client(key=KEY_FILE) if client_secret is None else Client(client_id, client_secret)
        self.compute = self.client.compute(machine=Machine.perlmutter)
        self.script_path = Path.cwd()
        self.total_num_of_jobs = TOTAL_JOBS
        self.job_db = Database()
        self.username = USER_NAME
        self.scatch_dir = f"/pscratch/sd/{self.username[0]}/{self.username}"

    def start_daemon(self):
        """
        Run scheduled task(s) periodically.
        """
        schedule.every(START_NEW_JOBS).seconds.do(self.check_running_and_add_new)
        schedule.every(DB_UPDATE_TIME).seconds.do(self.update_jobs_in_db)
        while True:
            schedule.run_pending()
            time.sleep(1)

    def update_jobs_in_db(self):
        jobids = self.job_db.get_non_complete_jobs()
        logger.info(jobids)
        if len(jobids) > 0:
            # Get the updated jobs
            jobs = self.compute.jobs(jobids=jobids, command=JobCommand.sacct)
            [self.job_db.update_job(job) for job in jobs]

    def check_running_and_add_new(self):
        """
        Check for runs in particular states.
        """
        logger.debug("Checking the queue")
        logger.debug(f"{self.compute.name} == {self.compute.status}")

        jobs = self._get_jobs()
        num_running = 0
        def completed(j): return j.state == JobState.COMPLETED or j.state == JobState.CANCELLED
        completed_jobs = filter(completed, jobs)
        list(map(self.job_db.update_job, completed_jobs))

        def running(j): return j.state == JobState.PENDING or j.state == JobState.RUNNING
        running_jobs = filter(running, jobs)
        num_running = len(list(running_jobs))
        logger.info(f"Running job # = {num_running}")
        if num_running < self.total_num_of_jobs:
            num_to_add = (self.total_num_of_jobs-num_running)
            logger.info(f"Starting {num_to_add} new jobs")
            [self._add_job() for _ in range(num_to_add)]

    def _get_jobs(self):
        """
        Get the current and past jobs from the user
        Should be equavalent to `sacct -u USERNAME -p PARTITIONS`
        """
        return self.compute.jobs(
            user=self.username,
            command=JobCommand.sacct,
            partition=PARTITIONS
        )

    def _add_job(self):
        """
        Adds a new job to the queue
        """
        job_uuid = uuid_gen()
        logger.info(f"Adding jobs {job_uuid}")
        new_job = self.compute.submit_job(script=job_template.format(name=f"sfapi_job_{job_uuid}",
                                                                     scratch_dir=self.scatch_dir)
                                          )
        # Put the job into the database
        self.job_db.insert_job(job_uuid, new_job)


if __name__ == "__main__":
    pool = PoolManagerDaemon()
    pool.check_running_and_add_new()
    pool.start_daemon()
