from pathlib import Path
import schedule
from sfapi_client import Client
from sfapi_client.compute import Machine
from sfapi_client.jobs import JobCommand, JobState
import time
from uuid import uuid4 as uuid_gen

import sqlalchemy as sqa
from datetime import datetime
import os


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
#SBATCH -N 4
#SBATCH -J {name}
#SBATCH -o {scratch_dir}/hostname-%j.out
#SBATCH -e {scratch_dir}/hostname-%j.err

srun -n 4 -c 2 --cpu_bind=cores hostname
sleep 4
"""

# Load in configs from environment variables
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "postgres")
USER_NAME = os.getenv("USER_NAME", "dastest")
TOTAL_JOBS = int(os.getenv("TOTAL_JOBS", 0))
KEY_FILE = os.getenv("KEY_FILE", Path("/keys/superfacility/dev.pem"))
PARTITIONS = os.getenv("PARTITIONS", "regular_milan_ss11,gpu_ss11,shared_milan_ss11,shared_gpu_ss11")

DB_UPDATE_TIME = int(os.getenv("DB_UPDATE_TIME", 60))  # once per minute
START_NEW_JOBS = int(os.getenv("START_NEW_JOBS", 600))  # once per hour


def get_time(str_time):
    try:
        return datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%S")
    except:
        logger.error(f"datetime.strptime could not convert {str_time}")


class Database:
    def __init__(self) -> None:
        # Connecto to the database
        db_string = 'postgresql://{}:{}@{}:{}/{}'.format(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
        self.engine = sqa.create_engine(db_string)
        metadata = sqa.MetaData()
        self.job_table = sqa.Table(
            "sfapi_jobs",
            metadata,
            sqa.Column("jobid", sqa.Integer, primary_key=True),
            sqa.Column("uuid", sqa.String),
            sqa.Column("name", sqa.String),
            sqa.Column("nodes", sqa.Integer),
            sqa.Column("nodelist", sqa.String),
            sqa.Column("reason", sqa.String),
            sqa.Column("submit_time", sqa.Time),
            sqa.Column("state", sqa.String),
            sqa.Column("start", sqa.Time),
            sqa.Column("end", sqa.Time),
            sqa.Column("exitcode", sqa.String),
        )

        if not sqa.inspect(self.engine).has_table("sfapi_jobs"):
            logger.info("First run, creating new table")
            metadata.create_all(bind=self.engine)

    def insert_job(self, uuid, job):
        job.update()
        insert_dict = {k: job.dict()[k] for k in (
            "jobid",
            "name",
            "nodes",
            "nodelist",
            "reason",
            "submit_time",
            "state"
        )}
        insert_dict['uuid'] = uuid
        insert_dict['start'] = None
        insert_dict['end'] = None
        insert_dict['exitcode'] = None
        insert_dict['submit_time'] = datetime.strptime(job.submit_time, "%Y-%m-%dT%H:%M:%S")
        stmt = sqa.insert(self.job_table).values(insert_dict)
        with self.engine.connect() as connection:
            connection.execute(stmt)
            connection.commit()

    def update_job(self, job):
        stmt = sqa.select(self.job_table).where((self.job_table.c.jobid == job.jobid))
        with self.engine.connect() as connection:
            jobids = [row.jobid for row in connection.execute(stmt)]

        if len(jobids) > 0:
            stmt = self.job_table.update().values(
                state=job.state,
                reason=job.reason,
                nodelist=job.nodelist,
                start=get_time(job.start),
                end=get_time(job.end),
                exitcode=job.exitcode
            ).where(
                (self.job_table.c.jobid == job.jobid) &
                (self.job_table.c.state != "COMPLETED")
            )

            logger.info(f"Updating {job.jobid}, state={job.state}")
            with self.engine.connect() as connection:
                connection.execute(stmt)
                connection.commit()
                return True
        else:
            insert_dict = {
                "jobid": job.jobid,
                "name": job.jobname,
                "nodes": job.nnodes,
                "nodelist": job.nodelist,
                "reason": job.reason,
                "submit_time": get_time(job.submit),
                "state": job.state,
                "uuid": job.jobname.split("_")[-1],
                "start": get_time(job.start),
                "end": get_time(job.end),
                "exitcode": job.exitcode,
            }
            stmt = sqa.insert(self.job_table).values(insert_dict)
            with self.engine.connect() as connection:
                connection.execute(stmt)
                connection.commit()
                return True

    def get_non_complete_jobs(self):
        stmt = sqa.select(self.job_table).where(
            (self.job_table.c.state == "RUNNING") |
            (self.job_table.c.state == "PENDING")
        )
        # stmt = sqa.select(self.job_table)
        with self.engine.connect() as connection:
            jobids = [row.jobid for row in connection.execute(stmt)]

        return jobids


class PoolManagerDaemon:
    """
    Daemon that periodically checks on this site's active runs.
    """

    def __init__(self, conf):
        logger.info("Initializing pool manager daemon")
        self.client = Client(key=KEY_FILE)
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

        jobs = self.compute.jobs(
            user=self.username,
            command=JobCommand.sacct,
            partition=PARTITIONS
        )
        num_running = 0
        for job in jobs:
            match job.state:
                case JobState.COMPLETED | JobState.CANCELLED:
                    self.job_db.update_job(job)
                case JobState.PENDING | JobState.RUNNING:
                    num_running += 1

        logger.info(f"Running job # = {num_running}")
        if num_running < self.total_num_of_jobs:
            num_to_add = (self.total_num_of_jobs-num_running)
            [self._add_job() for _ in range(num_to_add)]

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
    pool = PoolManagerDaemon(conf=None)
    pool.check_running_and_add_new()
    pool.start_daemon()
