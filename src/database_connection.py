import sqlalchemy as sqa
from datetime import datetime
import os

try:
    from loguru import logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger()


def get_time(str_time):
    try:
        return datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%S")
    except:
        logger.error(f"datetime.strptime could not convert {str_time}")


DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "postgres")


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
