from prefect import flow, task
from pathlib import Path
from prefect.server.schemas.schedules import CronSchedule

@task
def print_one():
    print("Testing one")

@task
def print_two():
    print("Testing two")

@flow(log_prints=True)
def main():
    print_one()
    print_two()
    print("Goodbye !")

if __name__ == "__main__":
    main.from_source(
        source=str(Path(__file__).parent),
        entrypoint="test.py:main",
    ).deploy(
        name="test-deployment",
        work_pool_name="local-pool",
        # cron="* * * * *"
    )