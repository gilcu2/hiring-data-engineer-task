from prefect import flow, task
from prefect_docker.containers import *
import asyncio


@flow(name="Run Docker Container Flow")
async def run_container_flow():
    docker_container = await create_docker_container(
        image="gilcu2/pipeline_kpi_etc:latest",
        command="python src/pipeline.py",
        environment={
            "PG_URL": "jdbc:postgresql://postgres:5432/postgres?user=postgres&password=postgres",
            "CH_URL": "jdbc:ch://clickhouse:8123/default?user=default&password=12345",
            "SPARK_MASTER_URL": "spark://spark-master:7077",
        }
    )

    docker_container.start()
    docker_container.wait()
    result = docker_container.logs()
    print(f"Docker container run information: {result}")


if __name__ == "__main__":
    run_container_flow.deploy(
        name="run_update_container",
        work_pool_name="default-pool",
    )
