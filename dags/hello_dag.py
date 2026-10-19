from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="hello_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=["demo"],
)
def hello_dag():
    @task
    def extract() -> list[int]:
        return [1, 2, 3]

    @task
    def transform(numbers: list[int]) -> int:
        return sum(numbers)

    @task
    def load(total: int) -> None:
        print(f"總和為 {total}")

    load(transform(extract()))


hello_dag()
