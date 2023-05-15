from airflow import DAG
from airflow.macros import ds_format
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _get_date_part(
        data_interval_start,
        data_interval_end,
        ds, **kwargs):
    print("ds", ds)
    print("data_interval_start", data_interval_start)
    print("data_interval_end", data_interval_end)
    # print(ds_format(ds, "%Y-%m-%d", "%Y/%m/%d/"))
    # return ds_format(ds, "%Y-%m-%d", "%Y/%m/%d/")


with DAG(
    dag_id="play_with_templating",
    schedule="@daily",
    start_date=timezone.datetime(2023, 5, 1),
    catchup=False,
    tags=["DEB", "2023"],
):

    run_this = PythonOperator(
        task_id="get_date_part",
        python_callable=_get_date_part,
        op_kwargs={
            # "ds": "{{ ds }}",
            "data_interval_start": "{{ data_interval_start }}",
            "data_interval_end": "{{ data_interval_end }}",
            "ds": "{{ ds }}",
            "ds_nodash": "{{ ds_nodash }}",
            "ts": "{{ ts }}",
            "ts_nodash_with_tz": "{{ ts_nodash_with_tz }}",
            "ts_nodash": "{{ ts_nodash }}",
            "prev_data_interval_start_success": "{{ prev_data_interval_start_success }}",
            "prev_data_interval_end_success": "{{ prev_data_interval_end_success }}",
            "prev_start_date_success": "{{ prev_start_date_success }}",
            "dag": "{{ dag }}",
            "task": "{{ task }}",
            "macros": "{{ macros }}",
            "ti": "{{ ti }}",
            "params": "{{ params }}",
            "var_value": "{{ var.value }}",
            "var": "{{ var.json }}",
            "conn": "{{ conn }}",
            "task_instance_key_str": "{{ task_instance_key_str }}",
            "conf": "{{ conf }}",
            "run_id": "{{ run_id }}",
            "dag_run": "{{ dag_run }}",
            "test_mode": "{{ test_mode }}",
            "expanded_ti_count": "{{ expanded_ti_count }}"
        },
	)


