import pytest
from datetime import datetime
from airflow import DAG
from airflow.models import TaskInstance
from smart_transfer.smart_transfer import SmartTransfer
import os
import psycopg2


PREOPERATOR1 = """
    DROP TABLE IF EXISTS test_target;
    DROP TABLE IF EXISTS test_source;
    CREATE TABLE test_source (
        texto text,
        numero numeric,
        updated_at timestamp without time zone,
        jsonobject jsonb
    );
    INSERT INTO test_source (texto,numero,jsonobject, updated_at) values 
    ('um' , 1,'[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json, '2020-01-01'),
    ('dois',2,'[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json, '2020-02-02'),
    ('tres',3,'[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json, '2020-03-03');
"""

PREOPERATOR2 = """
    DROP TABLE IF EXISTS test_target2;
    DROP TABLE IF EXISTS test_source2;
    CREATE TABLE test_source2 (
        id uuid PRIMARY KEY,
        texto text,
        numero numeric,
        updated_at timestamp without time zone,
        jsonobject jsonb
    );
    INSERT INTO test_source2 (id, texto,numero,jsonobject, updated_at) values 
    ('8320562f-2969-42f9-a852-249646afba02', 'um' , 1,'[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json, '2020-01-01'),
    ('8320562f-2969-42f9-a852-249646afbb02', 'dois',2,'[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json, '2020-02-02'),
    ('8320562f-2969-42f9-a852-249646afbc02', 'tres',3,'[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json, '2020-03-03');
"""

PREOPERATOR3 = """
    DROP TABLE IF EXISTS public.test_target3;
    DROP TABLE IF EXISTS public.test_source3;
    CREATE TABLE public.test_source3 (
        id uuid PRIMARY KEY,
        texto text,
        numero numeric,
        updated_at timestamp without time zone,
        jsonobject jsonb
    );
    INSERT INTO public.test_source3 (id, texto,numero,jsonobject, updated_at) values 
    ('8320562f-2969-42f9-a852-249646afba02', 'um' , 1,'[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json, '2020-01-01'),
    ('8320562f-2969-42f9-a852-249646afbb02', 'dois',2,'[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json, '2020-02-02'),
    ('8320562f-2969-42f9-a852-249646afbc02', 'tres',3,'[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json, '2020-03-03');
"""


def test_oracle_col_to_type():
    actual = SmartTransfer.oracle_col_to_type(("", cx_Oracle.DB_TYPE_VARCHAR))
    assert actual == ('text', '{column}')
    actual = SmartTransfer.oracle_col_to_type(("", cx_Oracle.DB_TYPE_NCHAR))
    assert actual == ('text', '{column}')
    actual = SmartTransfer.oracle_col_to_type(("", cx_Oracle.DB_TYPE_DATE))
    assert actual == ("timestamp without time zone", "{column}")
    actual = SmartTransfer.oracle_col_to_type(
        ("", cx_Oracle.DB_TYPE_TIMESTAMP))
    assert actual == ("timestamp without time zone", "{column}")
    actual = SmartTransfer.oracle_col_to_type(("", cx_Oracle.DB_TYPE_NUMBER))
    assert actual == ("numeric", "{column}")


def test_smart_transfer_operator():
    dag = DAG(dag_id="smart_transfer", start_date=datetime.now())
    task = SmartTransfer(
        task_id="smart_transfer_test",
        source_table="test_source",
        source_conn_id="test_db",
        destination_table="test_target",
        destination_conn_id="test_db",
        updated_at_filter=False,
        commit_every=100,
        preoperator=PREOPERATOR1,
        dag=dag,
    )

    ti = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(ti.get_template_context())
    print("Result: {}".format(result))
    assert result == 3


def test_smart_transfer_operator_with_filter():
    dag = DAG(dag_id="smart_transfer", start_date=datetime.now())
    task = SmartTransfer(
        task_id="smart_transfer_test",
        source_table="test_source",
        source_conn_id="test_db",
        destination_table="test_target",
        destination_conn_id="test_db",
        updated_at_filter=True,
        commit_every=100,
        preoperator=PREOPERATOR1,
        dag=dag,
    )

    ti = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(ti.get_template_context())
    print("Result: {}".format(result))
    assert result == 3


def test_smart_transfer_operator_with_primary_key():
    dag = DAG(dag_id="smart_transfer", start_date=datetime.now())
    task = SmartTransfer(
        task_id="smart_transfer_test_pk",
        source_table="test_source2",
        source_conn_id="test_db",
        destination_table="test_target2",
        destination_conn_id="test_db",
        updated_at_filter=False,
        commit_every=100,
        preoperator=PREOPERATOR2,
        dag=dag,
    )

    ti = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(ti.get_template_context())
    print("Result: {}".format(result))
    assert result == 3


def test_smart_transfer_operator_with_schema_name_pk():
    dag = DAG(dag_id="smart_transfer", start_date=datetime.now())
    task = SmartTransfer(
        task_id="smart_transfer_test_pk",
        source_table="public.test_source3",
        source_conn_id="test_db",
        destination_table="public.test_target3",
        destination_conn_id="test_db",
        updated_at_filter=False,
        commit_every=100,
        preoperator=PREOPERATOR3,
        dag=dag,
    )

    ti = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(ti.get_template_context())
    print("Result: {}".format(result))
    assert result == 3


def test_smart_transfer_operator_skip_columns():
    dag = DAG(dag_id="smart_transfer", start_date=datetime.now())
    task = SmartTransfer(
        task_id="smart_transfer_test_pk",
        source_table="public.test_source3",
        source_conn_id="test_db",
        destination_table="public.test_target3",
        destination_conn_id="test_db",
        updated_at_filter=False,
        commit_every=100,
        skip_columns=['jsonobject'],
        preoperator=PREOPERATOR3,
        dag=dag,
    )

    ti = TaskInstance(task=task, execution_date=datetime.now())
    result = task.execute(ti.get_template_context())
    print("Result: {}".format(result))
    assert result == 3
