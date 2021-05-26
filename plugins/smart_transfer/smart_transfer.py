import logging
import cx_Oracle
import psycopg2
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
from commons.util import grouper
import csv
from typing import List, Dict, Tuple, Optional, Any
import re

QUERY_PRIMARY_KEY = """select conrelid::regclass AS table_from, conname, pg_get_constraintdef(c.oid) as s
from   pg_constraint c
join   pg_namespace n ON n.oid = c.connamespace
where  contype in ('p')
and conrelid = '{name_filter}'::regclass """

class SmartTransfer(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        source_table,
        source_conn_id,
        destination_table,
        destination_conn_id,
        updated_at_filter,
        preoperator=None,
        commit_every=1000,
        conflict_action="",
        ignore_pk=False,
        oracle_source=False,
        skip_columns=[],
        *args,
        **kwargs,
    ):
        super(SmartTransfer, self).__init__(*args, **kwargs)
        self.source_table: str = source_table
        self.source_conn_id: str = source_conn_id
        self.destination_table: str = destination_table
        self.destination_conn_id: str = destination_conn_id
        self.updated_at_filter: bool = updated_at_filter
        self.ignore_pk: bool = ignore_pk
        self.preoperator: Optional[str] = preoperator
        self.commit_every: int = commit_every
        self.conflict_action: str = conflict_action
        self.oracle_source: str = oracle_source
        self.skip_columns: List[str] = [s.lower() for s in skip_columns]

    @classmethod
    def oracle_col_to_type(cls, c):
        if c[1] is cx_Oracle.DB_TYPE_VARCHAR:
            return "text", "{column}"
        elif c[1] is cx_Oracle.DB_TYPE_NCHAR:
            return "text", "{column}"
        if c[1] is cx_Oracle.DB_TYPE_NVARCHAR:
            return "text", "{column}"
        elif c[1] is cx_Oracle.DB_TYPE_CHAR:
            return "text", "{column}"
        elif c[1] is cx_Oracle.DB_TYPE_DATE:
            return "timestamp without time zone", "{column}"
        elif c[1] is cx_Oracle.DB_TYPE_TIMESTAMP:
            return "timestamp without time zone", "{column}"
        elif c[1] is cx_Oracle.DB_TYPE_NUMBER:
            return "numeric", "{column}"
        else:
            logging.error(f"Unknown Oracle column type: {c} {c.__class__}  type:{type(c)}  ")


    @classmethod
    def get_table_structure_oracle(cls, conn, table_name: str) -> Tuple[List[str], List[str]]:
        sql = f"select * from {table_name} FETCH FIRST 1 ROWS ONLY"  # noqa
        logging.info(sql)
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.fetchall()
        columns = [c[0] for c in cursor.description]
        coltypes = [cls.oracle_col_to_type(c) for c in cursor.description]
        return columns, coltypes

    @classmethod
    def pgsql_col_to_type(cls, c: int):
        # you can find the type code  table running  "SELECT pg_type.oid, *  FROM pg_type where pg_type.oid in (1082, 2950, 1114, 3802, 16)"
        if c == 16:  # boolean
            return "boolean", "{column}"
        elif c == 25:  # text
            return "text", "{column}"
        elif c == 1043:  # text
            return "text", "{column}"
        elif c == 20:  # int8
            return "int8", "{column}"
        elif c == 21:  # int2
            return "int2", "{column}"
        elif c == 23:  # int4
            return "int4", "{column}"
        elif c == 2950:  # uuid
            return "uuid", "cast({column} as text)"

        elif c == 1082:  # date
            return "date", "{column}"

        elif c == 1083:  # timestamp without time zone 
            return "time", "{column}"

        elif c == 1114:  # timestamp without time zone: we decided to convert to "with timezone" 
            return "timestamp with time zone", "{column}"

        elif c == 1184:  # timestamp with time zone 
            return "timestamp with time zone", "{column}"

        elif c == 1266:  # Time with time zone 
            return "time", "{column}"

        elif c == 3802:  # jsonb
            return "jsonb", "cast({column} as text) as {column}"

        elif c == 3614:  # tsvector
            return "tsvector", "{column}"

        elif c == 1700:  # numeric
            return "numeric", "{column}"
        else:
            logging.error(f"Unknown Postgres column type: {c}")
            return "text", "cast({column} as text) as {column}"


    @classmethod
    def pgsql_get_primary_key(cls, connection, table_full_name: str):
        logging.info(f"Getting PK for {table_full_name}")
        sql = QUERY_PRIMARY_KEY.format(name_filter=table_full_name)
        logging.info(sql)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(sql)
        rows = cursor.fetchall()
        logging.info(rows)
        if rows:
            s = rows[0][2]
            # s is something like "PRIMARY KEY(id)" or "PRIMARY KEY(id,id2)"
            # so we need to extract the columns:
            PRIMARY_KEY_REGEXP = re.compile(r"[A-Za-z ]* \(([A-Za-z,]+)\)", re.UNICODE)
            m = PRIMARY_KEY_REGEXP.match(s)
            if m is None:
                return []
            return m.groups()[0].split(",")
        return []

    def get_table_structure_pgsql(self, connection, table_name):
        sql = f"select * from {table_name} limit 1"  # noqa
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.fetchone()
        columns = [c.name for c in cursor.description]
        coltypes = [self.pgsql_col_to_type(c.type_code) for c in cursor.description]
        return columns, coltypes

    def check_table_exists(self, conn, table_name):
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT 1 from {table_name}")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            cursor.close()
            conn.rollback()
            return False

    def create_table_query(self, table, columns, coltypes, primary_keys):
        coltypes_create = [x[0] for x in coltypes]
        s = [f"CREATE UNLOGGED TABLE {table} ("]
        s.append(",\n".join(f"\t{c} {t}" for c, t in zip(columns, coltypes_create)))
        if primary_keys:
            pk_sql = ",".join(primary_keys)
            s.append(f", primary key ({pk_sql})")
        s.append(");\n\n")
        return "\n".join(s)

    def drop_table_query(self, table):
        return f"DROP TABLE {table};"

    def build_sql_conflic(self, columns, pk_fields):
        """ builds the 'ON CONFLICT DO UPDATE 'part of sql """
        update_fields = [
            f"{field} = EXCLUDED.{field}" for field in columns if field not in pk_fields
        ]
        if len(update_fields) == 0:
            return ""
        set_fields_str = ", ".join(update_fields)
        pk_fields_str = ", ".join(pk_fields)
        sql = f"ON CONFLICT ({pk_fields_str}) DO UPDATE SET {set_fields_str}"
        return sql

    def build_insert_sql(self, columns, len_values: int, pk_fields):
        """ builds the 'INSERT INTO table (cols) VALUES (a,b),(c,d)' part of sql """
        sql = [f"INSERT INTO {self.destination_table} ("]
        sql.append(",".join(columns))
        sql.append(") values ")
        values_placeholder = "( {} ) ".format(",".join(["%s"] * len(columns)))
        sql.append(",".join([values_placeholder] * len_values))
        if pk_fields:
            sql.append(self.build_sql_conflic(columns, pk_fields))
        return " ".join(sql)

    def send_data_to_db(self, connection, sql: str, rows):
        """ Sends the query and the values to the database """
        logging.info(
            f"Inserting {len(rows):,} rows into conn {self.destination_conn_id}, table: {self.destination_table}"
        )
        try:
            cursor = connection.cursor()
            values = [value for row in rows for value in row]
            cursor.execute(sql, values)
            connection.commit()
        except Exception as e:
            connection.rollback()
            print(e)
            logging.error(e)
            raise

    def execute_db(self, connection, sql):
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            connection.commit()
        except Exception as e:
            connection.rollback()
            print(e)
            logging.error(e)

    def build_select_from_source_query(
        self, table_name: str, columns: List[str], coltypes: List[str], filter_update: bool, context
    ):
        coltypes_select = [ct[1].format(column=cn) for ct, cn in zip(coltypes, columns)]
        s = ["SELECT"]
        s.append(f", ".join(coltypes_select))
        s.append("FROM")
        s.append(f"\t{table_name}")

        if filter_update:
            s.append("WHERE")
            s.append("updated_at <= '{next_execution_date}' :: TIMESTAMP".format(**context))
            s.append("AND updated_at >= '{execution_date}' :: TIMESTAMP".format(**context))
        return "\n".join(s)

    def execute(self, context):
        source_hook = BaseHook.get_hook(self.source_conn_id)
        source_conn = source_hook.get_conn()
        destination_hook = BaseHook.get_hook(self.destination_conn_id)
        destination_conn = destination_hook.get_conn()

        if self.preoperator:
            logging.info("Running preoperator")
            logging.info(self.preoperator)
            destination_hook.run(self.preoperator)

        logging.info(
            f"Smart transfer {self.source_table} ({self.source_conn_id}) to table {self.destination_table} ({self.destination_conn_id})"
        )

        # Retrieves source table structure
        if self.oracle_source:
            source_columns, source_coltypes = self.get_table_structure_oracle(
                source_conn, self.source_table
            )
        else:
            source_columns, source_coltypes = self.get_table_structure_pgsql(
                source_conn, self.source_table
            )

        for c in zip(source_columns, source_coltypes):
            logging.info(f"Found source column: {c[0]:<50} type {c[1]}")

        # remove columns in skip_columns
        source_columns, source_coltypes = zip(*[(c,t) for c,t in zip(source_columns, source_coltypes) if c.lower() not in self.skip_columns])
    
        # read primary keys
        if self.ignore_pk:
            logging.info(f"Ignoring primary keys")
            source_primary_keys = []
        else:
            source_primary_keys = self.pgsql_get_primary_key(source_conn, self.source_table)
        logging.info(f"Primary keys source: {source_primary_keys}")


        create_table_sql = self.create_table_query(
            self.destination_table, source_columns, source_coltypes, source_primary_keys
        )
        drop_table_sql = self.drop_table_query(self.destination_table)
        table_exists = self.check_table_exists(destination_conn, self.destination_table)
        need_full_import = False
        if table_exists:
            logging.info(f"Table {self.destination_table} already exists")
            destination_columns, destination_coltypes = self.get_table_structure_pgsql(
                destination_conn, self.destination_table
            )

            # remove columns in skip_columns
            destination_columns, destination_coltypes = zip(*[(c,t) for c,t in zip(destination_columns, destination_coltypes) if c.lower() not in self.skip_columns])


            for c in zip(destination_columns, destination_coltypes):
                logging.info(f"Found destination column: {c[0]:<50} type {c[1]}")

            if self.ignore_pk:
                destination_primary_keys = []
                logging.info(f"Ignoring primary keys")
            else:
                destination_primary_keys = self.pgsql_get_primary_key(
                    destination_conn, self.destination_table
                )
            logging.info(f"Primary keys destiny: {destination_primary_keys}")

            source_dict = dict(zip(source_columns, source_coltypes))
            destination_dict = dict(zip(destination_columns, destination_coltypes))
            if source_dict == destination_dict and source_primary_keys == destination_primary_keys:
                logging.info(f"Both tables have the same structure!")
            else:
                logging.info(f"Tables have different structures")
                logging.info(f"Dropping old table: {drop_table_sql}")
                self.execute_db(destination_conn, drop_table_sql)
                logging.info(f"Recreating: {create_table_sql}")
                self.execute_db(destination_conn, create_table_sql)
                need_full_import = True
        else:
            logging.info(f"Table doesn't exists, creating: {create_table_sql}")
            self.execute_db(destination_conn, create_table_sql)
            need_full_import = True

        if need_full_import:
            logging.info(f"Destination table has changed, a full import is needed")

        select_from_source_query = self.build_select_from_source_query(
            self.source_table,
            source_columns,
            source_coltypes,
            self.updated_at_filter and (not need_full_import),  # todo: explain logic
            context,
        )
        logging.info(f"Select query: {select_from_source_query}")
        logging.info(f"Commit every: {self.commit_every}")

        destination_hook = BaseHook.get_hook(self.destination_conn_id)
        destination_conn = destination_hook.get_conn()

        ### Do the transfer
        source_cursor = source_conn.cursor()
        source_cursor.execute(select_from_source_query)

        if isinstance(source_cursor, cx_Oracle.Cursor):
            logging.info(f"Oracle source connection detected.")

        rows_transferred = 0
        for rows in grouper(source_cursor, self.commit_every):
            if not rows:
                logging.info(f"no more rows to transfer!")
                break
            rows_transferred += len(rows)
            logging.info(f"Going to transfer {len(rows):,} rows...")
            sql = self.build_insert_sql(source_columns, len(rows), source_primary_keys)
            logging.info("SQL Header: {}".format(sql[:150]))
            logging.info("SQL Tail: {}".format(sql[-150:]))
            self.send_data_to_db(destination_conn, sql, rows)

        logging.info(f"Transfer completed! {rows_transferred:,} rows had been transferred!")
        return rows_transferred

