import csv
import io
import logging
import os
from datetime import datetime, timezone

import azure.functions as func
import pyodbc

app = func.FunctionApp()

# Rotina diária de carga: lê as tabelas do banco do professor (origem)
# e grava no nosso banco (destino).
# As tabelas "pai" rodam antes das "filhas" por causa das chaves estrangeiras.
# Horário em UTC (03:00 UTC = meia-noite em Brasília).


@app.timer_trigger(
    schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_fila(myTimer: func.TimerRequest) -> None:
    logging.info("iniciando carga da tabela fila")

    # credenciais da origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # credenciais do destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")
    dest_schema = os.getenv("DEST_SCHEMA", "itsm")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # 1) busca os dados na origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("select * from itsm.fila")

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f"{len(rows)} linhas lidas de itsm.fila")

        # 2) cópia em CSV no Data Lake, só se a connection string existir
        datalake_cs = os.getenv("DATALAKE_CONNECTION_STRING")
        if datalake_cs:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient

                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(columns)
                writer.writerows(rows)

                agora = datetime.now(timezone.utc)
                pasta = f"{agora:%Y/%m/%d}"
                arquivo = f"fila_{agora:%Y%m%d_%H%M%S}.csv"
                file_path = f"fila/{pasta}/{arquivo}"

                datalake = DataLakeServiceClient.from_connection_string(datalake_cs)
                file_client = datalake.get_file_system_client("bronze").get_file_client(
                    file_path
                )
                file_client.upload_data(
                    buffer.getvalue().encode("utf-8"), overwrite=True
                )

                logging.info(f"CSV salvo em bronze/{file_path}")
            except Exception as e:
                logging.warning(f"Falha ao salvar no Data Lake: {e}")
        else:
            logging.info("Sem DATALAKE_CONNECTION_STRING, etapa do Data Lake ignorada")

        # 3) grava no destino com MERGE (atualiza ou insere, nunca apaga)
        pk = "id_fila"
        tabela_dest = f"{dest_schema}.fila"
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        set_list = ", ".join(f"t.[{c}] = s.[{c}]" for c in columns if c != pk)
        ins_list = ", ".join(f"s.[{c}]" for c in columns)
        merge_sql = (
            f"MERGE {tabela_dest} AS t "
            f"USING (VALUES ({placeholders})) AS s ({col_list}) "
            f"ON t.[{pk}] = s.[{pk}] "
            f"WHEN MATCHED THEN UPDATE SET {set_list} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({ins_list});"
        )

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            if rows:
                # a coluna id é IDENTITY, então liberamos a inserção do id original
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} ON")
                dest_cursor.fast_executemany = False
                dest_cursor.executemany(merge_sql, [tuple(r) for r in rows])
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} OFF")

            dest_conn.commit()
            logging.info(f"{len(rows)} linhas gravadas em {tabela_dest}")
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro na carga de itsm.fila: {str(e)}")
        raise


@app.timer_trigger(
    schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_categoria(myTimer: func.TimerRequest) -> None:
    logging.info("iniciando carga da tabela categoria")

    # credenciais da origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # credenciais do destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")
    dest_schema = os.getenv("DEST_SCHEMA", "itsm")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # 1) busca os dados na origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("select * from itsm.categoria")

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f"{len(rows)} linhas lidas de itsm.categoria")

        # 2) cópia em CSV no Data Lake, só se a connection string existir
        datalake_cs = os.getenv("DATALAKE_CONNECTION_STRING")
        if datalake_cs:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient

                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(columns)
                writer.writerows(rows)

                agora = datetime.now(timezone.utc)
                pasta = f"{agora:%Y/%m/%d}"
                arquivo = f"categoria_{agora:%Y%m%d_%H%M%S}.csv"
                file_path = f"categoria/{pasta}/{arquivo}"

                datalake = DataLakeServiceClient.from_connection_string(datalake_cs)
                file_client = datalake.get_file_system_client("bronze").get_file_client(
                    file_path
                )
                file_client.upload_data(
                    buffer.getvalue().encode("utf-8"), overwrite=True
                )

                logging.info(f"CSV salvo em bronze/{file_path}")
            except Exception as e:
                logging.warning(f"Falha ao salvar no Data Lake: {e}")
        else:
            logging.info("Sem DATALAKE_CONNECTION_STRING, etapa do Data Lake ignorada")

        # 3) grava no destino com MERGE (atualiza ou insere, nunca apaga)
        pk = "id_categoria"
        tabela_dest = f"{dest_schema}.categoria"
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        set_list = ", ".join(f"t.[{c}] = s.[{c}]" for c in columns if c != pk)
        ins_list = ", ".join(f"s.[{c}]" for c in columns)
        merge_sql = (
            f"MERGE {tabela_dest} AS t "
            f"USING (VALUES ({placeholders})) AS s ({col_list}) "
            f"ON t.[{pk}] = s.[{pk}] "
            f"WHEN MATCHED THEN UPDATE SET {set_list} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({ins_list});"
        )

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            if rows:
                # a coluna id é IDENTITY, então liberamos a inserção do id original
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} ON")
                dest_cursor.fast_executemany = False
                dest_cursor.executemany(merge_sql, [tuple(r) for r in rows])
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} OFF")

            dest_conn.commit()
            logging.info(f"{len(rows)} linhas gravadas em {tabela_dest}")
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro na carga de itsm.categoria: {str(e)}")
        raise


@app.timer_trigger(
    schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    logging.info("iniciando carga da tabela cliente_organizacao")

    # credenciais da origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # credenciais do destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")
    dest_schema = os.getenv("DEST_SCHEMA", "itsm")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # 1) busca os dados na origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("select * from itsm.cliente_organizacao")

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f"{len(rows)} linhas lidas de itsm.cliente_organizacao")

        # 2) cópia em CSV no Data Lake, só se a connection string existir
        datalake_cs = os.getenv("DATALAKE_CONNECTION_STRING")
        if datalake_cs:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient

                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(columns)
                writer.writerows(rows)

                agora = datetime.now(timezone.utc)
                pasta = f"{agora:%Y/%m/%d}"
                arquivo = f"cliente_organizacao_{agora:%Y%m%d_%H%M%S}.csv"
                file_path = f"cliente_organizacao/{pasta}/{arquivo}"

                datalake = DataLakeServiceClient.from_connection_string(datalake_cs)
                file_client = datalake.get_file_system_client("bronze").get_file_client(
                    file_path
                )
                file_client.upload_data(
                    buffer.getvalue().encode("utf-8"), overwrite=True
                )

                logging.info(f"CSV salvo em bronze/{file_path}")
            except Exception as e:
                logging.warning(f"Falha ao salvar no Data Lake: {e}")
        else:
            logging.info("Sem DATALAKE_CONNECTION_STRING, etapa do Data Lake ignorada")

        # 3) grava no destino com MERGE (atualiza ou insere, nunca apaga)
        pk = "id_cliente_organizacao"
        tabela_dest = f"{dest_schema}.cliente_organizacao"
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        set_list = ", ".join(f"t.[{c}] = s.[{c}]" for c in columns if c != pk)
        ins_list = ", ".join(f"s.[{c}]" for c in columns)
        merge_sql = (
            f"MERGE {tabela_dest} AS t "
            f"USING (VALUES ({placeholders})) AS s ({col_list}) "
            f"ON t.[{pk}] = s.[{pk}] "
            f"WHEN MATCHED THEN UPDATE SET {set_list} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({ins_list});"
        )

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            if rows:
                # a coluna id é IDENTITY, então liberamos a inserção do id original
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} ON")
                dest_cursor.fast_executemany = False
                dest_cursor.executemany(merge_sql, [tuple(r) for r in rows])
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} OFF")

            dest_conn.commit()
            logging.info(f"{len(rows)} linhas gravadas em {tabela_dest}")
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro na carga de itsm.cliente_organizacao: {str(e)}")
        raise


@app.timer_trigger(
    schedule="0 0 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_sla(myTimer: func.TimerRequest) -> None:
    logging.info("iniciando carga da tabela sla")

    # credenciais da origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # credenciais do destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")
    dest_schema = os.getenv("DEST_SCHEMA", "itsm")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # 1) busca os dados na origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("select * from itsm.sla")

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f"{len(rows)} linhas lidas de itsm.sla")

        # 2) cópia em CSV no Data Lake, só se a connection string existir
        datalake_cs = os.getenv("DATALAKE_CONNECTION_STRING")
        if datalake_cs:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient

                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(columns)
                writer.writerows(rows)

                agora = datetime.now(timezone.utc)
                pasta = f"{agora:%Y/%m/%d}"
                arquivo = f"sla_{agora:%Y%m%d_%H%M%S}.csv"
                file_path = f"sla/{pasta}/{arquivo}"

                datalake = DataLakeServiceClient.from_connection_string(datalake_cs)
                file_client = datalake.get_file_system_client("bronze").get_file_client(
                    file_path
                )
                file_client.upload_data(
                    buffer.getvalue().encode("utf-8"), overwrite=True
                )

                logging.info(f"CSV salvo em bronze/{file_path}")
            except Exception as e:
                logging.warning(f"Falha ao salvar no Data Lake: {e}")
        else:
            logging.info("Sem DATALAKE_CONNECTION_STRING, etapa do Data Lake ignorada")

        # 3) grava no destino com MERGE (atualiza ou insere, nunca apaga)
        pk = "id_sla"
        tabela_dest = f"{dest_schema}.sla"
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        set_list = ", ".join(f"t.[{c}] = s.[{c}]" for c in columns if c != pk)
        ins_list = ", ".join(f"s.[{c}]" for c in columns)
        merge_sql = (
            f"MERGE {tabela_dest} AS t "
            f"USING (VALUES ({placeholders})) AS s ({col_list}) "
            f"ON t.[{pk}] = s.[{pk}] "
            f"WHEN MATCHED THEN UPDATE SET {set_list} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({ins_list});"
        )

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            if rows:
                # a coluna id é IDENTITY, então liberamos a inserção do id original
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} ON")
                dest_cursor.fast_executemany = False
                dest_cursor.executemany(merge_sql, [tuple(r) for r in rows])
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} OFF")

            dest_conn.commit()
            logging.info(f"{len(rows)} linhas gravadas em {tabela_dest}")
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro na carga de itsm.sla: {str(e)}")
        raise


@app.timer_trigger(
    schedule="0 10 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_analista(myTimer: func.TimerRequest) -> None:
    logging.info("iniciando carga da tabela analista")

    # credenciais da origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # credenciais do destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")
    dest_schema = os.getenv("DEST_SCHEMA", "itsm")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # 1) busca os dados na origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("select * from itsm.analista")

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f"{len(rows)} linhas lidas de itsm.analista")

        # 2) cópia em CSV no Data Lake, só se a connection string existir
        datalake_cs = os.getenv("DATALAKE_CONNECTION_STRING")
        if datalake_cs:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient

                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(columns)
                writer.writerows(rows)

                agora = datetime.now(timezone.utc)
                pasta = f"{agora:%Y/%m/%d}"
                arquivo = f"analista_{agora:%Y%m%d_%H%M%S}.csv"
                file_path = f"analista/{pasta}/{arquivo}"

                datalake = DataLakeServiceClient.from_connection_string(datalake_cs)
                file_client = datalake.get_file_system_client("bronze").get_file_client(
                    file_path
                )
                file_client.upload_data(
                    buffer.getvalue().encode("utf-8"), overwrite=True
                )

                logging.info(f"CSV salvo em bronze/{file_path}")
            except Exception as e:
                logging.warning(f"Falha ao salvar no Data Lake: {e}")
        else:
            logging.info("Sem DATALAKE_CONNECTION_STRING, etapa do Data Lake ignorada")

        # 3) grava no destino com MERGE (atualiza ou insere, nunca apaga)
        pk = "id_analista"
        tabela_dest = f"{dest_schema}.analista"
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        set_list = ", ".join(f"t.[{c}] = s.[{c}]" for c in columns if c != pk)
        ins_list = ", ".join(f"s.[{c}]" for c in columns)
        merge_sql = (
            f"MERGE {tabela_dest} AS t "
            f"USING (VALUES ({placeholders})) AS s ({col_list}) "
            f"ON t.[{pk}] = s.[{pk}] "
            f"WHEN MATCHED THEN UPDATE SET {set_list} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({ins_list});"
        )

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            if rows:
                # a coluna id é IDENTITY, então liberamos a inserção do id original
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} ON")
                dest_cursor.fast_executemany = False
                dest_cursor.executemany(merge_sql, [tuple(r) for r in rows])
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} OFF")

            dest_conn.commit()
            logging.info(f"{len(rows)} linhas gravadas em {tabela_dest}")
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro na carga de itsm.analista: {str(e)}")
        raise


@app.timer_trigger(
    schedule="0 10 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_solicitante(myTimer: func.TimerRequest) -> None:
    logging.info("iniciando carga da tabela solicitante")

    # credenciais da origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # credenciais do destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")
    dest_schema = os.getenv("DEST_SCHEMA", "itsm")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # 1) busca os dados na origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("select * from itsm.solicitante")

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f"{len(rows)} linhas lidas de itsm.solicitante")

        # 2) cópia em CSV no Data Lake, só se a connection string existir
        datalake_cs = os.getenv("DATALAKE_CONNECTION_STRING")
        if datalake_cs:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient

                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(columns)
                writer.writerows(rows)

                agora = datetime.now(timezone.utc)
                pasta = f"{agora:%Y/%m/%d}"
                arquivo = f"solicitante_{agora:%Y%m%d_%H%M%S}.csv"
                file_path = f"solicitante/{pasta}/{arquivo}"

                datalake = DataLakeServiceClient.from_connection_string(datalake_cs)
                file_client = datalake.get_file_system_client("bronze").get_file_client(
                    file_path
                )
                file_client.upload_data(
                    buffer.getvalue().encode("utf-8"), overwrite=True
                )

                logging.info(f"CSV salvo em bronze/{file_path}")
            except Exception as e:
                logging.warning(f"Falha ao salvar no Data Lake: {e}")
        else:
            logging.info("Sem DATALAKE_CONNECTION_STRING, etapa do Data Lake ignorada")

        # 3) grava no destino com MERGE (atualiza ou insere, nunca apaga)
        pk = "id_solicitante"
        tabela_dest = f"{dest_schema}.solicitante"
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        set_list = ", ".join(f"t.[{c}] = s.[{c}]" for c in columns if c != pk)
        ins_list = ", ".join(f"s.[{c}]" for c in columns)
        merge_sql = (
            f"MERGE {tabela_dest} AS t "
            f"USING (VALUES ({placeholders})) AS s ({col_list}) "
            f"ON t.[{pk}] = s.[{pk}] "
            f"WHEN MATCHED THEN UPDATE SET {set_list} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({ins_list});"
        )

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            if rows:
                # a coluna id é IDENTITY, então liberamos a inserção do id original
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} ON")
                dest_cursor.fast_executemany = False
                dest_cursor.executemany(merge_sql, [tuple(r) for r in rows])
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} OFF")

            dest_conn.commit()
            logging.info(f"{len(rows)} linhas gravadas em {tabela_dest}")
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro na carga de itsm.solicitante: {str(e)}")
        raise


@app.timer_trigger(
    schedule="0 20 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado(myTimer: func.TimerRequest) -> None:
    logging.info("iniciando carga da tabela chamado")

    # credenciais da origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # credenciais do destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")
    dest_schema = os.getenv("DEST_SCHEMA", "itsm")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # 1) busca os dados na origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("select * from itsm.chamado")

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f"{len(rows)} linhas lidas de itsm.chamado")

        # 2) cópia em CSV no Data Lake, só se a connection string existir
        datalake_cs = os.getenv("DATALAKE_CONNECTION_STRING")
        if datalake_cs:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient

                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(columns)
                writer.writerows(rows)

                agora = datetime.now(timezone.utc)
                pasta = f"{agora:%Y/%m/%d}"
                arquivo = f"chamado_{agora:%Y%m%d_%H%M%S}.csv"
                file_path = f"chamado/{pasta}/{arquivo}"

                datalake = DataLakeServiceClient.from_connection_string(datalake_cs)
                file_client = datalake.get_file_system_client("bronze").get_file_client(
                    file_path
                )
                file_client.upload_data(
                    buffer.getvalue().encode("utf-8"), overwrite=True
                )

                logging.info(f"CSV salvo em bronze/{file_path}")
            except Exception as e:
                logging.warning(f"Falha ao salvar no Data Lake: {e}")
        else:
            logging.info("Sem DATALAKE_CONNECTION_STRING, etapa do Data Lake ignorada")

        # 3) grava no destino com MERGE (atualiza ou insere, nunca apaga)
        pk = "id_chamado"
        tabela_dest = f"{dest_schema}.chamado"
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        set_list = ", ".join(f"t.[{c}] = s.[{c}]" for c in columns if c != pk)
        ins_list = ", ".join(f"s.[{c}]" for c in columns)
        merge_sql = (
            f"MERGE {tabela_dest} AS t "
            f"USING (VALUES ({placeholders})) AS s ({col_list}) "
            f"ON t.[{pk}] = s.[{pk}] "
            f"WHEN MATCHED THEN UPDATE SET {set_list} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({ins_list});"
        )

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            if rows:
                # a coluna id é IDENTITY, então liberamos a inserção do id original
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} ON")
                dest_cursor.fast_executemany = False
                dest_cursor.executemany(merge_sql, [tuple(r) for r in rows])
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} OFF")

            dest_conn.commit()
            logging.info(f"{len(rows)} linhas gravadas em {tabela_dest}")
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro na carga de itsm.chamado: {str(e)}")
        raise


@app.timer_trigger(
    schedule="0 30 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:
    logging.info("iniciando carga da tabela chamado_sla")

    # credenciais da origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # credenciais do destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")
    dest_schema = os.getenv("DEST_SCHEMA", "itsm")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # 1) busca os dados na origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("select * from itsm.chamado_sla")

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f"{len(rows)} linhas lidas de itsm.chamado_sla")

        # 2) cópia em CSV no Data Lake, só se a connection string existir
        datalake_cs = os.getenv("DATALAKE_CONNECTION_STRING")
        if datalake_cs:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient

                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(columns)
                writer.writerows(rows)

                agora = datetime.now(timezone.utc)
                pasta = f"{agora:%Y/%m/%d}"
                arquivo = f"chamado_sla_{agora:%Y%m%d_%H%M%S}.csv"
                file_path = f"chamado_sla/{pasta}/{arquivo}"

                datalake = DataLakeServiceClient.from_connection_string(datalake_cs)
                file_client = datalake.get_file_system_client("bronze").get_file_client(
                    file_path
                )
                file_client.upload_data(
                    buffer.getvalue().encode("utf-8"), overwrite=True
                )

                logging.info(f"CSV salvo em bronze/{file_path}")
            except Exception as e:
                logging.warning(f"Falha ao salvar no Data Lake: {e}")
        else:
            logging.info("Sem DATALAKE_CONNECTION_STRING, etapa do Data Lake ignorada")

        # 3) grava no destino com MERGE (atualiza ou insere, nunca apaga)
        pk = "id_chamado_sla"
        tabela_dest = f"{dest_schema}.chamado_sla"
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        set_list = ", ".join(f"t.[{c}] = s.[{c}]" for c in columns if c != pk)
        ins_list = ", ".join(f"s.[{c}]" for c in columns)
        merge_sql = (
            f"MERGE {tabela_dest} AS t "
            f"USING (VALUES ({placeholders})) AS s ({col_list}) "
            f"ON t.[{pk}] = s.[{pk}] "
            f"WHEN MATCHED THEN UPDATE SET {set_list} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({ins_list});"
        )

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            if rows:
                # a coluna id é IDENTITY, então liberamos a inserção do id original
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} ON")
                dest_cursor.fast_executemany = False
                dest_cursor.executemany(merge_sql, [tuple(r) for r in rows])
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} OFF")

            dest_conn.commit()
            logging.info(f"{len(rows)} linhas gravadas em {tabela_dest}")
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro na carga de itsm.chamado_sla: {str(e)}")
        raise


@app.timer_trigger(
    schedule="0 30 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    logging.info("iniciando carga da tabela chamado_status_historico")

    # credenciais da origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # credenciais do destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")
    dest_schema = os.getenv("DEST_SCHEMA", "itsm")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # 1) busca os dados na origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("select * from itsm.chamado_status_historico")

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f"{len(rows)} linhas lidas de itsm.chamado_status_historico")

        # 2) cópia em CSV no Data Lake, só se a connection string existir
        datalake_cs = os.getenv("DATALAKE_CONNECTION_STRING")
        if datalake_cs:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient

                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(columns)
                writer.writerows(rows)

                agora = datetime.now(timezone.utc)
                pasta = f"{agora:%Y/%m/%d}"
                arquivo = f"chamado_status_historico_{agora:%Y%m%d_%H%M%S}.csv"
                file_path = f"chamado_status_historico/{pasta}/{arquivo}"

                datalake = DataLakeServiceClient.from_connection_string(datalake_cs)
                file_client = datalake.get_file_system_client("bronze").get_file_client(
                    file_path
                )
                file_client.upload_data(
                    buffer.getvalue().encode("utf-8"), overwrite=True
                )

                logging.info(f"CSV salvo em bronze/{file_path}")
            except Exception as e:
                logging.warning(f"Falha ao salvar no Data Lake: {e}")
        else:
            logging.info("Sem DATALAKE_CONNECTION_STRING, etapa do Data Lake ignorada")

        # 3) grava no destino com MERGE (atualiza ou insere, nunca apaga)
        pk = "id_chamado_status_historico"
        tabela_dest = f"{dest_schema}.chamado_status_historico"
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        set_list = ", ".join(f"t.[{c}] = s.[{c}]" for c in columns if c != pk)
        ins_list = ", ".join(f"s.[{c}]" for c in columns)
        merge_sql = (
            f"MERGE {tabela_dest} AS t "
            f"USING (VALUES ({placeholders})) AS s ({col_list}) "
            f"ON t.[{pk}] = s.[{pk}] "
            f"WHEN MATCHED THEN UPDATE SET {set_list} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({ins_list});"
        )

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            if rows:
                # a coluna id é IDENTITY, então liberamos a inserção do id original
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} ON")
                dest_cursor.fast_executemany = False
                dest_cursor.executemany(merge_sql, [tuple(r) for r in rows])
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} OFF")

            dest_conn.commit()
            logging.info(f"{len(rows)} linhas gravadas em {tabela_dest}")
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro na carga de itsm.chamado_status_historico: {str(e)}")
        raise


@app.timer_trigger(
    schedule="0 30 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:
    logging.info("iniciando carga da tabela csat_avaliacao")

    # credenciais da origem
    sql_server = os.getenv("HOST")
    sql_database = os.getenv("DATABASE")
    sql_user = os.getenv("USER")
    sql_pass = os.getenv("PASSWORD")

    # credenciais do destino
    dest_server = os.getenv("DEST_HOST")
    dest_database = os.getenv("DEST_DATABASE")
    dest_user = os.getenv("DEST_USER")
    dest_pass = os.getenv("DEST_PASSWORD")
    dest_schema = os.getenv("DEST_SCHEMA", "itsm")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    dest_conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_server};"
        f"DATABASE={dest_database};"
        f"UID={dest_user};"
        f"PWD={dest_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        # 1) busca os dados na origem
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("select * from itsm.csat_avaliacao")

            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logging.info(f"{len(rows)} linhas lidas de itsm.csat_avaliacao")

        # 2) cópia em CSV no Data Lake, só se a connection string existir
        datalake_cs = os.getenv("DATALAKE_CONNECTION_STRING")
        if datalake_cs:
            try:
                from azure.storage.filedatalake import DataLakeServiceClient

                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(columns)
                writer.writerows(rows)

                agora = datetime.now(timezone.utc)
                pasta = f"{agora:%Y/%m/%d}"
                arquivo = f"csat_avaliacao_{agora:%Y%m%d_%H%M%S}.csv"
                file_path = f"csat_avaliacao/{pasta}/{arquivo}"

                datalake = DataLakeServiceClient.from_connection_string(datalake_cs)
                file_client = datalake.get_file_system_client("bronze").get_file_client(
                    file_path
                )
                file_client.upload_data(
                    buffer.getvalue().encode("utf-8"), overwrite=True
                )

                logging.info(f"CSV salvo em bronze/{file_path}")
            except Exception as e:
                logging.warning(f"Falha ao salvar no Data Lake: {e}")
        else:
            logging.info("Sem DATALAKE_CONNECTION_STRING, etapa do Data Lake ignorada")

        # 3) grava no destino com MERGE (atualiza ou insere, nunca apaga)
        pk = "id_csat_avaliacao"
        tabela_dest = f"{dest_schema}.csat_avaliacao"
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        set_list = ", ".join(f"t.[{c}] = s.[{c}]" for c in columns if c != pk)
        ins_list = ", ".join(f"s.[{c}]" for c in columns)
        merge_sql = (
            f"MERGE {tabela_dest} AS t "
            f"USING (VALUES ({placeholders})) AS s ({col_list}) "
            f"ON t.[{pk}] = s.[{pk}] "
            f"WHEN MATCHED THEN UPDATE SET {set_list} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({ins_list});"
        )

        dest_conn = pyodbc.connect(dest_conn_str, autocommit=False)
        try:
            dest_cursor = dest_conn.cursor()

            if rows:
                # a coluna id é IDENTITY, então liberamos a inserção do id original
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} ON")
                dest_cursor.fast_executemany = False
                dest_cursor.executemany(merge_sql, [tuple(r) for r in rows])
                dest_cursor.execute(f"SET IDENTITY_INSERT {tabela_dest} OFF")

            dest_conn.commit()
            logging.info(f"{len(rows)} linhas gravadas em {tabela_dest}")
        except Exception:
            dest_conn.rollback()
            raise
        finally:
            dest_conn.close()

    except Exception as e:
        logging.error(f"Erro na carga de itsm.csat_avaliacao: {str(e)}")
        raise
