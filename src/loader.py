"""
Fase 3: Carga (Load)
Clase MySQLLoader para la persistencia transaccional y masiva en base de datos relacional MySQL.
"""
from typing import List, Dict, Any, Tuple
import mysql.connector
from mysql.connector import Error as MySQLError

from config.db_config import DatabaseConfig


class LoadError(Exception):
    """Excepción personalizada para fallos durante la fase de carga en MySQL."""
    pass


class MySQLLoader:
    """
    Clase responsable de la Fase de Carga en el flujo ETL.
    Gestiona la conexión con MySQL, la creación de la tabla y la inserción
    masiva mediante transacciones ACID (commit / rollback).
    """

    TABLE_NAME = "historial_ventas"

    CREATE_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id_transaccion INT PRIMARY KEY,
        producto VARCHAR(150) NOT NULL,
        cantidad INT NOT NULL,
        precio_unitario DECIMAL(10, 2) NOT NULL,
        total_venta DECIMAL(12, 2) NOT NULL,
        fecha_registro DATE NOT NULL,
        correo_empleado VARCHAR(150) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    INSERT_SQL = f"""
    INSERT INTO {TABLE_NAME} (
        id_transaccion,
        producto,
        cantidad,
        precio_unitario,
        total_venta,
        fecha_registro,
        correo_empleado
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        producto = VALUES(producto),
        cantidad = VALUES(cantidad),
        precio_unitario = VALUES(precio_unitario),
        total_venta = VALUES(total_venta),
        fecha_registro = VALUES(fecha_registro),
        correo_empleado = VALUES(correo_empleado);
    """

    def __init__(self, config: DatabaseConfig = None) -> None:
        """
        Inicializa el loader con la configuración de la base de datos.
        :param config: Instancia de DatabaseConfig. Si es None, crea una por defecto.
        """
        self.config = config or DatabaseConfig()

    def ensure_database_exists(self) -> None:
        """
        Garantiza que la base de datos configurada exista en el servidor MySQL.
        Si no existe, la crea automáticamente.
        """
        server_conn = None
        cursor = None
        try:
            server_conn = mysql.connector.connect(**self.config.get_server_connection_params())
            cursor = server_conn.cursor()
            create_db_query = (
                f"CREATE DATABASE IF NOT EXISTS `{self.config.database}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            )
            cursor.execute(create_db_query)
            server_conn.commit()
        except MySQLError as err:
            raise LoadError(f"Error al verificar/crear la base de datos '{self.config.database}': {err}") from err
        finally:
            if cursor:
                cursor.close()
            if server_conn and server_conn.is_connected():
                server_conn.close()

    def get_connection(self) -> mysql.connector.MySQLConnection:
        """
        Establece y retorna una conexión activa con la base de datos.
        """
        try:
            return mysql.connector.connect(**self.config.get_db_connection_params())
        except MySQLError as err:
            raise LoadError(
                f"No se pudo conectar a la base de datos '{self.config.database}' en {self.config.host}:{self.config.port}. "
                f"Detalle: {err}"
            ) from err

    def create_table_if_not_exists(self, connection: mysql.connector.MySQLConnection) -> None:
        """
        Crea automáticamente la tabla destino 'historial_ventas' si no existe.
        """
        cursor = None
        try:
            cursor = connection.cursor()
            cursor.execute(self.CREATE_TABLE_SQL)
            connection.commit()
        except MySQLError as err:
            connection.rollback()
            raise LoadError(f"Error al crear la tabla '{self.TABLE_NAME}': {err}") from err
        finally:
            if cursor:
                cursor.close()

    def load(self, clean_records: List[Dict[str, Any]]) -> int:
        """
        Inserta los registros transformados de manera masiva utilizando cursor.executemany
        y garantizando el uso de transacciones con commit() y rollback().
        :param clean_records: Lista de diccionarios con los registros limpios.
        :return: Cantidad de registros insertados/procesados.
        """
        if not clean_records:
            return 0

        # Asegurar existencia de la base de datos
        self.ensure_database_exists()

        connection = None
        cursor = None
        try:
            connection = self.get_connection()

            # Asegurar existencia de la tabla
            self.create_table_if_not_exists(connection)

            # Iniciar transacción de carga masiva
            cursor = connection.cursor()

            # Preparar tuplas parametrizadas
            records_data: List[Tuple[Any, ...]] = [
                (
                    row["id_transaccion"],
                    row["producto"],
                    row["cantidad"],
                    row["precio_unitario"],
                    row["total_venta"],
                    row["fecha_registro"],
                    row["correo_empleado"],
                )
                for row in clean_records
            ]

            # Inserción masiva parametrizada
            cursor.executemany(self.INSERT_SQL, records_data)

            # Confirmar transacción atómica
            connection.commit()
            total_loaded = len(records_data)

            return total_loaded

        except MySQLError as err:
            # Revertir transacción en caso de fallo
            if connection and connection.is_connected():
                connection.rollback()
            raise LoadError(f"Fallo en la carga masiva hacia '{self.TABLE_NAME}'. Transacción revertida (Rollback). Error: {err}") from err

        except Exception as ex:
            if connection and connection.is_connected():
                connection.rollback()
            if isinstance(ex, LoadError):
                raise ex
            raise LoadError(f"Error inesperado en la carga de datos: {str(ex)}") from ex

        finally:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()
