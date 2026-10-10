"""
Script de verificación y consulta de la base de datos MySQL.
Ejecuta 'SELECT * FROM historial_ventas;' y muestra los datos limpios y cargados correctamente.
"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import mysql.connector
from config.db_config import DatabaseConfig


def verify_database() -> None:
    """Consulta y presenta los datos de la tabla historial_ventas."""
    config = DatabaseConfig()
    print("=" * 115)
    print(f"  CONSULTA DE COMPROBACIÓN: BASE DE DATOS '{config.database}' | TABLA 'historial_ventas'")
    print("=" * 115)

    try:
        conn = mysql.connector.connect(**config.get_db_connection_params())
        cursor = conn.cursor(dictionary=True)

        # 1. Total de registros en la tabla
        cursor.execute("SELECT COUNT(*) AS total FROM historial_ventas;")
        total = cursor.fetchone()["total"]
        print(f"\n[+] Total de registros consolidados en 'historial_ventas': {total}")

        # 2. Comprobación de integridad (verificar que no hay anomalías)
        cursor.execute("SELECT COUNT(*) AS anomalos FROM historial_ventas WHERE cantidad <= 0 OR precio_unitario <= 0;")
        anomalos = cursor.fetchone()["anomalos"]
        print(f"[+] Registros con cantidad o precio <= 0: {anomalos} (Correcto: 0)")

        # 3. Consulta de los registros: SELECT * FROM historial_ventas;
        cursor.execute("SELECT * FROM historial_ventas ORDER BY id_transaccion ASC;")
        rows = cursor.fetchall()

        print("\n" + "-" * 115)
        print("EJECUTANDO: SELECT * FROM historial_ventas;")
        print("-" * 115)
        header = f"{'ID':<6} | {'PRODUCTO':<30} | {'CANT':<6} | {'PRECIO (S/)':<12} | {'TOTAL (S/)':<12} | {'FECHA':<12} | {'CORREO'}"
        print(header)
        print("-" * 115)

        for row in rows:
            fecha_str = str(row['fecha_registro'])
            print(
                f"{row['id_transaccion']:<6} | "
                f"{row['producto']:<30} | "
                f"{row['cantidad']:<6} | "
                f"{float(row['precio_unitario']):<12.2f} | "
                f"{float(row['total_venta']):<12.2f} | "
                f"{fecha_str:<12} | "
                f"{row['correo_empleado']}"
            )

        print("-" * 115)

        # 4. Estadísticas adicionales
        cursor.execute("SELECT SUM(total_venta) AS gran_total, AVG(total_venta) AS promedio FROM historial_ventas;")
        stats = cursor.fetchone()
        print(f"\n[+] Monto Total de Ventas: S/ {stats['gran_total']:,.2f}")
        print(f"[+] Promedio por Transacción: S/ {stats['promedio']:,.2f}")
        print("=" * 115 + "\n")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[!] Error al consultar la base de datos: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    verify_database()
