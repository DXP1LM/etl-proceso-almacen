"""
Punto de entrada principal (Entry Point) para la ejecución del proceso ETL.
Permite ejecutar el pipeline con configuración por defecto o parámetros CLI.
"""
import sys
import argparse
from pathlib import Path

# Agregar directorio actual al sys.path para resolución de módulos
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.db_config import DatabaseConfig
from src.pipeline import ETLPipeline


def parse_arguments() -> argparse.Namespace:
    """Configura y procesa los argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Automatización de Proceso ETL para Depuración y Carga de Ventas en MySQL"
    )
    default_csv = BASE_DIR / "data" / "transacciones_almacen.csv"

    parser.add_argument(
        "--file",
        type=str,
        default=str(default_csv),
        help=f"Ruta al archivo CSV fuente (por defecto: {default_csv})"
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Host del servidor MySQL (anula variable de entorno DB_HOST)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Puerto del servidor MySQL (anula variable de entorno DB_PORT)"
    )
    parser.add_argument(
        "--user",
        type=str,
        default=None,
        help="Usuario de MySQL (anula variable de entorno DB_USER)"
    )
    parser.add_argument(
        "--password",
        type=str,
        default=None,
        help="Contraseña de MySQL (anula variable de entorno DB_PASSWORD)"
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Nombre de la base de datos MySQL (anula variable de entorno DB_NAME)"
    )
    return parser.parse_args()


def main() -> None:
    """Función principal que instancia la configuración y ejecuta el pipeline ETL."""
    args = parse_arguments()

    # Cargar configuración de base de datos
    db_config = DatabaseConfig()

    # Sobrescribir con argumentos de CLI si fueron especificados
    if args.host:
        db_config.host = args.host
    if args.port:
        db_config.port = args.port
    if args.user:
        db_config.user = args.user
    if args.password:
        db_config.password = args.password
    if args.db:
        db_config.database = args.db

    # Instanciar y ejecutar el pipeline ETL
    try:
        pipeline = ETLPipeline(file_path=args.file, db_config=db_config)
        pipeline.run()
    except Exception as exc:
        print(f"\n[ERROR FATAL] La ejecución del proceso ETL ha fallado: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
