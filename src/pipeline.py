"""
Orquestador del proceso ETL (Fases 1, 2 y 3).
Clase ETLPipeline que coordina la extracción, transformación y carga.
"""
import time
from typing import Dict, Any, Optional

from src.extractor import CSVExtractor, ExtractionError
from src.transformer import DataTransformer, TransformationError
from src.loader import MySQLLoader, LoadError
from config.db_config import DatabaseConfig


class ETLPipeline:
    """
    Clase orquestadora del proceso ETL.
    Coordina secuencialmente la extracción del archivo CSV, la transformación
    y limpieza de los datos, y la carga transaccional hacia MySQL.
    """

    def __init__(self, file_path: str, db_config: Optional[DatabaseConfig] = None) -> None:
        """
        Inicializa el pipeline con sus respectivos componentes orientados a objetos.
        :param file_path: Ruta al archivo fuente .csv.
        :param db_config: Instancia opcional de DatabaseConfig.
        """
        self.file_path = file_path
        self.db_config = db_config or DatabaseConfig()

        self.extractor = CSVExtractor(file_path=self.file_path)
        self.transformer = DataTransformer()
        self.loader = MySQLLoader(config=self.db_config)

    def run(self) -> Dict[str, Any]:
        """
        Ejecuta el ciclo de vida completo del proceso ETL.
        Retorna un resumen estructurado de la ejecución.
        """
        start_time = time.time()
        print("=" * 70)
        print("          PROCESO ETL - ALMACÉN Y GESTIÓN DE VENTAS")
        print("=" * 70)
        print(f"Archivo fuente: {self.file_path}")
        print(f"Base de datos destino: {self.db_config.database} en {self.db_config.host}:{self.db_config.port}")
        print("-" * 70)

        # -------------------------------------------------------------
        # FASE 1: EXTRACCIÓN (EXTRACT)
        # -------------------------------------------------------------
        print("\n[FASE 1: EXTRACCIÓN] Iniciando lectura de archivo fuente...")
        try:
            raw_records = self.extractor.extract()
            print(f" -> Éxito: Se extrajeron correctamente {len(raw_records)} registros del archivo CSV.")
        except ExtractionError as err:
            print(f" [!] ERROR EN FASE 1 (Extracción): {err}")
            raise

        # -------------------------------------------------------------
        # FASE 2: TRANSFORMACIÓN (TRANSFORM)
        # -------------------------------------------------------------
        print("\n[FASE 2: TRANSFORMACIÓN] Aplicando reglas de depuración y normalización...")
        try:
            clean_records, stats = self.transformer.transform(raw_records)
            print(f" -> Registros evaluados:    {stats['total_recibidos']}")
            print(f" -> Registros descartados:   {stats['total_descartados']}")
            if stats['total_descartados'] > 0:
                print(f"     * Nulos/vacíos (NaN):  {stats['descartes_nulos']}")
                print(f"     * Cantidad inválida:   {stats['descartes_cantidad']}")
                print(f"     * Precio inválido:     {stats['descartes_precio']}")
                print(f"     * Fechas inválidas:    {stats['descartes_fechas']}")
                print(f"     * Duplicados:          {stats['descartes_duplicados']}")
            print(f" -> Registros limpios y válidos: {stats['total_validos']}")
        except TransformationError as err:
            print(f" [!] ERROR EN FASE 2 (Transformación): {err}")
            raise

        # -------------------------------------------------------------
        # FASE 3: CARGA (LOAD)
        # -------------------------------------------------------------
        print("\n[FASE 3: CARGA] Conectando a MySQL y ejecutando carga masiva...")
        try:
            loaded_count = self.loader.load(clean_records)
            print(f" -> Éxito: Se cargaron transaccionalmente {loaded_count} registros en la tabla 'historial_ventas'.")
            print(" -> Transacción confirmada (COMMIT).")
        except LoadError as err:
            print(f" [!] ERROR EN FASE 3 (Carga): {err}")
            raise

        # -------------------------------------------------------------
        # RESUMEN FINAL
        # -------------------------------------------------------------
        elapsed_time = round(time.time() - start_time, 2)
        print("\n" + "=" * 70)
        print("          RESUMEN DE EJECUCIÓN DEL PROCESO ETL")
        print("=" * 70)
        print(f" Total registros leídos:       {len(raw_records)}")
        print(f" Total registros depurados:     {stats['total_descartados']}")
        print(f" Total registros consolidados:  {loaded_count}")
        print(f" Tiempo total de ejecución:     {elapsed_time} segundos")
        print(f" Estado:                        EXITOSO")
        print("=" * 70 + "\n")

        return {
            "status": "SUCCESS",
            "extracted": len(raw_records),
            "transformed": stats["total_validos"],
            "discarded": stats["total_descartados"],
            "loaded": loaded_count,
            "elapsed_seconds": elapsed_time
        }
