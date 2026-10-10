"""
Fase 1: Extracción (Extract)
Clase CSVExtractor para la lectura y extracción segura de transacciones desde archivos CSV.
"""
import csv
import os
from pathlib import Path
from typing import List, Dict, Any


class ExtractionError(Exception):
    """Excepción personalizada para errores ocurridos durante la fase de extracción."""
    pass


class CSVExtractor:
    """
    Clase responsable de la Fase de Extracción en el flujo ETL.
    Lee y valida archivos planos .csv con manejo robusto de excepciones.
    """

    REQUIRED_COLUMNS = [
        "id_transaccion",
        "producto",
        "cantidad",
        "precio_unitario",
        "fecha_registro",
        "correo_empleado"
    ]

    def __init__(self, file_path: str) -> None:
        """
        Inicializa el extractor con la ruta del archivo fuente.
        :param file_path: Ruta absoluta o relativa al archivo CSV.
        """
        self.file_path = Path(file_path)

    def extract(self) -> List[Dict[str, Any]]:
        """
        Ejecuta la extracción de registros desde el archivo CSV.
        Maneja excepciones de archivo no encontrado, permisos y formato corrupto.
        :return: Lista de diccionarios con los registros extraídos sin procesar.
        """
        if not self.file_path.exists():
            raise ExtractionError(
                f"Error de Extracción: El archivo no fue encontrado en la ruta '{self.file_path.resolve()}'."
            )

        if not self.file_path.is_file():
            raise ExtractionError(
                f"Error de Extracción: La ruta '{self.file_path.resolve()}' no corresponde a un archivo válido."
            )

        if os.path.getsize(self.file_path) == 0:
            raise ExtractionError(
                f"Error de Extracción: El archivo '{self.file_path.name}' se encuentra vacío."
            )

        extracted_records: List[Dict[str, Any]] = []

        try:
            with open(self.file_path, mode="r", encoding="utf-8-sig", newline="") as csv_file:
                reader = csv.DictReader(csv_file)

                # Validar cabeceras requeridas
                fieldnames = [f.strip() for f in (reader.fieldnames or [])]
                missing_columns = [col for col in self.REQUIRED_COLUMNS if col not in fieldnames]

                if missing_columns:
                    raise ExtractionError(
                        f"El archivo CSV no contiene las columnas requeridas: {missing_columns}. "
                        f"Columnas encontradas: {fieldnames}"
                    )

                for row_num, row in enumerate(reader, start=2):
                    cleaned_row = {k.strip(): v for k, v in row.items() if k is not None}
                    extracted_records.append(cleaned_row)

        except PermissionError as pe:
            raise ExtractionError(
                f"Error de permisos al intentar acceder al archivo '{self.file_path.name}': {str(pe)}"
            ) from pe
        except csv.Error as ce:
            raise ExtractionError(
                f"Error de formato CSV al procesar el archivo '{self.file_path.name}': {str(ce)}"
            ) from ce
        except UnicodeDecodeError as ue:
            raise ExtractionError(
                f"Error de codificación al leer el archivo '{self.file_path.name}': {str(ue)}"
            ) from ue
        except Exception as ex:
            if isinstance(ex, ExtractionError):
                raise ex
            raise ExtractionError(
                f"Error inesperado al extraer datos desde '{self.file_path.name}': {str(ex)}"
            ) from ex

        return extracted_records
