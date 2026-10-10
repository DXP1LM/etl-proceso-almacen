"""
Pruebas unitarias para validar los componentes POO del proceso ETL.
"""
import unittest
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.extractor import CSVExtractor, ExtractionError
from src.transformer import DataTransformer


class TestETLProcess(unittest.TestCase):
    """Batería de pruebas unitarias para Extractor y Transformer."""

    def setUp(self) -> None:
        self.csv_path = BASE_DIR / "data" / "transacciones_almacen.csv"
        self.transformer = DataTransformer()

    def test_extractor_file_found(self) -> None:
        """Verifica que el archivo fuente existente sea leído con éxito."""
        extractor = CSVExtractor(str(self.csv_path))
        data = extractor.extract()
        self.assertEqual(len(data), 100)

    def test_extractor_file_not_found(self) -> None:
        """Verifica que se lance ExtractionError ante un archivo inexistente."""
        fake_path = BASE_DIR / "data" / "archivo_fantasma.csv"
        extractor = CSVExtractor(str(fake_path))
        with self.assertRaises(ExtractionError):
            extractor.extract()

    def test_transformer_data_cleaning_and_calculation(self) -> None:
        """Verifica el descarte de nulos/<=0, normalización y cálculo de total_venta."""
        sample_data = [
            # Registro válido
            {
                "id_transaccion": "1",
                "producto": "  laptop lenovo i5  ",
                "cantidad": "2",
                "precio_unitario": "2500.50",
                "fecha_registro": "15/03/2026",
                "correo_empleado": "ANA.TORRES@UNSA.EDU.PE"
            },
            # Cantidad <= 0 (debe descartarse)
            {
                "id_transaccion": "2",
                "producto": "Mouse",
                "cantidad": "0",
                "precio_unitario": "50.00",
                "fecha_registro": "2026-03-01",
                "correo_empleado": "juan@unsa.edu.pe"
            },
            # Precio <= 0 (debe descartarse)
            {
                "id_transaccion": "3",
                "producto": "Teclado",
                "cantidad": "1",
                "precio_unitario": "-10.00",
                "fecha_registro": "2026-03-01",
                "correo_empleado": "pedro@unsa.edu.pe"
            },
            # Valor nulo (debe descartarse)
            {
                "id_transaccion": "4",
                "producto": "Monitor",
                "cantidad": "",
                "precio_unitario": "600.00",
                "fecha_registro": "2026-03-01",
                "correo_empleado": "luis@unsa.edu.pe"
            }
        ]

        clean_records, stats = self.transformer.transform(sample_data)

        # Debe quedar solo 1 registro válido y 3 descartados
        self.assertEqual(len(clean_records), 1)
        self.assertEqual(stats["total_descartados"], 3)

        rec = clean_records[0]
        # Normalización
        self.assertEqual(rec["producto"], "LAPTOP LENOVO I5")
        self.assertEqual(rec["correo_empleado"], "ana.torres@unsa.edu.pe")
        # Cálculo derivado
        self.assertEqual(rec["total_venta"], 5001.00)
        # Formato de fecha
        self.assertEqual(rec["fecha_registro"], "2026-03-15")

    def test_transformer_dataset_complete(self) -> None:
        """Verifica la transformación sobre el dataset completo del examen."""
        extractor = CSVExtractor(str(self.csv_path))
        raw_data = extractor.extract()
        clean_records, stats = self.transformer.transform(raw_data)

        self.assertEqual(stats["total_recibidos"], 100)
        self.assertEqual(stats["total_descartados"], 3)
        self.assertEqual(stats["total_validos"], 97)
        self.assertEqual(len(clean_records), 97)


if __name__ == "__main__":
    unittest.main()
