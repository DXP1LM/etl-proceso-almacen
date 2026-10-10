"""
Fase 2: Transformación (Transform)
Clase DataTransformer para limpieza, normalización, cálculos y estandarización de fechas.
"""
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional


class TransformationError(Exception):
    """Excepción para errores críticos durante el proceso de transformación."""
    pass


class DataTransformer:
    """
    Clase responsable de la Fase de Transformación en el flujo ETL.
    Aplica reglas de negocio: limpieza, normalización, cálculo derivado y estandarización.
    """

    SUPPORTED_DATE_FORMATS = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%m-%d-%Y",
        "%m/%d/%Y"
    ]

    def __init__(self) -> None:
        self.stats: Dict[str, Any] = {
            "total_recibidos": 0,
            "total_validos": 0,
            "total_descartados": 0,
            "descartes_nulos": 0,
            "descartes_cantidad": 0,
            "descartes_precio": 0,
            "descartes_fechas": 0,
            "descartes_duplicados": 0,
            "detalles_descartes": []
        }

    def _clean_string(self, value: Any) -> str:
        """Elimina espacios en blanco iniciales, finales y normaliza espacios."""
        if value is None:
            return ""
        return str(value).strip()

    def _parse_and_standardize_date(self, date_str: str) -> Optional[str]:
        """
        Estandariza formatos heterogéneos de fecha al formato MySQL: YYYY-MM-DD.
        Retorna None si la fecha no coincide con ningún formato válido.
        """
        cleaned_date = self._clean_string(date_str)
        if not cleaned_date:
            return None

        for fmt in self.SUPPORTED_DATE_FORMATS:
            try:
                dt = datetime.strptime(cleaned_date, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return None

    def transform(self, raw_records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Ejecuta el pipeline de transformación completo sobre los registros recibidos.
        :param raw_records: Lista de registros crudos extraídos del CSV.
        :return: Tupla con (lista de registros limpios, diccionario de estadísticas).
        """
        self.stats = {
            "total_recibidos": len(raw_records),
            "total_validos": 0,
            "total_descartados": 0,
            "descartes_nulos": 0,
            "descartes_cantidad": 0,
            "descartes_precio": 0,
            "descartes_fechas": 0,
            "descartes_duplicados": 0,
            "detalles_descartes": []
        }

        clean_records: List[Dict[str, Any]] = []
        seen_ids = set()

        for idx, row in enumerate(raw_records, start=1):
            # 1. Limpieza inicial de texto (strip a todos los campos)
            id_transaccion_str = self._clean_string(row.get("id_transaccion"))
            producto_raw = self._clean_string(row.get("producto"))
            cantidad_str = self._clean_string(row.get("cantidad"))
            precio_str = self._clean_string(row.get("precio_unitario"))
            fecha_str = self._clean_string(row.get("fecha_registro"))
            correo_raw = self._clean_string(row.get("correo_empleado"))

            # 2. Validación de campos nulos o vacíos en cantidad o precio
            if not cantidad_str or not precio_str or cantidad_str.lower() in ["nan", "null", "none"] or precio_str.lower() in ["nan", "null", "none"]:
                self.stats["total_descartados"] += 1
                self.stats["descartes_nulos"] += 1
                self.stats["detalles_descartes"].append({
                    "fila": idx,
                    "id_transaccion": id_transaccion_str,
                    "motivo": "Valor nulo/vacío (NaN) en cantidad o precio_unitario",
                    "registro": row
                })
                continue

            # Conversión de tipos numéricos
            try:
                cantidad = int(float(cantidad_str))
            except ValueError:
                self.stats["total_descartados"] += 1
                self.stats["descartes_cantidad"] += 1
                self.stats["detalles_descartes"].append({
                    "fila": idx,
                    "id_transaccion": id_transaccion_str,
                    "motivo": f"Formato inválido de cantidad '{cantidad_str}'",
                    "registro": row
                })
                continue

            try:
                precio_unitario = round(float(precio_str), 2)
            except ValueError:
                self.stats["total_descartados"] += 1
                self.stats["descartes_precio"] += 1
                self.stats["detalles_descartes"].append({
                    "fila": idx,
                    "id_transaccion": id_transaccion_str,
                    "motivo": f"Formato inválido de precio_unitario '{precio_str}'",
                    "registro": row
                })
                continue

            # 3. Descartar filas con cantidad o precio menor o igual a cero
            if cantidad <= 0:
                self.stats["total_descartados"] += 1
                self.stats["descartes_cantidad"] += 1
                self.stats["detalles_descartes"].append({
                    "fila": idx,
                    "id_transaccion": id_transaccion_str,
                    "motivo": f"Cantidad no válida ({cantidad} <= 0)",
                    "registro": row
                })
                continue

            if precio_unitario <= 0:
                self.stats["total_descartados"] += 1
                self.stats["descartes_precio"] += 1
                self.stats["detalles_descartes"].append({
                    "fila": idx,
                    "id_transaccion": id_transaccion_str,
                    "motivo": f"Precio unitario no válido ({precio_unitario} <= 0)",
                    "registro": row
                })
                continue

            # 4. Control de transacciones duplicadas por id_transaccion
            try:
                id_transaccion = int(id_transaccion_str)
            except ValueError:
                self.stats["total_descartados"] += 1
                self.stats["detalles_descartes"].append({
                    "fila": idx,
                    "id_transaccion": id_transaccion_str,
                    "motivo": f"ID de transacción inválido '{id_transaccion_str}'",
                    "registro": row
                })
                continue

            if id_transaccion in seen_ids:
                self.stats["total_descartados"] += 1
                self.stats["descartes_duplicados"] += 1
                self.stats["detalles_descartes"].append({
                    "fila": idx,
                    "id_transaccion": id_transaccion,
                    "motivo": f"ID de transacción duplicado ({id_transaccion})",
                    "registro": row
                })
                continue
            seen_ids.add(id_transaccion)

            # 5. Formato de fechas: estandarizar a YYYY-MM-DD
            fecha_estandarizada = self._parse_and_standardize_date(fecha_str)
            if not fecha_estandarizada:
                self.stats["total_descartados"] += 1
                self.stats["descartes_fechas"] += 1
                self.stats["detalles_descartes"].append({
                    "fila": idx,
                    "id_transaccion": id_transaccion,
                    "motivo": f"Formato de fecha no reconocido o inválido: '{fecha_str}'",
                    "registro": row
                })
                continue

            # 6. Normalización de texto
            # Nombre de producto a MAYÚSCULAS
            producto_normalizado = producto_raw.upper()

            # Correo electrónico a minúsculas
            correo_normalizado = correo_raw.lower()

            # 7. Cálculo derivado: total_venta = cantidad * precio_unitario
            total_venta = round(cantidad * precio_unitario, 2)

            # Registro transformado y limpio
            clean_record = {
                "id_transaccion": id_transaccion,
                "producto": producto_normalizado,
                "cantidad": cantidad,
                "precio_unitario": precio_unitario,
                "total_venta": total_venta,
                "fecha_registro": fecha_estandarizada,
                "correo_empleado": correo_normalizado
            }

            clean_records.append(clean_record)

        self.stats["total_validos"] = len(clean_records)
        return clean_records, self.stats
