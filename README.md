# Proceso ETL Automatizado en Python y MySQL (POO)

Proyecto de automatización de un proceso ETL (Extract, Transform, Load) desarrollado bajo el paradigma de **Programación Orientada a Objetos (POO)** en Python y conectado a una base de datos relacional **MySQL**.

Diseñado para depurar operaciones diarias de inventario y ventas desde archivos planos `.csv` con inconsistencias humanas (espacios en blanco, formatos heterogéneos de fechas, valores nulos y registros inválidos), consolidándolos de forma estructurada en la tabla `historial_ventas`.

---

## 🏗️ Arquitectura y Diseño POO

El sistema está modularizado siguiendo principios SOLID y separación de responsabilidades:

```text
etl_almacen_project/
├── config/
│   ├── __init__.py
│   └── db_config.py         # Clase DatabaseConfig: Carga y encapsula parámetros de MySQL
├── data/
│   └── transacciones_almacen.csv  # Archivo fuente con inconsistencias
├── src/
│   ├── __init__.py
│   ├── extractor.py         # Clase CSVExtractor (Fase 1: Extracción con manejo de excepciones)
│   ├── transformer.py       # Clase DataTransformer (Fase 2: Limpieza, normalización y cálculos)
│   ├── loader.py            # Clase MySQLLoader (Fase 3: Carga transaccional en MySQL)
│   └── pipeline.py          # Clase ETLPipeline (Orquestador central del flujo)
├── tests/
│   └── test_etl.py          # Pruebas unitarias automatizadas con unittest
├── .env                     # Variables de entorno locales (credenciales MySQL)
├── .env.example             # Plantilla de variables de entorno
├── .gitignore               # Exclusión de archivos sensibles y temporales
├── main.py                  # Punto de entrada de la aplicación (CLI y ejecución directa)
├── verify_db.py             # Script de consulta SQL y verificación de la tabla destino
├── requirements.txt         # Dependencias del proyecto
└── README.md                # Documentación técnica completa
```

---

## 📋 Fases del Proceso ETL

### Fase 1: Extracción (`CSVExtractor`)
- Lectura segura del archivo fuente `transacciones_almacen.csv`.
- Validación de existencia de archivo, archivo no vacío y presencia de cabeceras requeridas (`id_transaccion`, `producto`, `cantidad`, `precio_unitario`, `fecha_registro`, `correo_empleado`).
- Manejo integral de excepciones personalizadas (`ExtractionError`, `FileNotFoundError`, `PermissionError`, `csv.Error`, `UnicodeDecodeError`).

### Fase 2: Transformación (`DataTransformer`)
- **Limpieza de datos:** Eliminación de espacios en blanco sobrantes (`strip()`) en todos los campos de texto.
- **Filtrado y depuración:** Descarte de filas donde `cantidad` o `precio_unitario` sea $\le 0$, o contenga valores nulos/vacíos (`NaN`, `NULL`).
- **Control de duplicados:** Identificación y descarte de registros duplicados por `id_transaccion`.
- **Normalización:**
  - Nombres de productos convertidos a formato estándar en **MAYÚSCULAS**.
  - Correos electrónicos de empleados convertidos a **minúsculas**.
- **Cálculo derivado:** Generación de la columna calculada `total_venta = round(cantidad * precio_unitario, 2)`.
- **Estandarización de fechas:** Detección de formatos heterogéneos (`YYYY-MM-DD`, `YYYY/MM/DD`, `DD-MM-YYYY`, `DD/MM/YYYY`, `MM-DD-YYYY`, `MM/DD/YYYY`) y conversión uniforme al formato estándar de MySQL: `YYYY-MM-DD`.

### Fase 3: Carga (`MySQLLoader`)
- Establecimiento de conexión con MySQL mediante la librería `mysql-connector-python`.
- Creación automática de la base de datos y de la tabla destino `historial_ventas` mediante `CREATE TABLE IF NOT EXISTS`:
  ```sql
  CREATE TABLE IF NOT EXISTS historial_ventas (
      id_transaccion INT PRIMARY KEY,
      producto VARCHAR(150) NOT NULL,
      cantidad INT NOT NULL,
      precio_unitario DECIMAL(10, 2) NOT NULL,
      total_venta DECIMAL(12, 2) NOT NULL,
      fecha_registro DATE NOT NULL,
      correo_empleado VARCHAR(150) NOT NULL
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
  ```
- Inserción masiva y parametrizada con `cursor.executemany(...)`.
- Gestión estricta de transacciones ACID (`connection.commit()` ante éxito y `connection.rollback()` ante cualquier error).

---

## 🚀 Requisitos e Instalación

### 1. Requisitos Previos
- Python 3.9 o superior
- Servidor MySQL activo (ej. MySQL Server 8.0, XAMPP, MariaDB)

### 2. Instalación de dependencias
```bash
pip install -r requirements.txt
```

### 3. Configuración del archivo `.env`
Copiar el archivo `.env.example` o editar el archivo `.env` existente con las credenciales de tu base de datos MySQL:
```ini
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=tu_contraseña
DB_NAME=almacen_db
```

---

## 💻 Ejecución del Proyecto

### 1. Ejecutar el Proceso ETL Completo
```bash
python main.py
```

*También puedes especificar argumentos opcionales por línea de comandos:*
```bash
python main.py --file data/transacciones_almacen.csv --host localhost --user root --password MySQL --db almacen_db
```

**Salida en consola esperada:**
```text
======================================================================
          PROCESO ETL - ALMACÉN Y GESTIÓN DE VENTAS
======================================================================
Archivo fuente: .../data/transacciones_almacen.csv
Base de datos destino: almacen_db en localhost:3306
----------------------------------------------------------------------

[FASE 1: EXTRACCIÓN] Iniciando lectura de archivo fuente...
 -> Éxito: Se extrajeron correctamente 100 registros del archivo CSV.

[FASE 2: TRANSFORMACIÓN] Aplicando reglas de depuración y normalización...
 -> Registros evaluados:    100
 -> Registros descartados:   3
     * Nulos/vacíos (NaN):  1
     * Cantidad inválida:   2
     * Precio inválido:     0
     * Fechas inválidas:    0
     * Duplicados:          0
 -> Registros limpios y válidos: 97

[FASE 3: CARGA] Conectando a MySQL y ejecutando carga masiva...
 -> Éxito: Se cargaron transaccionalmente 97 registros en la tabla 'historial_ventas'.
 -> Transacción confirmada (COMMIT).

======================================================================
          RESUMEN DE EJECUCIÓN DEL PROCESO ETL
======================================================================
 Total registros leídos:       100
 Total registros depurados:     3
 Total registros consolidados:  97
 Tiempo total de ejecución:     0.34 segundos
 Estado:                        EXITOSO
======================================================================
```

### 2. Verificar los Datos en MySQL
Ejecuta el script de verificación para consultar la base de datos directamente desde la terminal:
```bash
python verify_db.py
```

O desde MySQL Workbench / CLI de MySQL:
```sql
USE almacen_db;
SELECT * FROM historial_ventas;
```

### 3. Ejecutar Pruebas Unitarias
```bash
python -m unittest tests/test_etl.py
```

---

## 📸 Entregables para el Examen

1. **Captura de Pantalla 1:** Terminal ejecutando `python main.py` mostrando el resumen de registros leídos (100), depurados (3) y consolidados (97).
2. **Captura de Pantalla 2:** Resultado de la consulta `SELECT * FROM historial_ventas;` ejecutada en MySQL Workbench o mediante `python verify_db.py`.
3. **Enlace a GitHub:** Subir este repositorio a GitHub y entregar el enlace público.
