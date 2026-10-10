"""
Configuración de conexión a la base de datos MySQL.
Implementado mediante Programación Orientada a Objetos (POO).
"""
import os
from pathlib import Path
from typing import Dict, Any

try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False


class DatabaseConfig:
    """
    Clase encargada de gestionar y encapsular los parámetros de configuración
    de la base de datos MySQL a partir de variables de entorno o archivo .env.
    """

    def __init__(self, env_path: str = None) -> None:
        """
        Inicializa la configuración cargando variables de entorno.
        :param env_path: Ruta opcional hacia el archivo .env
        """
        self._load_environment(env_path)
        self.host: str = os.getenv("DB_HOST", "localhost")
        self.port: int = int(os.getenv("DB_PORT", "3306"))
        self.user: str = os.getenv("DB_USER", "root")
        self.password: str = os.getenv("DB_PASSWORD", "MySQL")
        self.database: str = os.getenv("DB_NAME", "almacen_db")

    def _load_environment(self, env_path: str = None) -> None:
        """
        Carga las variables de entorno desde el archivo .env si existe.
        """
        if env_path:
            target_env = Path(env_path)
        else:
            # Buscar .env en la raíz del proyecto
            target_env = Path(__file__).resolve().parent.parent / ".env"

        if target_env.exists():
            if DOTENV_AVAILABLE:
                load_dotenv(dotenv_path=target_env, override=True)
            else:
                self._manual_load_env(target_env)

    @staticmethod
    def _manual_load_env(env_file_path: Path) -> None:
        """
        Carga manual sencilla en caso de que python-dotenv no esté disponible.
        """
        try:
            with open(env_file_path, "r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, val = line.split("=", 1)
                        key = key.strip()
                        val = val.strip().strip("\"'")
                        os.environ.setdefault(key, val)
        except Exception:
            pass

    def get_server_connection_params(self) -> Dict[str, Any]:
        """
        Retorna los parámetros de conexión al servidor MySQL (sin especificar base de datos).
        Útil para verificar o crear la base de datos si no existe.
        """
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "charset": "utf8mb4"
        }

    def get_db_connection_params(self) -> Dict[str, Any]:
        """
        Retorna los parámetros de conexión directa a la base de datos destino.
        """
        params = self.get_server_connection_params()
        params["database"] = self.database
        return params

    def __str__(self) -> str:
        return f"DatabaseConfig(host='{self.host}', port={self.port}, user='{self.user}', db='{self.database}')"
