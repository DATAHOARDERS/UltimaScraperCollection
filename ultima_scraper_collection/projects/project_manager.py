import random
from typing import Any

import paramiko
from sqlalchemy import MetaData
from sshtunnel import SSHTunnelForwarder  # type: ignore
from ultima_scraper_db.managers.database_manager import Alembica, DatabaseManager


class Project:
    def __init__(
        self,
        name: str,
    ) -> None:
        self.name = name
        self.db_manager = DatabaseManager()

    def handle_ssh(self, db_info: dict[str, Any]):
        ssh_auth_info = db_info["ssh"]
        if ssh_auth_info["host"]:
            private_key_filepath = ssh_auth_info["private_key_filepath"]
            ssh_private_key_password = ssh_auth_info["private_key_password"]
            private_key = (
                paramiko.RSAKey.from_private_key_file(
                    private_key_filepath, ssh_private_key_password
                ).key
                if private_key_filepath
                else None
            )
            random_port = random.randint(6000, 6999)
            ssh_obj = SSHTunnelForwarder(
                (ssh_auth_info["host"], ssh_auth_info["port"]),
                ssh_username=ssh_auth_info["username"],
                ssh_pkey=private_key,
                ssh_private_key_password=ssh_private_key_password,
                remote_bind_address=(db_info["host"], db_info["port"]),
                local_bind_address=(db_info["host"], random_port),
            )
            db_info["ssh"] = ssh_obj
        else:
            db_info["ssh"] = None
        return db_info

    async def _init_db(
        self,
        db_info: dict[str, Any],
        alembica: Alembica,
        metadata: MetaData = MetaData(),
        echo: bool = False,
        upgrade: bool = False,
    ):
        if upgrade:
            alembica.is_generate = True
            alembica.is_migrate = True
        else:
            alembica.is_generate = False
            alembica.is_migrate = False
        db_info = self.handle_ssh(db_info)
        temp_database = self.db_manager.create_database(
            **db_info, metadata=metadata, alembica=alembica
        )
        self.db_manager.add_database(temp_database)
        await temp_database.init_db(echo)
        return temp_database
