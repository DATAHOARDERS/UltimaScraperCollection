import random
import socket
from typing import TYPE_CHECKING, Any, Sequence

import netifaces
import paramiko

# from degen_x.managers.database_manager.database_manager import DatabaseManager
# from degen_x.managers.database_manager.job_manager import JobManager
# from degen_x.managers.database_manager.legacy import legacy_to_new
from sqlalchemy import MetaData, select
from sshtunnel import SSHTunnelForwarder  # type: ignore
from ultima_scraper_db.databases.ultima.schemas.management import ServerModel, SiteModel
from ultima_scraper_db.managers.database_manager import Alembica, DatabaseManager
from ultima_scraper_db.managers.database_manager import Schema

if TYPE_CHECKING:
    from ultima_scraper_collection import datascraper_types


class ServerManager:
    def __init__(
        self,
        db_info: dict[str, Any],
        db_manager: DatabaseManager = DatabaseManager(),
    ) -> None:
        self.db_manager = db_manager
        # self.job_manager = JobManager(self)
        self.db_info = db_info.copy()
        if self.db_info["ssh"]["host"]:
            ssh_private_key_password = self.db_info["ssh"]["private_key_password"]
            private_key = paramiko.RSAKey.from_private_key_file(
                self.db_info["ssh"]["private_key_filepath"], ssh_private_key_password
            ).key
            random_port = random.randint(6000, 6999)
            ssh_obj = SSHTunnelForwarder(
                (self.db_info["ssh"]["host"], self.db_info["ssh"]["port"]),
                ssh_username=self.db_info["ssh"]["username"],
                ssh_pkey=private_key,
                ssh_private_key_password=ssh_private_key_password,
                remote_bind_address=(self.db_info["host"], self.db_info["port"]),
                local_bind_address=(self.db_info["host"], random_port),
            )
            self.db_info["ssh"] = ssh_obj
        else:
            self.db_info["ssh"] = None

    async def init(self, alembica: Alembica, metadata: MetaData = MetaData()):
        temp_database = self.db_manager.create_database(
            **self.db_info, metadata=metadata, alembica=alembica
        )

        def create_socket(socket_type: socket.SocketKind = socket.SOCK_DGRAM):
            temp_socket = socket.socket(socket.AF_INET, socket_type)
            temp_socket.connect(("8.8.8.8", 80))  # Connecting to Google's DNS server
            return temp_socket

        def get_local_ip():
            # Create a temporary connection to a remote server to retrieve the local IP address
            temp_socket = create_socket()
            local_ip = temp_socket.getsockname()[0]
            temp_socket.close()
            return local_ip

        def mac_for_ip(ip: str) -> str | None:
            "Returns a list of MACs for interfaces that have given IP, returns None if not found"
            for i in netifaces.interfaces():  # type: ignore
                addrs = netifaces.ifaddresses(i)  # type: ignore
                try:
                    if_mac: str | None = addrs[netifaces.AF_LINK][0]["addr"]  # type: ignore
                    if_ip: str | None = addrs[netifaces.AF_INET][0]["addr"]  # type: ignore
                except (IndexError, KeyError):  # ignore ifaces that dont have MAC or IP
                    if_mac = if_ip = None
                if if_ip == ip:
                    return if_mac  # type: ignore
            return None

        self.db_manager.add_database(temp_database)
        database = await temp_database.init_db()
        temp_database.alembica = alembica
        if temp_database.alembica.is_generate:
            await temp_database.generate_migration()
            await temp_database.resolve_schemas()
        await temp_database.run_migrations()
        self.management_db = database.schemas["management"]
        db_sites = await self.management_db.session.scalars(select(SiteModel))
        db_sites = db_sites.all()
        self.reset = False
        if not db_sites:
            # Need to add a create or update for additional sites
            from ultima_scraper_db.databases.ultima.schemas.management import (
                default_sites,
            )

            for site in default_sites:
                self.management_db.session.add(site)
            await self.management_db.session.commit()
            db_sites = await self.management_db.session.scalars(select(SiteModel))
            db_sites = db_sites.all()
            self.reset = True
        private_ip = get_local_ip()
        mac_address = mac_for_ip(private_ip)
        # public_ip = requests.get("https://checkip.amazonaws.com/").text.strip()
        self.db_sites: Sequence[SiteModel] = db_sites
        self.ip_address = private_ip

        db_servers = await self.management_db.session.scalars(select(ServerModel))
        db_servers = db_servers.all()
        if not db_servers:
            default_server = ServerModel(
                name="home", ip=self.ip_address, mac_address=mac_address
            )
            self.management_db.session.add(default_server)
            await self.management_db.session.commit()
        active_server = await self.management_db.session.scalars(
            select(ServerModel).where(
                (ServerModel.ip == self.ip_address)
                & (ServerModel.mac_address == mac_address)
            )
        )
        self.active_server = active_server.one()
        self.site_schemas: list[Schema] = []
        for db_site in self.db_sites:
            site_schema_api = await self.get_site_db(db_site.db_name)
            self.site_schemas.append(site_schema_api.schema)
        return self

    async def get_site_db(
        self, name: str, datascraper: "datascraper_types | None" = None
    ):
        from ultima_scraper_db.managers.site_db import SiteDB

        return SiteDB(
            self.db_manager.databases[self.db_info["name"]].schemas[name.lower()],
            datascraper=datascraper,
        )

    async def resolve_db_site(self, value: str):
        return [x for x in self.db_sites if x.db_name == value][0]
