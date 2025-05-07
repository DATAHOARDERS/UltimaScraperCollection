import socket
from typing import Sequence

import netifaces
from sqlalchemy import select
from ultima_scraper_db.databases.ultima_archive.database_api import ArchiveAPI
from ultima_scraper_db.databases.ultima_archive.schemas.management import (
    ServerModel,
    SiteModel,
)
from ultima_scraper_db.managers.database_manager import Schema
from ultima_scraper_db.databases.ultima_archive.schemas.management import (
    get_default_sites,
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


class ServerManager:
    def __init__(self, ultima_archive_db_api: ArchiveAPI) -> None:
        self.ultima_archive_db_api = ultima_archive_db_api

    async def init(self):

        async with self.ultima_archive_db_api.create_management_api() as management_api:
            session = management_api.get_session()
            db_sites = await session.scalars(select(SiteModel))
            db_sites = db_sites.all()
            self.reset = False
            if not db_sites:
                # Need to add a create or update for additional sites

                for site in get_default_sites():
                    session.add(site)
                await session.commit()
                db_sites = await session.scalars(select(SiteModel))
                db_sites = db_sites.all()
                self.reset = True
            private_ip = get_local_ip()
            mac_address = mac_for_ip(private_ip)
            # public_ip = requests.get("https://checkip.amazonaws.com/").text.strip()
            self.db_sites: Sequence[SiteModel] = db_sites
            self.ip_address = private_ip

            db_servers = await session.scalars(select(ServerModel))
            db_servers = db_servers.all()
            if not db_servers:
                default_server = ServerModel(
                    name="home", ip=self.ip_address, mac_address=mac_address
                )
                session.add(default_server)
                await session.commit()
            active_server = await session.scalars(
                select(ServerModel).where(
                    (ServerModel.ip == self.ip_address)
                    & (ServerModel.mac_address == mac_address)
                )
            )
            self.active_server = active_server.one()
            self.site_schemas: dict[str, Schema] = {}
            for db_site in self.db_sites:
                site_schema_api = self.ultima_archive_db_api.get_site_api(
                    db_site.db_name
                )
                self.site_schemas[site_schema_api.schema.name] = site_schema_api.schema
            return self

    async def resolve_site_schema(self, value: str):
        return self.site_schemas[value]

    async def resolve_db_site(self, value: str):
        return [x for x in self.db_sites if x.db_name == value][0]

    async def find_site_api(self, name: str):
        return self.ultima_archive_db_api.site_apis[name]

    def get_server_id(self):
        return self.active_server.id
