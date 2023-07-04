from nintendo.nex import common, rmc
from nex_protocols_common_py.authentication_protocol import AuthenticationUser

from pymongo.collection import Collection

import grpc
import amkj_service_pb2
import amkj_service_pb2_grpc

from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp


class AmkjService(amkj_service_pb2_grpc.AmkjServiceServicer):

    def __init__(self, api_key: str, status_db: Collection, gatherings_db: Collection, tournaments_db: Collection):
        self.rmc_secure_server = None
        self.api_key = api_key
        self.status_db = status_db
        self.gatherings_db = gatherings_db
        self.tournaments_db = tournaments_db

        self.is_online = False
        self.is_maintenance = False
        self.is_whitelist = False
        self.num_clients = 0
        self.start_maintenance_time = datetime.utcnow()
        self.end_maintenance_time = datetime.utcnow()
        self.whitelist = []

        self.rmc_clients: dict[int, rmc.RMCClient] = {}

        self.sync_status_from_database()
        self.sync_status_to_database()

    def sync_status_to_database(self):
        self.status_db.find_one_and_update({}, {
            "$set": {
                "is_online": self.is_online,
                "is_maintenance": self.is_maintenance,
                "is_whitelist": self.is_whitelist,
                "num_clients": self.num_clients,
                "start_maintenance_time": self.start_maintenance_time,
                "end_maintenance_time": self.end_maintenance_time,
                "whitelist": self.whitelist,
            }
        }, upsert=True)

    def sync_status_from_database(self):
        status = self.status_db.find_one({})
        if status:
            self.is_online = status["is_online"]
            self.is_maintenance = status["is_maintenance"]
            self.is_whitelist = status["is_whitelist"]
            self.num_clients = status["num_clients"]
            self.start_maintenance_time = status["start_maintenance_time"]
            self.end_maintenance_time = status["end_maintenance_time"]
            self.whitelist = status["whitelist"]

    def add_player_connected(self, client: rmc.RMCClient):
        self.num_clients += 1
        self.rmc_clients[client.pid()] = client

    def del_player_connected(self, client: rmc.RMCClient):
        self.num_clients -= 1
        if client.pid() in self.rmc_clients:
            del self.rmc_clients[client.pid()]

    async def check_auth(self, context: grpc.aio.ServicerContext):
        metadata = dict(context.invocation_metadata())
        api_key = metadata.get("x-api-key")
        if not api_key:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing header X-API-Key")

        if api_key != self.api_key:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Bad API key")

    async def GetServerStatus(self,
                              request: amkj_service_pb2.GetServerStatusRequest,
                              context: grpc.aio.ServicerContext) -> amkj_service_pb2.GetServerStatusResponse:

        await self.check_auth(context)

        start_maintenance = Timestamp()
        start_maintenance.FromDatetime(self.start_maintenance_time)

        end_maintenance = Timestamp()
        end_maintenance.FromDatetime(self.end_maintenance_time)

        return amkj_service_pb2.GetServerStatusResponse(
            is_online=self.is_online,
            is_maintenance=self.is_maintenance,
            is_whitelist=self.is_whitelist,
            num_clients=self.num_clients,
            start_maintenance_time=start_maintenance,
            end_maintenance_time=end_maintenance,
        )

    async def GetAllGatherings(self,
                               request: amkj_service_pb2.GetAllGatheringsRequest,
                               context: grpc.aio.ServicerContext) -> amkj_service_pb2.GetAllGatheringsResponse:

        await self.check_auth(context)

        cursor = self.gatherings_db.find({}).skip(request.offset)
        if request.limit > 0:
            cursor = cursor.limit(request.limit)

        gatherings = []
        for gathering in cursor:
            attribs = gathering["attribs"] if "attribs" in gathering else []
            app_data = gathering["application_data"] if "application_data" in gathering else b""
            game_mode = gathering["game_mode"] if "game_mode" in gathering else 0

            gatherings.append(
                amkj_service_pb2.Gathering(
                    gid=gathering["id"],
                    host=gathering["host"],
                    owner=gathering["owner"],
                    attributes=attribs,
                    game_mode=game_mode,
                    app_data=app_data,
                    players=gathering["players"],
                    min_participants=gathering["min_participants"],
                    max_participants=gathering["max_participants"]
                ))

        return amkj_service_pb2.GetAllGatheringsResponse(gatherings=gatherings)

    async def GetAllTournaments(self,
                                request: amkj_service_pb2.GetAllTournamentsRequest,
                                context: grpc.aio.ServicerContext) -> amkj_service_pb2.GetAllTournamentsResponse:

        await self.check_auth(context)

        cursor = self.tournaments_db.find({}).skip(request.offset)
        if request.limit > 0:
            cursor = cursor.limit(request.limit)

        tournaments = []
        for tournament in cursor:
            tournaments.append(
                amkj_service_pb2.Tournament(
                    id=tournament["id"],
                    owner=tournament["owner"],
                    attributes=tournament["attributes"],
                    community_code=tournament["community_code"],
                    app_data=tournament["metadata"],
                    total_participants=tournament["total_participants"],
                    season_id=tournament["season_id"],
                    name=tournament["parsed_metadata"]["name"],
                    description=tournament["parsed_metadata"]["description"],
                    red_team=tournament["parsed_metadata"]["red_team"],
                    blue_team=tournament["parsed_metadata"]["blue_team"],
                    repeat_type=tournament["parsed_metadata"]["repeat_type"],
                    gameset_num=tournament["parsed_metadata"]["gameset_num"],
                    icon_type=tournament["parsed_metadata"]["icon_type"],
                    battle_time=tournament["parsed_metadata"]["battle_time"],
                    update_date=tournament["parsed_metadata"]["update_date"],
                ))

        return amkj_service_pb2.GetAllTournamentsResponse(tournaments=tournaments)
