from nintendo.nex import common, rmc
from nex_protocols_common_py.authentication_protocol import AuthenticationUser

from pymongo.collection import Collection

import grpc
import amkj_service_pb2
import amkj_service_pb2_grpc

import asyncio

from datetime import datetime, timezone
from google.protobuf.timestamp_pb2 import Timestamp


class AmkjService(amkj_service_pb2_grpc.AmkjServiceServicer):

    def __init__(self, api_key: str, status_db: Collection, gatherings_db: Collection, tournaments_db: Collection, commondata_db: Collection):
        self.rmc_secure_server = None
        self.api_key = api_key
        self.status_db = status_db
        self.gatherings_db = gatherings_db
        self.tournaments_db = tournaments_db
        self.commondata_db = commondata_db

        self.is_online = False
        self.is_maintenance = False
        self.is_whitelist = False

        self.should_switch_to_maintenance = False
        self.start_maintenance_time = datetime.utcnow()
        self.end_maintenance_time = datetime.utcnow()

        self.whitelist = []

        self.rmc_clients: dict[int, rmc.RMCClient] = {}
        self.rmc_clients_lock = asyncio.Lock()

        self.sync_status_from_database()
        self.sync_status_to_database()

    @staticmethod
    def grpc_timestamp_to_local(timestamp: Timestamp) -> datetime:
        seconds = timestamp.seconds
        nanoseconds = timestamp.nanos

        # Convert nanoseconds to microseconds
        microseconds = nanoseconds // 1000

        # Create datetime object from timestamp
        utc_datetime = datetime.utcfromtimestamp(seconds).replace(microsecond=microseconds)

        # Convert UTC datetime to local datetime
        local_datetime = utc_datetime.astimezone()

        return local_datetime

    def sync_status_to_database(self):
        self.status_db.find_one_and_update({}, {
            "$set": {
                "is_online": self.is_online,
                "is_maintenance": self.is_maintenance,
                "is_whitelist": self.is_whitelist,
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
            self.start_maintenance_time = status["start_maintenance_time"]
            self.end_maintenance_time = status["end_maintenance_time"]
            self.whitelist = status["whitelist"]

    async def add_player_connected(self, client: rmc.RMCClient):
        async with self.rmc_clients_lock:
            self.rmc_clients[client.pid()] = client

    async def del_player_connected(self, client: rmc.RMCClient):
        async with self.rmc_clients_lock:
            if client.pid() in self.rmc_clients:
                del self.rmc_clients[client.pid()]

    async def check_auth(self, context: grpc.aio.ServicerContext):
        metadata = dict(context.invocation_metadata())
        api_key = metadata.get("x-api-key")
        if not api_key:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing header X-API-Key")

        if api_key != self.api_key:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Bad API key")

    async def kick_all(self) -> int:
        pids = list(self.rmc_clients.keys())
        for pid in pids:
            cl = self.rmc_clients.pop(pid)
            await cl.disconnect()
        return len(pids)

    async def kick_by_pid(self, pid) -> bool:
        if pid in self.rmc_clients:
            cl = self.rmc_clients.pop(pid)
            await cl.disconnect()
            self.rmc_clients_lock.release()
            return True

        self.rmc_clients_lock.release()
        return False

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
            num_clients=len(list(self.rmc_clients)),
            start_maintenance_time=start_maintenance,
            end_maintenance_time=end_maintenance,
        )

    async def StartMaintenance(self,
                               request: amkj_service_pb2.StartMaintenanceRequest,
                               context: grpc.aio.ServicerContext) -> amkj_service_pb2.StartMaintenanceResponse:

        await self.check_auth(context)

        start_time: Timestamp = request.utc_start_maintenance_time
        end_time: Timestamp = request.utc_end_maintenance_time

        self.should_switch_to_maintenance = True
        self.start_maintenance_time = start_time.ToDatetime()
        self.end_maintenance_time = end_time.ToDatetime()

        return amkj_service_pb2.StartMaintenanceResponse()

    async def EndMaintenance(self,
                             request: amkj_service_pb2.EndMaintenanceRequest,
                             context: grpc.aio.ServicerContext) -> amkj_service_pb2.EndMaintenanceResponse:

        await self.check_auth(context)

        self.start_maintenance_time = datetime(1970, 1, 1, 0, 0, 0, 0)
        self.is_maintenance = False

        return amkj_service_pb2.EndMaintenanceResponse()

    async def ToggleWhitelist(self,
                              request: amkj_service_pb2.ToggleWhitelistRequest,
                              context: grpc.aio.ServicerContext) -> amkj_service_pb2.ToggleWhitelistResponse:

        await self.check_auth(context)

        self.is_whitelist = not self.is_whitelist

        return amkj_service_pb2.ToggleWhitelistResponse(is_whitelist=self.is_whitelist)

    async def GetWhitelist(self,
                           request: amkj_service_pb2.GetWhitelistRequest,
                           context: grpc.aio.ServicerContext) -> amkj_service_pb2.GetWhitelistRequest:

        await self.check_auth(context)

        return amkj_service_pb2.GetWhitelistRequest(pids=self.whitelist)

    async def AddWhitelistUser(self,
                               request: amkj_service_pb2.AddWhitelistUserRequest,
                               context: grpc.aio.ServicerContext) -> amkj_service_pb2.AddWhitelistUserResponse:

        await self.check_auth(context)

        if request.pid not in self.whitelist:
            self.whitelist.append(request.pid)

        return amkj_service_pb2.AddWhitelistUserResponse()

    async def DelWhitelistUser(self,
                               request: amkj_service_pb2.DelWhitelistUserRequest,
                               context: grpc.aio.ServicerContext) -> amkj_service_pb2.DelWhitelistUserResponse:

        await self.check_auth(context)

        if request.pid in self.whitelist:
            self.whitelist.remove(request.pid)

        return amkj_service_pb2.DelWhitelistUserResponse()

    async def GetAllUsers(self,
                          request: amkj_service_pb2.GetAllUsersRequest,
                          context: grpc.aio.ServicerContext) -> amkj_service_pb2.GetAllUsersResponse:

        await self.check_auth(context)

        res = amkj_service_pb2.GetAllUsersResponse(pids=list(self.rmc_clients.keys()))

        return res

    async def KickUser(self,
                       request: amkj_service_pb2.KickUserRequest,
                       context: grpc.aio.ServicerContext) -> amkj_service_pb2.KickUserResponse:

        await self.check_auth(context)

        res = await self.kick_by_pid(request.pid)
        if res:
            return amkj_service_pb2.KickUserResponse(was_connected=True)

        return amkj_service_pb2.KickUserResponse(was_connected=False)

    async def KickAllUsers(self,
                           request: amkj_service_pb2.KickAllUsersRequest,
                           context: grpc.aio.ServicerContext) -> amkj_service_pb2.KickAllUsersResponse:

        await self.check_auth(context)

        res = await self.kick_all()

        return amkj_service_pb2.KickAllUsersResponse(num_kicked=res)

    async def GetAllGatherings(self,
                               request: amkj_service_pb2.GetAllGatheringsRequest,
                               context: grpc.aio.ServicerContext) -> amkj_service_pb2.GetAllGatheringsResponse:

        await self.check_auth(context)

        pipeline = [
            {
                '$lookup': {
                    'from': self.commondata_db.name,
                    'localField': 'players',
                    'foreignField': 'pid',
                    'as': 'players'
                }
            },
            {
                "$project": {
                    "attribs": 1,
                    "application_data": 1,
                    "game_mode": 1,
                    "id": 1,
                    "host": 1,
                    "owner": 1,
                    "min_participants": 1,
                    "max_participants": 1,
                    "players.pid": 1,
                    "players.mii_name": 1
                }
            },
            {
                '$skip': request.offset
            },
        ]

        if request.limit >= 0:
            pipeline.append({"$limit": request.limit})

        cursor = self.gatherings_db.aggregate(pipeline)

        gatherings = []
        for gathering in cursor:
            attribs = gathering["attribs"] if "attribs" in gathering else []
            app_data = gathering["application_data"] if "application_data" in gathering else b""
            game_mode = gathering["game_mode"] if "game_mode" in gathering else 0

            players = []
            for player in gathering["players"]:
                mii_name = player["mii_name"] if "mii_name" in player else "<Restart game>"
                players.append(amkj_service_pb2.GatheringParticipant(pid=player["pid"], mii_name=mii_name))

            gatherings.append(
                amkj_service_pb2.Gathering(
                    gid=gathering["id"],
                    host=gathering["host"],
                    owner=gathering["owner"],
                    attributes=attribs,
                    game_mode=game_mode,
                    app_data=app_data,
                    players=players,
                    min_participants=gathering["min_participants"],
                    max_participants=gathering["max_participants"]
                ))

        return amkj_service_pb2.GetAllGatheringsResponse(gatherings=gatherings)

    async def GetAllTournaments(self,
                                request: amkj_service_pb2.GetAllTournamentsRequest,
                                context: grpc.aio.ServicerContext) -> amkj_service_pb2.GetAllTournamentsResponse:

        await self.check_auth(context)

        # Search all public tournaments
        cursor = self.tournaments_db.find({"attributes.0": 1}).skip(request.offset)
        if request.limit > 0:
            cursor = cursor.limit(request.limit)

        tournaments = []
        for tournament in cursor:
            start_date_time = Timestamp()
            start_date_time.FromDatetime(common.DateTime(tournament["datetime"]["start_datetime"]).standard_datetime())

            end_date_time = Timestamp()
            end_date_time.FromDatetime(common.DateTime(tournament["datetime"]["end_datetime"]).standard_datetime())

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
                    start_day_time=tournament["datetime"]["start_daytime"],
                    end_day_time=tournament["datetime"]["end_daytime"],
                    start_time=tournament["datetime"]["start_time"],
                    end_time=tournament["datetime"]["end_time"],
                    start_date_time=start_date_time,
                    end_date_time=end_date_time
                ))

        return amkj_service_pb2.GetAllTournamentsResponse(tournaments=tournaments)

    async def GetUnlocks(self,
                         request: amkj_service_pb2.GetUnlocksRequest,
                         context: grpc.aio.ServicerContext) -> amkj_service_pb2.GetUnlocksResponse:

        await self.check_auth(context)

        last_update = Timestamp()
        last_update.FromDatetime(datetime.utcnow())

        data = self.commondata_db.find_one({"pid": request.pid})
        if data:
            last_update.FromDatetime(data["last_update"])
            res = amkj_service_pb2.GetUnlocksResponse(
                has_data=True,
                vr_rate=data["vr_rate"],
                br_rate=data["br_rate"],
                last_update=last_update,
                gp_unlocks=data["gp_unlocks"],
                engine_unlocks=data["engine_unlocks"],
                driver_unlocks=data["driver_unlocks"],
                body_unlocks=data["body_unlocks"],
                tire_unlocks=data["tire_unlocks"],
                wing_unlocks=data["wing_unlocks"],
                stamp_unlocks=data["stamp_unlocks"],
                dlc_unlocks=data["dlc_unlocks"]
            )
        else:
            res = amkj_service_pb2.GetUnlocksResponse(
                has_data=False,
                vr_rate=0.0,
                br_rate=0.0,
                last_update=last_update,
                gp_unlocks=[],
                engine_unlocks=[],
                driver_unlocks=[],
                body_unlocks=[],
                tire_unlocks=[],
                wing_unlocks=[],
                stamp_unlocks=[],
                dlc_unlocks=[]
            )

        return res
