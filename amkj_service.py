from nintendo.nex import common, rmc
from nex_protocols_common_py.authentication_protocol import AuthenticationUser

from pymongo.collection import Collection

import grpc
import amkj_service_pb2
import amkj_service_pb2_grpc

from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp


class AmkjService(amkj_service_pb2_grpc.AmkjServiceServicer):

    def __init__(self, status_db: Collection):
        self.rmc_secure_server = None
        self.status_db = status_db

        self.is_online = False
        self.is_maintenance = False
        self.is_whitelist = False
        self.num_clients = 0
        self.start_maintenance_time = datetime.utcnow()
        self.end_maintenance_time = datetime.utcnow()
        self.whitelist = []

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

    def add_player_connected(self):
        self.num_clients += 1

    def del_player_connected(self):
        self.num_clients -= 1

    async def GetServerStatus(self,
                              request: amkj_service_pb2.GetServerStatusRequest,
                              context: grpc.aio.ServicerContext) -> amkj_service_pb2.GetServerStatusResponse:
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
