# ============= Importing necessary libraries =============

from nintendo.nex import rmc, kerberos, common
import logging
import asyncio
import time
import aioconsole
import requests
import contextlib
import datetime

from nex_protocols_common_py.authentication_protocol import AuthenticationUser, CommonAuthenticationServer
from nex_protocols_common_py.secure_connection_protocol import CommonSecureConnectionServer
from nex_protocols_common_py.matchmaking_protocol import CommonMatchMakingServer
from nex_protocols_common_py.nat_traversal_protocol import CommonNATTraversalServer
from nex_protocols_common_py.matchmaking_ext_protocol import CommonMatchMakingServerExt
from mk8_matchmake_extension_protocol import MK8MatchmakeExtensionServer
from mk8_ranking_protocol import MK8RankingServer, mk8_common_data_handler
from mk8_datastore_protocol import MK8DataStoreServer

import grpc
from amkj_service import AmkjService, amkj_service_pb2_grpc
from grpc_py.account import account_service_pb2_grpc
from grpc_py.account.get_nex_password_rpc_pb2 import GetNEXPasswordRequest
from grpc_py.friends import friends_service_pb2_grpc
from grpc_py.friends.get_user_friend_pids_rpc_pb2 import GetUserFriendPIDsRequest

import redis

import boto3
from botocore.client import Config

try:
    from server_config import NEX_CONFIG, NEX_SETTINGS
except ModuleNotFoundError as e:
    print('Rename "server_config.example.py" to "server_config.py" and modify the configuration options to fit with your env.')
    exit(-1)


logging.basicConfig(level=logging.INFO)

# ============= Connecting to the database =============

GameDatabase = NEX_CONFIG.game_db_server.connect()[NEX_CONFIG.game_database]

# ============= Main server program =============

amkj_service = AmkjService(NEX_CONFIG.mario_kart_8_grpc_api_key,
                           GameDatabase["status"],
                           GameDatabase[NEX_CONFIG.gatherings_collection],
                           GameDatabase[NEX_CONFIG.tournaments_collection],
                           GameDatabase[NEX_CONFIG.ranking_common_data_collection])

friends_grpc_client = grpc.insecure_channel('%s:%d' % (NEX_CONFIG.friends_grpc_host, NEX_CONFIG.friends_grpc_port))
friends_service = friends_service_pb2_grpc.FriendsStub(friends_grpc_client)

account_grpc_client = grpc.insecure_channel('%s:%d' % (NEX_CONFIG.account_grpc_host, NEX_CONFIG.account_grpc_port))
account_service = account_service_pb2_grpc.AccountStub(account_grpc_client)

redis_client = redis.from_url(NEX_CONFIG.redis_uri)
redis_client.ping()

s3_client = boto3.client(
    's3',
    region_name=NEX_CONFIG.s3_region,
    endpoint_url=NEX_CONFIG.s3_endpoint,
    aws_access_key_id=NEX_CONFIG.s3_access_key,
    aws_secret_access_key=NEX_CONFIG.s3_secret,
    config=Config(signature_version='s3v4')
)


def mk8_get_friend_pids(pid: int) -> list[int]:
    response = friends_service.GetUserFriendPIDs(GetUserFriendPIDsRequest(pid=pid), metadata=[("x-api-key", NEX_CONFIG.friends_grpc_api_key)])
    pids = [pids for pids in response.pids]
    return pids


def mk8_get_nex_password(pid: int) -> str:
    response = account_service.GetNEXPassword(GetNEXPasswordRequest(pid=pid), metadata=[("x-api-key", NEX_CONFIG.account_grpc_api_key)])
    return response.password


def mk8_auth_callback(auth_user: AuthenticationUser) -> common.Result:
    if amkj_service.is_maintenance:
        return common.Result.error("Authentication::UnderMaintenance")
    if amkj_service.is_whitelist and (auth_user.pid not in amkj_service.whitelist):
        return common.Result.error("RendezVous::PermissionDenied")
    return common.Result.success()

# ============= Custom RMC serving function =============


class ExtendedRMCClient(rmc.RMCClient):
    def __init__(self, settings, client):
        super().__init__(settings, client)
        asyncio.ensure_future(amkj_service.add_player_connected(self))

    async def cleanup(self):
        if not self.closed:
            await amkj_service.del_player_connected(self)
        await super().cleanup()


@contextlib.asynccontextmanager
async def serve_rmc_custom(settings, servers, host="", port=0, vport=1, context=None, key=None):
    async def handle(client):
        host, port = client.remote_address()
        rmc.logger.debug("New RMC connection: %s:%i", host, port)

        client = ExtendedRMCClient(settings, client)
        async with client:
            await client.start(servers)

    rmc.logger.info("Starting RMC server at %s:%i:%i", host, port, vport)
    async with rmc.prudp.serve(handle, settings, host, port, vport, 10, context, key):
        yield
    rmc.logger.info("RMC server is closed")


async def main():
    sett = NEX_SETTINGS

    # ============= Initializing our counter sequences =============

    counters = [("gathering_id", 1000), ("tournament_id", 20000), ("datastore_object_id", 20000)]
    for counter in counters:
        GameDatabase[NEX_CONFIG.sequence_collection].find_one_and_update(
            {"_id": counter[0]}, {"$setOnInsert": {"_id": counter[0], "seq": counter[1]}}, upsert=True)

    # ============= Initializing Authentication Protocol =============

    SecureServerUser = AuthenticationUser(2, "Quazal Rendez-Vous", NEX_CONFIG.nex_secure_user_password)
    GuestUser = AuthenticationUser(100, "guest", "MMQea3n!fsik")

    AuthenticationServer = CommonAuthenticationServer(sett,
                                                      secure_host=NEX_CONFIG.nex_external_address,
                                                      secure_port=NEX_CONFIG.nex_secure_port,
                                                      build_string="Pretendo MK8 server",
                                                      special_users=[SecureServerUser, GuestUser],
                                                      get_nex_password_func=mk8_get_nex_password,
                                                      auth_callback=mk8_auth_callback)

    # ============= Initializing Secure Protocol =============

    GameDatabase[NEX_CONFIG.sessions_collection].delete_many({})  # Clear all remaining sessions

    SecureConnectionServer = CommonSecureConnectionServer(sett,
                                                          sessions_db=GameDatabase[NEX_CONFIG.sessions_collection],
                                                          reportdata_db=GameDatabase[NEX_CONFIG.secure_reports_collection])

    # ============= Initializing Ranking Protocol =============

    RankingServer = MK8RankingServer(sett,
                                     rankings_db=GameDatabase[NEX_CONFIG.rankings_score_collection],
                                     redis_instance=redis_client,
                                     commondata_db=GameDatabase[NEX_CONFIG.ranking_common_data_collection],
                                     common_data_handler=mk8_common_data_handler,
                                     rankings_category={},
                                     tournaments_db=GameDatabase[NEX_CONFIG.tournaments_collection],
                                     tournaments_scores_db=GameDatabase[NEX_CONFIG.tournaments_score_collection])

    # ============= Initializing Matchmake Extension Protocol =============

    MatchmakeExtensionServer = MK8MatchmakeExtensionServer(sett,
                                                           gatherings_db=GameDatabase[NEX_CONFIG.gatherings_collection],
                                                           sequence_db=GameDatabase[NEX_CONFIG.sequence_collection],
                                                           get_friend_pids_func=mk8_get_friend_pids,
                                                           secure_connection_server=SecureConnectionServer,
                                                           tournaments_db=GameDatabase[NEX_CONFIG.tournaments_collection])

    # ============= Initializing Matchmaking Ext Protocol =============

    MatchmakingExtServer = CommonMatchMakingServerExt(sett,
                                                      gatherings_db=GameDatabase[NEX_CONFIG.gatherings_collection],
                                                      sequence_db=GameDatabase[NEX_CONFIG.sequence_collection])

    # ============= Initializing NAT Traversal Protocol =============

    NATTraversalServer = CommonNATTraversalServer(sett,
                                                  sessions_db=GameDatabase[NEX_CONFIG.sessions_collection],
                                                  secure_connection_server=SecureConnectionServer)

    # ============= Initializing Matchmaking Protocol =============

    MatchmakingServer = CommonMatchMakingServer(sett,
                                                gatherings_db=GameDatabase[NEX_CONFIG.gatherings_collection],
                                                sessions_db=GameDatabase[NEX_CONFIG.sessions_collection],
                                                sequence_db=GameDatabase[NEX_CONFIG.sequence_collection])

    # ============= Initializing DataStore Protocol  =============

    def mk8_calculate_s3_object_key_ex(database, pid, persistence_id: int, object_id: int) -> str:
        if persistence_id < 1024:
            return "ghosts/%d/%d.bin" % (pid, persistence_id)
        else:
            return "mktv/%d.bin" % (object_id)

    def mk8_calculate_s3_object_key(database, client, persistence_id: int, object_id: int) -> str:
        if persistence_id < 1024:
            return "ghosts/%d/%d.bin" % (client.pid(), persistence_id)
        else:
            return "mktv/%d.bin" % (object_id)

    def mk8_head_object_by_key(key: str) -> tuple[bool, int, str]:
        url = "https://%s.b-cdn.net/%s" % (NEX_CONFIG.bucket_name, key)
        res = requests.head(url)
        success = (res.status_code == 200)
        return success, (0 if not success else int(res.headers["Content-Length"])), url

    DataStoreServer = MK8DataStoreServer(sett,
                                         s3_client=s3_client,
                                         s3_endpoint_domain=NEX_CONFIG.s3_endpoint_domain,
                                         s3_bucket=NEX_CONFIG.bucket_name,
                                         datastore_db=GameDatabase[NEX_CONFIG.datastore_collection],
                                         sequence_db=GameDatabase[NEX_CONFIG.sequence_collection],
                                         head_object_by_key=mk8_head_object_by_key,
                                         calculate_s3_object_key=mk8_calculate_s3_object_key,
                                         calculate_s3_object_key_ex=mk8_calculate_s3_object_key_ex)

    # ============= Creating our RMC server =============

    auth_servers = [
        AuthenticationServer
    ]
    secure_servers = [
        SecureConnectionServer,
        RankingServer,
        MatchmakeExtensionServer,
        MatchmakingExtServer,
        NATTraversalServer,
        MatchmakingServer,
        DataStoreServer,
    ]

    amkj_service.bind_ranking_manager(RankingServer.ranking_mgr)

    server_key = kerberos.KeyDerivationOld(65000, 1024).derive_key(NEX_CONFIG.nex_secure_user_password.encode("ascii"), pid=2)
    async with rmc.serve(sett, auth_servers, NEX_CONFIG.nex_host, NEX_CONFIG.nex_auth_port):
        async with serve_rmc_custom(sett, secure_servers, NEX_CONFIG.nex_host, NEX_CONFIG.nex_secure_port, key=server_key):
            server = grpc.aio.server(options=(("grpc.primary_user_agent", "Pretendo_MK8_GRPC"),))
            amkj_service_pb2_grpc.add_AmkjServiceServicer_to_server(amkj_service, server)

            listen_addr = "%s:%d" % (NEX_CONFIG.mario_kart_8_grpc_host, NEX_CONFIG.mario_kart_8_grpc_port)
            server.add_insecure_port(listen_addr)
            logging.info("Starting gRPC amkj server on %s", listen_addr)

            await server.start()
            await aioconsole.ainput("Press enter to exit...\n")


async def sync_amkj_status_to_database(task: asyncio.Task):
    last_time = time.time()
    while True:
        if task.done():
            break

        if time.time() - last_time > 5:
            amkj_service.sync_status_to_database()
            last_time = time.time()

            if amkj_service.is_maintenance == False and amkj_service.should_switch_to_maintenance == True:
                if datetime.datetime.utcnow() > amkj_service.start_maintenance_time:
                    amkj_service.is_maintenance = True
                    amkj_service.should_switch_to_maintenance = False
                    await amkj_service.kick_all()

        await asyncio.sleep(0.1)

    print("Exiting AMKJ service, emptying player list/count ...")
    amkj_service.num_clients = 0
    amkj_service.sync_status_to_database()


async def init():
    main_task = asyncio.create_task(main())
    sync_task = asyncio.create_task(sync_amkj_status_to_database(main_task))

    await main_task
    await sync_task

if __name__ == "__main__":
    asyncio.run(init())
