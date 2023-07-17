from nintendo.nex import rmc, common, matchmaking_mk8d
from pymongo.collection import Collection
from typing import Callable

from nex_protocols_common_py.matchmake_extension_protocol import CommonMatchmakeExtensionServer

from nex_protocols_common_py.secure_connection_protocol import CommonSecureConnectionServer
import nex_protocols_common_py.matchmaking_utils as matchmaking_utils
import simple_search_object_utils


import logging
logger = logging.getLogger(__name__)


class MK8MatchmakeExtensionServer(CommonMatchmakeExtensionServer):

    METHOD_CREATE_SIMPLE_SEARCH_OBJECT = 36
    METHOD_UPDATE_SIMPLE_SEARCH_OBJECT = 37
    METHOD_DELETE_SIMPLE_SEARCH_OBJECT = 38
    METHOD_SEARCH_SIMPLE_SEARCH_OBJECT = 39
    METHOD_JOIN_MATCHMAKE_SESSION_WITH_EXTRA_PARTICIPANTS = 40
    METHOD_SEARCH_SIMPLE_SEARCH_OBJECT_BY_OBJECT_IDS = 41

    def __init__(self,
                 settings,
                 gatherings_db: Collection,
                 sequence_db: Collection,
                 get_friend_pids_func: Callable[[int], list[int]],
                 secure_connection_server: CommonSecureConnectionServer,
                 tournaments_db: Collection):

        super().__init__(settings, gatherings_db, sequence_db, get_friend_pids_func, secure_connection_server)
        self.settings = settings
        self.tournaments_db = tournaments_db

        self.methods.update({
            self.METHOD_CREATE_SIMPLE_SEARCH_OBJECT: self.handle_create_simple_search_object,
            self.METHOD_UPDATE_SIMPLE_SEARCH_OBJECT: self.handle_update_simple_search_object,
            self.METHOD_DELETE_SIMPLE_SEARCH_OBJECT: self.handle_delete_simple_search_object,
            self.METHOD_SEARCH_SIMPLE_SEARCH_OBJECT: self.handle_search_simple_search_object,
            self.METHOD_JOIN_MATCHMAKE_SESSION_WITH_EXTRA_PARTICIPANTS: self.handle_join_matchmake_session_with_extra_participants,
            self.METHOD_SEARCH_SIMPLE_SEARCH_OBJECT_BY_OBJECT_IDS: self.handle_search_simple_search_object_by_object_ids,
        })

    def verify_gathering_type(self, obj):
        super().verify_gathering_type(obj)

        if (matchmaking_utils.gathering_type_to_name(obj) != "MatchmakeSession"):
            raise common.RMCError("Core::InvalidArgument")

        if (obj.max_participants > 12):
            raise common.RMCError("Core::InvalidArgument")

        if (len(obj.attribs) < 5):
            raise common.RMCError("Core::InvalidArgument")

    @staticmethod
    def extension_filters(client, filter) -> dict:
        res = {}
        if matchmaking_utils.gathering_type_to_name(filter) == "MatchmakeSession":
            res.update({"attribs.0": filter.attribs[0]})  # Tournament ID (0 for not a tournament)
            res.update({"attribs.4": filter.attribs[4]})  # Filter DLC status
            res.update({"attribs.3": filter.attribs[3]})  # Filter region
        return res

    def verify_simple_search_param_type(self, obj: matchmaking_mk8d.SimpleSearchParam):
        simple_search_object_utils.verify_simple_search_param_type(obj)

    def verify_simple_search_object_type(self, obj: matchmaking_mk8d.SimpleSearchObject):
        simple_search_object_utils.verify_simple_search_object_type(obj)

    # ============= Method handlers implementations  =============

    async def handle_create_simple_search_object(self, client, input, output):
        logger.info("MK8MatchmakeExtensionServer.create_simple_search_object()")
        # --- request ---
        object = input.extract(matchmaking_mk8d.SimpleSearchObject)
        response = await self.create_simple_search_object(client, object)

        # --- response ---
        if not isinstance(response, int):
            raise RuntimeError("Expected int, got %s" % response.__class__.__name__)
        output.u32(response)

    async def handle_update_simple_search_object(self, client, input, output):
        logger.info("MK8MatchmakeExtensionServer.update_simple_search_object()")
        # --- request ---
        id = input.u32()
        object = input.extract(matchmaking_mk8d.SimpleSearchObject)
        await self.update_simple_search_object(client, id, object)

    async def handle_delete_simple_search_object(self, client, input, output):
        logger.info("MK8MatchmakeExtensionServer.delete_simple_search_object()")
        # --- request ---
        id = input.u32()
        await self.delete_simple_search_object(client, id)

    async def handle_search_simple_search_object(self, client, input, output):
        logger.info("MK8MatchmakeExtensionServer.search_simple_search_object()")
        # --- request ---
        param = input.extract(matchmaking_mk8d.SimpleSearchParam)
        response = await self.search_simple_search_object(client, param)

        # --- response ---
        if not isinstance(response, list):
            raise RuntimeError("Expected list, got %s" % response.__class__.__name__)
        output.list(response, output.add)

    async def handle_join_matchmake_session_with_extra_participants(self, client, input, output):
        logger.info("MK8MatchmakeExtensionServer.join_matchmake_session_with_extra_participants")
        # --- request ---
        gid = input.u32()
        join_message = input.string()
        ignore_blacklist = input.bool()
        participation_count = input.u16()
        extra_participants = input.u32()

        response = await self.join_matchmake_session_with_extra_participants(client, gid, join_message, ignore_blacklist, participation_count, extra_participants)

        # --- response ---
        if not isinstance(response, bytes):
            raise RuntimeError("Expected bytes, got %s" % response.__class__.__name__)
        output.buffer(response)

    async def handle_search_simple_search_object_by_object_ids(self, client, input, output):
        logger.info("MK8MatchmakeExtensionServer.search_simple_search_object_by_object_ids()")
        # --- request ---
        ids = input.list(input.u32)
        response = await self.search_simple_search_object_by_object_ids(client, ids)

        # --- response ---
        if not isinstance(response, list):
            raise RuntimeError("Expected list, got %s" % response.__class__.__name__)
        output.list(response, output.add)

    # ============= Method implementations  =============

    async def create_simple_search_object(self, client: rmc.RMCClient, obj: matchmaking_mk8d.SimpleSearchObject):

        self.verify_simple_search_object_type(obj)

        obj.id = simple_search_object_utils.get_next_tournament_id(self.sequence_db)
        obj.owner = client.pid()

        if len(obj.community_code) != 12 or obj.community_id == 0:
            raise common.RMCError("Core::InvalidArgument")

        for s in obj.community_code:
            if s < '0' or s > '9':
                raise common.RMCError("Core::InvalidArgument")

        tournament = self.tournaments_db.find_one({"community_code": obj.community_code})
        if tournament:
            raise common.RMCError("Core::InvalidArgument")

        metadata = simple_search_object_utils.TournamentMetadata(obj.metadata)
        metadata.parse()

        doc = simple_search_object_utils.simple_search_object_to_document(obj)
        doc.update({
            "total_participants": 0,
            "season_id": 1,
            "parsed_metadata": {
                "name": metadata.name,
                "description": metadata.description,
                "red_team": metadata.red_team,
                "blue_team": metadata.blue_team,
                "repeat_type": metadata.repeat_type,
                "gameset_num": metadata.gameset_num,
                "icon_type": metadata.icon_type,
                "battle_time": metadata.battle_time,
                "update_date": metadata.update_date,
            }
        })
        self.tournaments_db.insert_one(doc)

        return obj.id

    async def update_simple_search_object(self, client: rmc.RMCClient, id: int, obj: matchmaking_mk8d.SimpleSearchObject):

        self.verify_simple_search_object_type(obj)

        tournament = self.tournaments_db.find_one({"id": id})
        if not tournament:
            raise common.RMCError("Core::InvalidIndex")

        if tournament["owner"] != client.pid():
            raise common.RMCError("Core::AccessDenied")

        metadata = simple_search_object_utils.TournamentMetadata(obj.metadata)
        metadata.parse()

        self.tournaments_db.update_one({"id": id}, {
            "$set": {
                "attributes": obj.attributes,
                "metadata": obj.metadata,
                "datetime": simple_search_object_utils.simple_search_date_time_attribute_to_document(obj.datetime),
                "parsed_metadata": {
                    "name": metadata.name,
                    "description": metadata.description,
                    "red_team": metadata.red_team,
                    "blue_team": metadata.blue_team,
                    "repeat_type": metadata.repeat_type,
                    "gameset_num": metadata.gameset_num,
                    "icon_type": metadata.icon_type,
                    "battle_time": metadata.battle_time,
                    "update_date": metadata.update_date,
                }
            }
        })

    async def delete_simple_search_object(self, client: rmc.RMCClient, id: int):
        tournament = self.tournaments_db.find_one({"id": id})
        if not tournament:
            raise common.RMCError("Core::InvalidIndex")

        if tournament["owner"] != client.pid():
            raise common.RMCError("Core::AccessDenied")

        self.tournaments_db.delete_one({"id": id})

    async def search_simple_search_object(self, client, search_param: matchmaking_mk8d.SimpleSearchParam):

        self.verify_simple_search_param_type(search_param)

        list_conditions = []
        if search_param.id != 0:
            list_conditions.append({"id": search_param.id})

        if search_param.owner != 0:
            list_conditions.append({"owner": search_param.owner})

        if search_param.community_code != "":
            list_conditions.append({"community_code": search_param.community_code})

        if len(search_param.conditions) > 0:
            search_conditions_filters = simple_search_object_utils.get_query_filters_from_search_conditions(search_param.conditions)
            if len(search_conditions_filters.keys()) > 0:
                list_conditions.append(search_conditions_filters)

        res = []
        if len(list_conditions) > 1:
            res = list(self.tournaments_db.find({"$and": list_conditions}).skip(search_param.range.offset).limit(search_param.range.size))
        elif len(list_conditions) == 1:
            res = list(self.tournaments_db.find(list_conditions[0]).skip(search_param.range.offset).limit(search_param.range.size))

        return list(map(simple_search_object_utils.simple_search_object_from_document, res))

    async def join_matchmake_session_with_extra_participants(self, client, gid, join_message, ignore_blacklist, participation_count, extra_participants):
        gathering = self.gatherings_db.find_one({"id": gid})
        if not gathering:
            raise common.RMCError("RendezVous::SessionVoid")

        if not self.can_user_join_gathering(client, gathering):
            raise common.RMCError("RendezVous::NotFriend")

        gathering = matchmaking_utils.add_user_to_gathering_ex(self.gatherings_db, client, gathering, join_message, participation_count)
        return gathering["session_key"]

    async def search_simple_search_object_by_object_ids(self, client, ids):
        if len(ids) > 100:
            raise common.RMCError("Core::InvalidArgument")

        tournaments = self.tournaments_db.find({"id": {"$in": ids}})
        return list(map(simple_search_object_utils.simple_search_object_from_document, tournaments))
