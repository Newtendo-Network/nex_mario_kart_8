from nintendo.nex import common
from pymongo.collection import Collection
from typing import Callable
import pymongo
import redis
import struct
import bson
import datetime

from nex_protocols_common_py.ranking_protocol import CommonRankingServer

from nintendo.nex.ranking_mk8d import \
    CompetitionRankingGetScoreParam, CompetitionRankingUploadScoreParam,\
    CompetitionRankingInfoGetParam, CompetitionRankingScoreInfo, CompetitionRankingInfo, CompetitionRankingScoreData

import logging
logger = logging.getLogger(__name__)


def mk8_common_data_handler(collection: Collection, pid: int, data: bytes, unique_id: int) -> bool:

    if len(data) != 0xd4:
        raise common.RMCError("Ranking::InvalidDataSize")

    unk1, unk2, unk3 = struct.unpack(">III", data[0:0x0c])
    vr_rate, br_rate = struct.unpack(">ff", data[0x0c:0x14])
    account_related_data = data[0x14:0x74]
    unk4, unk5, unk6, unk7, unk8 = struct.unpack(">BBxxIII", data[0x74:0x84])
    open_flag_pack = data[0x84:0xc3]

    flags = open_flag_pack
    bits = [flags[i // 8] & 1 << i % 8 != 0 for i in range(len(flags) * 8)]

    # Literally a copy of how the game does it, but reverse

    gp_unlocks = []
    for i in range(20):
        gp_unlocks.append(int(bits[(0 * 8) + i]))

    engine_unlocks = []
    for i in range(5):
        engine_unlocks.append(int(bits[(4 * 8) + i]))

    driver_unlocks = []
    for i in range(37):
        driver_unlocks.append(int(bits[(5 * 8) + i]))

    body_unlocks = []
    for i in range(39):
        body_unlocks.append(int(bits[(13 * 8) + i]))

    tire_unlocks = []
    for i in range(21):
        tire_unlocks.append(int(bits[(21 * 8) + i]))

    wing_unlocks = []
    for i in range(14):
        wing_unlocks.append(int(bits[(29 * 8) + i]))

    stamp_unlocks = []
    for i in range(100):
        stamp_unlocks.append(int(bits[(45 * 8) + i]))

    dlc_unlocks = []
    for i in range(5):
        dlc_unlocks.append(int(bits[(61 * 8) + i]))

    document = {
        "pid": pid,
        "data": bson.Binary(data),
        "size": len(data),
        "unique_id": unique_id,
        "last_update": datetime.datetime.utcnow(),

        "vr_rate": vr_rate,
        "br_rate": br_rate,
        "gp_unlocks": gp_unlocks,
        "engine_unlocks": engine_unlocks,
        "driver_unlocks": driver_unlocks,
        "body_unlocks": body_unlocks,
        "tire_unlocks": tire_unlocks,
        "wing_unlocks": wing_unlocks,
        "stamp_unlocks": stamp_unlocks,
        "dlc_unlocks": dlc_unlocks,
    }

    collection.find_one_and_replace({"pid": pid}, document, upsert=True)

    return True


class MK8RankingServer(CommonRankingServer):

    def __init__(self,
                 settings,
                 rankings_db: Collection,
                 redis_instance: redis.client.Redis,
                 commondata_db: Collection,
                 common_data_handler: Callable[[Collection, int, bytes, int], bool],
                 rankings_category: dict[int, bool],
                 tournaments_db: Collection,
                 tournaments_scores_db: Collection):

        super().__init__(settings, rankings_db, redis_instance, commondata_db, common_data_handler, rankings_category)

        self.tournaments_db = tournaments_db
        self.tournaments_scores_db = tournaments_scores_db

        self.methods.update({
            14: self.handle_get_competition_ranking_score,
            15: self.handle_upload_competition_ranking_score,
            16: self.handle_get_competition_info,
        })

    # ============= Utility functions  =============

    def get_redis_key_or_value(self, key: str, default: int = 0) -> int:
        value = self.redis_instance.get(key)
        return default if not value else int(value)

    def is_category_ordered_desc(self, category: int) -> bool:
        return False  # All MK8 categories are actually ASC.

    async def handle_get_competition_ranking_score(self, client, input, output):
        logger.info("RankingServerMK8.get_competition_ranking_score()")
        # --- request ---
        param = input.extract(CompetitionRankingGetScoreParam)
        response = await self.get_competition_ranking_score(client, param)

        # --- response ---
        if not isinstance(response, list):
            raise RuntimeError("Expected list, got %s" % response.__class__.__name__)
        output.list(response, output.add)

    async def handle_upload_competition_ranking_score(self, client, input, output):
        logger.info("RankingServerMK8.upload_competition_ranking_score()")
        # --- request ---
        param = input.extract(CompetitionRankingUploadScoreParam)
        response = await self.upload_competition_ranking_score(client, param)

        # --- response ---
        if not isinstance(response, bool):
            raise RuntimeError("Expected bool, got %s" % response.__class__.__name__)
        output.bool(response)

    async def handle_get_competition_info(self, client, input, output):
        logger.info("RankingServerMK8.get_competition_info()")
        # --- request ---
        param = input.extract(CompetitionRankingInfoGetParam)
        response = await self.get_competition_info(client, param)

        # --- response ---
        if not isinstance(response, list):
            raise RuntimeError("Expected list, got %s" % response.__class__.__name__)
        output.list(response, output.add)

    # ============= Method implementations  =============

    async def get_competition_ranking_score(self, client, param: CompetitionRankingGetScoreParam) -> list[CompetitionRankingScoreInfo]:
        if (param.range.size > 5):
            raise common.RMCError("Core::InvalidArgument")

        tournament = self.tournaments_db.find_one({"id": param.id})
        if not tournament:
            raise common.RMCError("Ranking::InvalidArgument")

        season_id_cur = tournament["season_id"]
        season_id_min = season_id_cur - param.range.size
        if season_id_min < 1:
            season_id_min = 1

        total_scores = []

        num_seasons = season_id_cur - season_id_min + 1
        for i in range(num_seasons):
            season_scores = []
            season_id = i + season_id_min
            scores = list(self.tournaments_scores_db.find(
                {"tournament_id": tournament["id"], "season_id": season_id}).sort("score", pymongo.DESCENDING).limit(20))

            team_scores = [0, 0, 0, 0]
            if tournament["attributes"][4] == 2:
                team_scores[2] = self.get_redis_key_or_value("tournaments:participation:%d_%d_team0" % (param.id, season_id))
                team_scores[3] = self.get_redis_key_or_value("tournaments:participation:%d_%d_team1" % (param.id, season_id))
                team_scores[0] = self.get_redis_key_or_value("tournaments:scores:%d_%d_team0" % (param.id, season_id)) - team_scores[2]
                team_scores[1] = self.get_redis_key_or_value("tournaments:scores:%d_%d_team1" % (param.id, season_id)) - team_scores[3]

            for i in range(len(scores)):
                score = scores[i]
                obj = CompetitionRankingScoreData()
                obj.rank = i + 1
                obj.pid = score["pid"]
                obj.score = score["score"]
                obj.last_update = common.DateTime(score["last_update"])
                obj.team_id = score["team_id"]
                obj.metadata = score["metadata"]
                season_scores.append(obj)

            score_info = CompetitionRankingScoreInfo()
            score_info.team_scores = team_scores
            score_info.num_participants = self.get_redis_key_or_value("tournaments:participation:%d_%d_total" % (param.id, season_id))
            score_info.season_id = season_id
            score_info.scores = season_scores

            total_scores.append(score_info)

        # TODO
        return total_scores

    async def upload_competition_ranking_score(self, client, param: CompetitionRankingUploadScoreParam) -> bool:
        if len(param.metadata) > 0x100:
            raise common.RMCError("Core::InvalidArgument")

        tournament = self.tournaments_db.find_one({"id": param.id})
        if not tournament:
            raise common.RMCError("Ranking::InvalidArgument")

        new_score = {
            "pid": client.pid(),
            "tournament_id": param.id,
            "season_id": param.season_id,
            "score": param.score,
            "team_id": param.team_id,
            "team_score": param.team_score,
            "metadata": param.metadata,
            "last_update": common.DateTime.now().value()
        }

        old_score = self.tournaments_scores_db.find_one({
            "pid": client.pid(),
            "tournament_id": param.id,
            "season_id": param.season_id,
        })

        diff_score = param.score
        if old_score:
            diff_score -= old_score["score"]
            self.tournaments_scores_db.replace_one({"_id": old_score["_id"]}, new_score)
        else:
            self.tournaments_scores_db.insert_one(new_score)

            # Increment total participants and season participants count
            self.tournaments_db.update_one({"id": param.id}, {"$inc": {"total_participants": 1}})
            self.redis_instance.incr("tournaments:participation:%d_total" % (param.id), 1)
            self.redis_instance.incr("tournaments:participation:%d_%d_total" % (param.id, param.season_id), 1)

            # Increment total team participants and season team participants count
            if param.team_id in [0, 1]:
                self.redis_instance.incr("tournaments:participation:%d_team%d" % (param.id, param.team_id), 1)
                self.redis_instance.incr("tournaments:participation:%d_%d_team%d" % (param.id, param.season_id, param.team_id), 1)

            if param.season_id > tournament["season_id"]:
                self.tournaments_db.update_one({"id": param.id}, {"$set": {"season_id": param.season_id}})

        if param.team_id in [0, 1]:
            self.redis_instance.incr("tournaments:scores:%d_team%d" % (param.id, param.team_id), diff_score)
            self.redis_instance.incr("tournaments:scores:%d_%d_team%d" % (param.id, param.season_id, param.team_id), diff_score)

        return True

    async def get_competition_info(self, client, param: CompetitionRankingInfoGetParam) -> list[CompetitionRankingInfo]:
        if (param.range.size > 100):
            raise common.RMCError("Core::InvalidArgument")

        res = []
        query = {"attributes.0": 1, "attributes.12": {"$ne": 2}, "attributes.13": {"$ne": 2}}
        tournaments = self.tournaments_db.find(query).sort("total_participants", pymongo.DESCENDING).skip(param.range.offset).limit(param.range.size)

        for tournament in tournaments:
            info = CompetitionRankingInfo()
            info.id = tournament["id"]
            info.team_scores = [0, 0, 0, 0]
            info.num_participants = self.get_redis_key_or_value("tournaments:participation:%d_total" % (tournament["id"]))

            # If it's a team tournament
            if tournament["attributes"][4] == 2:
                info.team_scores[2] = self.get_redis_key_or_value("tournaments:participation:%d_team0" % (tournament["id"]))
                info.team_scores[3] = self.get_redis_key_or_value("tournaments:participation:%d_team1" % (tournament["id"]))
                info.team_scores[0] = self.get_redis_key_or_value("tournaments:scores:%d_team0" % (tournament["id"])) - info.team_scores[2]
                info.team_scores[1] = self.get_redis_key_or_value("tournaments:scores:%d_team1" % (tournament["id"])) - info.team_scores[3]

            res.append(info)

        return res
