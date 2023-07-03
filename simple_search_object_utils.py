from nintendo.nex import common, matchmaking_mk8d
from pymongo.collection import Collection
from anynet import streams
import struct
from datetime import datetime


def get_attr_if_exist(obj, attr: str, default):
    if hasattr(obj, attr):
        return obj[attr]
    return default


def get_next_tournament_id(col: Collection) -> int:
    tid = col.find_one_and_update({"_id": "tournament_id"}, {"$inc": {"seq": 1}})["seq"]
    if tid == 0xffffffff:
        col.update_one({"_id": "tournament_id"}, {"$set": {"seq": 0}})

    return tid


def simple_search_date_time_attribute_to_document(obj: matchmaking_mk8d.SimpleSearchDateTimeAttribute) -> dict:
    res = {}
    res["start_daytime"] = obj.start_daytime
    res["end_daytime"] = obj.end_daytime
    res["start_time"] = obj.start_time
    res["end_time"] = obj.end_time
    res["start_datetime"] = obj.start_datetime.value()
    res["end_datetime"] = obj.end_datetime.value()
    return res


def simple_search_object_to_document(obj: matchmaking_mk8d.SimpleSearchObject) -> dict:
    res = {}
    res["id"] = obj.id
    res["owner"] = obj.owner
    res["attributes"] = obj.attributes
    res["metadata"] = obj.metadata
    res["community_id"] = obj.community_id
    res["community_code"] = obj.community_code
    res["datetime"] = simple_search_date_time_attribute_to_document(obj.datetime)
    return res


def simple_search_date_time_attribute_from_document(obj: dict) -> matchmaking_mk8d.SimpleSearchDateTimeAttribute:
    res = matchmaking_mk8d.SimpleSearchDateTimeAttribute()
    res.start_daytime = obj["start_daytime"]
    res.end_daytime = obj["end_daytime"]
    res.start_time = obj["start_time"]
    res.end_time = obj["end_time"]
    res.start_datetime = common.DateTime(obj["start_datetime"])
    res.end_datetime = common.DateTime(obj["end_datetime"])
    return res


def simple_search_object_from_document(obj: dict) -> matchmaking_mk8d.SimpleSearchObject:
    res = matchmaking_mk8d.SimpleSearchObject()
    res.id = obj["id"]
    res.owner = obj["owner"]
    res.attributes = obj["attributes"]
    res.metadata = obj["metadata"]
    res.community_id = obj["community_id"]
    res.community_code = obj["community_code"]
    res.datetime = simple_search_date_time_attribute_from_document(obj["datetime"])
    return res


def get_query_filters_from_search_conditions(conditions: list[matchmaking_mk8d.SimpleSearchCondition]) -> dict:
    out = {}
    for i in range(len(conditions)):
        if conditions[i].operator != 0:
            if conditions[i].operator == 1:
                out["attributes." + str(i)] = {"$eq": conditions[i].value}
            elif conditions[i].operator == 2:
                out["attributes." + str(i)] = {"$gt": conditions[i].value}
            elif conditions[i].operator == 3:
                out["attributes." + str(i)] = {"$lt": conditions[i].value}
            elif conditions[i].operator == 4:
                out["attributes." + str(i)] = {"$gte": conditions[i].value}
            elif conditions[i].operator == 5:
                out["attributes." + str(i)] = {"$lte": conditions[i].value}
            else:
                raise common.RMCError("Core::InvalidArgument")

    return out


def verify_simple_search_param_type(obj: matchmaking_mk8d.SimpleSearchParam):
    if len(obj.community_code) > 12:
        raise common.RMCError("Core::InvalidArgument")

    if obj.range.size > 100:
        raise common.RMCError("Core::InvalidArgument")


def verify_simple_search_object_type(obj: matchmaking_mk8d.SimpleSearchObject):
    if len(obj.attributes) != 20:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[0] not in [1, 2]:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[2] > 5:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[3] == 0 or obj.attributes[3] > 8:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[4] not in [1, 2]:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[5] not in [1, 2, 3]:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[6] not in [1, 2]:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[7] not in [1, 2]:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[8] == 0 or obj.attributes[8] > 9:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[9] > 4:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[10] not in [1, 2]:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[11] not in [1, 2, 3, 4]:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[12] not in [1, 2]:
        raise common.RMCError("Core::InvalidArgument")

    if obj.attributes[13] not in [1, 2]:
        raise common.RMCError("Core::InvalidArgument")

    try:
        metadata = TournamentMetadata(obj.metadata)
        metadata.parse()
    except Exception as e:
        raise common.RMCError("Core::InvalidArgument")


class ChunkData:
    def __init__(self, data: bytes, max_id: int = 12):
        self.buffer = data
        self.data: dict[int, bytes] = {}
        self.max_id = max_id

    def parse(self):
        stream = streams.StreamIn(self.buffer, ">")

        magic = stream.u16()
        if magic != 0x5a5a:
            raise ValueError("Wrong magic")

        id = stream.u8()
        while id != 255:

            if id > self.max_id:
                raise ValueError("Invalid ID")

            size = stream.u16()
            data = stream.read(size)
            self.data[id] = data
            id = stream.u8()


class TournamentMetadata:
    def __init__(self, data: bytes):
        self.chunk_data = ChunkData(data)
        self.revision = 0
        self.name = ""
        self.description = ""
        self.red_team = ""
        self.blue_team = ""
        self.repeat_type = 0
        self.gameset_num = 0
        self.icon_type = 0
        self.battle_time = 0
        self.update_date = 0
        self.version = 0

    def parse(self):
        self.chunk_data.parse()

        if self.chunk_data.data[0]:
            self.revision = struct.unpack(">B", self.chunk_data.data[0])[0]

        if self.chunk_data.data[2]:
            self.name = self.chunk_data.data[2].decode("utf-16be")[:-1]

        if self.chunk_data.data[4]:
            self.description = self.chunk_data.data[4].decode("utf-16be")[:-1]

        if self.chunk_data.data[7]:
            self.red_team = self.chunk_data.data[7].decode("utf-16be")[:-1]

        if self.chunk_data.data[8]:
            self.blue_team = self.chunk_data.data[8].decode("utf-16be")[:-1]

        if self.chunk_data.data[5]:
            self.repeat_type = struct.unpack(">I", self.chunk_data.data[5])[0]

        if self.chunk_data.data[6]:
            self.gameset_num = struct.unpack(">I", self.chunk_data.data[6])[0]

        if self.chunk_data.data[9]:
            self.battle_time = struct.unpack(">I", self.chunk_data.data[9])[0]

        if self.chunk_data.data[11]:
            self.update_date = struct.unpack(">I", self.chunk_data.data[11])[0]

        if self.chunk_data.data[3]:
            self.icon_type = struct.unpack(">B", self.chunk_data.data[3])[0]

        if self.chunk_data.data[1]:
            self.version = struct.unpack(">I", self.chunk_data.data[1])[0]


class NetworkCompeWeekTime:
    def __init__(self, value):
        self.minute = value % 100
        self.hour = value // 100
        self.day_week = value // 10000

    def __str__(self):
        return "%s %02d:%02d" % (str(self.day_week), self.hour, self.minute)

    @staticmethod
    def get_day_of_week(value):
        return [2, 3, 4, 5, 6, 0, 1][value >> 16]

    def to_localtime(self):
        now = datetime.now()
        utc_offset = now.astimezone().utcoffset()
        offset_minutes = int(utc_offset.total_seconds() / 60)

        local_hour = (self.hour + (offset_minutes // 60)) % 24
        local_minute = (self.minute + (offset_minutes % 60)) % 60

        day_increment = (self.hour + (offset_minutes // 60)) // 24
        local_day = (self.day_week + now.weekday() + day_increment) % 7

        return local_hour, local_minute, local_day


class NetworkCompeDate:
    def __init__(self, value):
        self.minute = (10 * (value % 10)) & 0xff
        self.hour = (value // 10 % 100) & 0xff
        self.day = (value // 1000 % 100) & 0xff
        self.month = ((value // 100000 % 100) & 0xff) - 1
        self.year = ((value // 10000000) & 0xffff) + 2000

    def __str__(self):
        return "%02d-%02d-%02d %02d:%02d" % (self.day, self.month, self.year, self.hour, self.minute)


class NetworkCompeTime:
    def __init__(self, value):
        self.minute = (value % 100) & 0xff
        self.hour = (value // 100) & 0xff

    def __str__(self):
        return "%02d:%02d" % (self.hour, self.minute)
