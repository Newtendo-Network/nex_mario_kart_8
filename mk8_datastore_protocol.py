from nintendo.nex import datastore, rmc, common
from pymongo.collection import Collection
from typing import Callable
import datetime
import pymongo

from nex_protocols_common_py.datastore_protocol import CommonDataStoreServer


class MK8DataStoreChangeMetaParam(common.Structure):
    def __init__(self):
        super().__init__()
        self.data_id = None
        self.modifies_flag = None
        self.name = None
        self.permission = datastore.DataStorePermission()
        self.delete_permission = datastore.DataStorePermission()
        self.period = None
        self.meta_binary = None
        self.tags = None
        self.update_password = None
        self.referred_count = None
        self.data_type = None
        self.status = None
        self.compare_param = datastore.DataStoreChangeMetaCompareParam()

    def check_required(self, settings, version):
        for field in ['data_id', 'modifies_flag', 'name', 'period', 'meta_binary', 'tags', 'update_password', 'referred_count', 'data_type', 'status']:
            if getattr(self, field) is None:
                raise ValueError("No value assigned to required field: %s" % field)

    def load(self, stream, version):
        self.data_id = stream.u64()
        self.modifies_flag = stream.u32()
        self.name = stream.string()
        self.permission = stream.extract(datastore.DataStorePermission)
        self.delete_permission = stream.extract(datastore.DataStorePermission)
        self.period = stream.u16()
        self.meta_binary = stream.qbuffer()
        self.tags = stream.list(stream.string)
        self.update_password = stream.u64()
        self.referred_count = stream.u32()
        self.data_type = stream.u16()
        self.status = stream.u8()
        self.compare_param = stream.extract(datastore.DataStoreChangeMetaCompareParam)

    def save(self, stream, version):
        self.check_required(stream.settings, version)
        stream.u64(self.data_id)
        stream.u32(self.modifies_flag)
        stream.string(self.name)
        stream.add(self.permission)
        stream.add(self.delete_permission)
        stream.u16(self.period)
        stream.qbuffer(self.meta_binary)
        stream.list(self.tags, stream.string)
        stream.u64(self.update_password)
        stream.u32(self.referred_count)
        stream.u16(self.data_type)
        stream.u8(self.status)
        stream.add(self.compare_param)


class MK8DataStoreSearchParam(common.Structure):
    def __init__(self):
        super().__init__()
        self.search_target = 1
        self.owner_ids = []
        self.owner_type = 0
        self.destination_ids = []
        self.data_type = 65535
        self.created_after = common.DateTime(671076024059)
        self.created_before = common.DateTime(671076024059)
        self.updated_after = common.DateTime(671076024059)
        self.updated_before = common.DateTime(671076024059)
        self.refer_data_id = 0
        self.tags = []
        self.result_order_column = 0
        self.result_order = 0
        self.result_range = common.ResultRange()
        self.result_option = 0
        self.minimal_rating_frequency = 0

    def check_required(self, settings, version):
        pass

    def load(self, stream, version):
        self.search_target = stream.u8()
        self.owner_ids = stream.list(stream.pid)
        self.owner_type = stream.u8()
        self.destination_ids = stream.list(stream.u64)
        self.data_type = stream.u16()
        self.created_after = stream.datetime()
        self.created_before = stream.datetime()
        self.updated_after = stream.datetime()
        self.updated_before = stream.datetime()
        self.refer_data_id = stream.u32()
        self.tags = stream.list(stream.string)
        self.result_order_column = stream.u8()
        self.result_order = stream.u8()
        self.result_range = stream.extract(common.ResultRange)
        self.result_option = stream.u8()
        self.minimal_rating_frequency = stream.u32()

    def save(self, stream, version):
        self.check_required(stream.settings, version)
        stream.u8(self.search_target)
        stream.list(self.owner_ids, stream.pid)
        stream.u8(self.owner_type)
        stream.list(self.destination_ids, stream.u64)
        stream.u16(self.data_type)
        stream.datetime(self.created_after)
        stream.datetime(self.created_before)
        stream.datetime(self.updated_after)
        stream.datetime(self.updated_before)
        stream.u32(self.refer_data_id)
        stream.list(self.tags, stream.string)
        stream.u8(self.result_order_column)
        stream.u8(self.result_order)
        stream.add(self.result_range)
        stream.u8(self.result_option)
        stream.u32(self.minimal_rating_frequency)


class MK8DataStoreServer(CommonDataStoreServer):
    def __init__(self,
                 settings,
                 s3_client,
                 s3_endpoint_domain: str,
                 s3_bucket: str,
                 datastore_db: Collection,
                 sequence_db: Collection,
                 head_object_by_key: Callable[[str], tuple[bool, int, str]],
                 calculate_s3_object_key: Callable[[Collection, rmc.RMCClient, int, int], str],
                 calculate_s3_object_key_ex: Callable[[Collection, int, int, int], str]):

        super().__init__(settings,
                         s3_client,
                         s3_endpoint_domain,
                         s3_bucket,
                         datastore_db,
                         sequence_db,
                         head_object_by_key,
                         calculate_s3_object_key,
                         calculate_s3_object_key_ex)

        self.methods[43] = self.handle_get_object_infos

    # ==================================================================================

    async def handle_change_meta(self, client, input, output):
        datastore.logger.info("DataStoreServer.change_meta()")
        # --- request ---
        param = input.extract(MK8DataStoreChangeMetaParam)
        await self.change_meta(client, param)

    async def handle_search_object(self, client, input, output):
        datastore.logger.info("DataStoreServer.search_object()")
        # --- request ---
        param = input.extract(MK8DataStoreSearchParam)
        response = await self.search_object(client, param)

        # --- response ---
        if not isinstance(response, datastore.DataStoreSearchResult):
            raise RuntimeError("Expected DataStoreSearchResult, got %s" % response.__class__.__name__)
        output.add(response)

    # ==================================================================================
