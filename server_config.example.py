from nintendo.nex import settings
import pymongo
import pymongo
import urllib.parse

GAME_SERVER_ID = 0x1010EB00
ACCESS_KEY = "25dbf96a"
NEX_VERSION = 30504

# NEX_SETTINGS = settings.load("friends")
NEX_SETTINGS = settings.default()
NEX_SETTINGS.configure(ACCESS_KEY, NEX_VERSION)
NEX_SETTINGS["prudp.resend_timeout"] = 1.5
NEX_SETTINGS["prudp.resend_limit"] = 3
NEX_SETTINGS["prudp.version"] = 1
NEX_SETTINGS["prudp.max_substream_id"] = 1


class MongoCredentials:
    def __init__(self, host: str, port: int, use_auth: bool = False, username: str = "", password: str = ""):
        self.host = host
        self.port = port
        self.use_auth = use_auth
        self.username = username
        self.password = password

    def connect(self):
        # Username and password must be percent-escaped
        db_use_auth = self.use_auth
        db_user = urllib.parse.quote_plus(self.username)
        db_pass = urllib.parse.quote_plus(self.password)
        db_host = self.host
        db_port = self.port

        if db_use_auth:
            db_uri = 'mongodb://%s:%s@%s:%d' % (db_user, db_pass, db_host, db_port)
        else:
            db_uri = 'mongodb://%s:%d' % (db_host, db_port)

        return pymongo.MongoClient(db_uri, serverSelectionTimeoutMS=3000)


class NEXConfig:
    def __init__(self):
        self.nex_host = "0.0.0.0"
        self.nex_auth_port = 1000
        self.nex_secure_port = 1001
        self.nex_secure_user_password = "abcdef123456"  # PLEASE, make this a real private password.
        self.nex_external_address = "147.147.147.147"  # Your external IP, for external clients to connect.

        self.friends_grpc_host = "123.123.123.123"
        self.friends_grpc_port = 1002
        self.friends_grpc_api_key = "abcdefghijklmnopqrstuvwxyz123456789"

        self.account_grpc_host = "124.124.124.124"
        self.account_grpc_port = 1003
        self.account_grpc_api_key = "abcdefghijklmnopqrstuvwxyz123456789"

        self.account_db_server = MongoCredentials(
            host="111.111.111.111",
            port=1003
        )

        self.account_database = "pretendo"

        self.pnid_collection = "pnids"
        self.nex_account_collection = "nexaccounts"

        self.game_db_server = MongoCredentials(
            host="222.222.222.222",
            port=1004
        )

        self.game_database = "mk8rewrite"

        self.sequence_collection = "counters"
        self.gatherings_collection = "gatherings"
        self.sessions_collection = "sessions"
        self.tournaments_collection = "tournaments"
        self.tournaments_score_collection = "tournaments_scores"
        self.ranking_common_data_collection = "commondata"
        self.rankings_score_collection = "rankings"
        self.secure_reports_collection = "secure_reports"
        self.datastore_collection = "datastore"

        self.s3_endpoint_domain = "..."
        self.s3_endpoint = "https://" + self.s3_endpoint_domain
        self.s3_access_key = "..."
        self.s3_secret = "..."
        self.s3_region = "..."
        self.bucket_name = "amkj"

        self.redis_uri = "redis://53.53.53.53:1005"  # redis://HOST[:PORT][?db=DATABASE[&password=PASSWORD]]


NEX_CONFIG = NEXConfig()
