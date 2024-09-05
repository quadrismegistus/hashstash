from . import *
import hashlib
import sys
import subprocess
import time

# Global variables
_process_started = False
_container_id = None

MAX_MONGO_DB = 64  # MongoDB can handle many more databases than Redis

def get_db_name(dbname: str) -> str:
    """Convert a string dbname to a unique database name for MongoDB."""
    hash_value = hashlib.md5(dbname.encode()).hexdigest()
    return f"hashstash_{hash_value[:10]}"

def stream_subprocess_output(process):
    for line in iter(process.stdout.readline, b''):
        sys.stdout.write(line.decode())
        sys.stdout.flush()

class MongoHashStash(BaseHashStash):
    engine = 'mongo'
    host = 'localhost'
    port = 27017
    ensure_dir = False
    string_keys = True
    string_values = True

    def __init__(self, *args, host=None, port=None, **kwargs):
        super().__init__(*args, **kwargs)
        if host is not None: self.host = host
        if port is not None: self.port = port
        # force b64 True for mongo
        self.b64 = True
        
    @log.debug
    def get_db(self):
        from pymongo import MongoClient
        client = MongoClient(host=self.host, port=self.port)
        db = client[get_db_name(self.dbname)]
        coll_name = (self.name+'/'+self.dbname).replace('/','.')
        coll = db[coll_name]
        coll._client = client
        coll._db = db
        return coll
    
    @staticmethod
    def _close_connection(coll):
        coll._client.close()

    def _set(self, encoded_key, encoded_value):
        with self.db as db:
            db.update_one(
                {"_id": encoded_key},
                {"$set": {"value": encoded_value}},
                upsert=True
            )

    def _get(self, encoded_key):
        with self.db as db:
            result = db.find_one({"_id": encoded_key})
        return result["value"] if result else None

    def _has(self, encoded_key):
        with self.db as db:
            return db.count_documents({"_id": encoded_key}, limit=1) > 0

    def _del(self, encoded_key: Union[str, bytes]) -> None:
        with self.db as db:
            db.delete_one({"_id": encoded_key})

    def clear(self):
        with self.db as db:
            db.drop()
        return self

    def __len__(self):
        with self.db as db:
            return db.count_documents({})

    def _keys(self):
        with self.db as db:
            return (doc["_id"] for doc in db.find({}, {"_id": 1}))
        
    @property
    def filesize(self):
        return sum(bytesize(k) + bytesize(self._get(k)) for k in self._keys())

def start_mongo_server(host='localhost', port=27017, dbname=DEFAULT_DBNAME, data_dir=DEFAULT_MONGO_DIR):
    global _process_started, _container_id

    if _process_started:
        return

    # Convert data_dir to absolute path
    abs_data_dir = os.path.abspath(data_dir)
    os.makedirs(abs_data_dir, exist_ok=True)

    try:
        # First, try to connect to MongoDB
        from pymongo import MongoClient
        client = MongoClient(host=host, port=port, serverSelectionTimeoutMS=2000)
        client.server_info()  # Will raise an exception if cannot connect
        _process_started = True
        # logger.info("MongoDB server is already running and accessible")
        return
    except Exception as e:
        logger.info(f"Unable to connect to MongoDB. Checking Docker container status. Error: {e}")

    try:
        # Check if a MongoDB container already exists
        result = subprocess.run(
            ['docker', 'ps', '-a', '--filter', f'name=mongo-{port}', '--format', '{{.ID}}'],
            capture_output=True,
            text=True,
            check=True
        )
        existing_container = result.stdout.strip()

        if existing_container:
            logger.info(f"Existing MongoDB container found: {existing_container}")
            # Check if the container is running
            result = subprocess.run(
                ['docker', 'inspect', '-f', '{{.State.Running}}', existing_container],
                capture_output=True,
                text=True,
                check=True
            )
            is_running = result.stdout.strip() == 'true'

            if is_running:
                logger.info("Existing container is already running. Using it.")
                _container_id = existing_container
            else:
                logger.info("Starting existing container.")
                process = subprocess.Popen(['docker', 'start', existing_container], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                stream_subprocess_output(process)
                _container_id = existing_container
        else:
            logger.info("No existing MongoDB container found. Starting a new one.")
            process = subprocess.Popen(
                [
                    'docker', 'run', '-d',
                    '--name', f'mongo-{port}',
                    '-p', f'{port}:{port}',
                    '-v', f'{abs_data_dir}:/data/db',
                    '--restart', 'unless-stopped',
                    'mongo'
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )
            stream_subprocess_output(process)
            _container_id = process.stdout.read().decode().strip()
            logger.info(f"MongoDB Docker container started with ID: {_container_id}")

        # Wait for MongoDB to be ready
        max_retries = 30
        for _ in range(max_retries):
            try:
                from pymongo import MongoClient
                client = MongoClient(host=host, port=port, serverSelectionTimeoutMS=2000)
                client.server_info()
                _process_started = True
                logger.info("MongoDB server is ready to accept connections")
                return
            except Exception:
                time.sleep(1)
                sys.stdout.write(".")
                sys.stdout.flush()
        
        raise TimeoutError("MongoDB server did not start within the expected time")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start MongoDB Docker container: {e.stderr}", exc_info=True)
        raise RuntimeError("Failed to start MongoDB Docker container")
    except Exception as e:
        logger.error(f"Unexpected error while starting MongoDB Docker container: {str(e)}", exc_info=True)
        raise