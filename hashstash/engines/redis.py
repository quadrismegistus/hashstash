from . import *



# Global variables
_process_started = False
_container_id = None


class RedisHashStash(BaseHashStash):
    engine = 'redis'
    host = REDIS_HOST
    port = REDIS_PORT
    dbname = DEFAULT_DBNAME
    ensure_dir = False
    string_keys = True
    string_values = True

    def __init__(self, *args, host=None,port=None,**kwargs):
        super().__init__(*args, **kwargs)
        if host is not None: self.host = host
        if port is not None: self.port = port

        
    
    def get_db(self):
        from redis_dict import RedisDict
        log.debug(f"Connecting to Redis at {self.host}:{self.port}")
        name = (self.name+'/'+self.dbname).replace('/','.')
        return DictContext(RedisDict(namespace=name, host=self.host, port=self.port))




def start_redis_server(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, data_dir=DEFAULT_REDIS_DIR):
    global _process_started, _container_id

    if _process_started:
        return

    # Convert data_dir to absolute path
    abs_data_dir = os.path.abspath(data_dir)
    os.makedirs(abs_data_dir, exist_ok=True)

    try:
        # First, try to connect to Redis
        import redis
        redis_client = redis.Redis(host=host, port=port, db=db)
        redis_client.ping()
        _process_started = True
        logger.info("Redis server is already running and accessible")
        return
    except (redis.exceptions.ConnectionError, redis.exceptions.ResponseError) as e:
        logger.info(f"Unable to connect to Redis. Checking Docker container status. Error: {e}")

    try:
        # Check if a Redis container already exists
        result = subprocess.run(
            ['docker', 'ps', '-a', '--filter', f'name=redis-{port}', '--format', '{{.ID}}'],
            capture_output=True,
            text=True,
            check=True
        )
        existing_container = result.stdout.strip()

        if existing_container:
            logger.info(f"Existing Redis container found: {existing_container}")
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
                subprocess.run(['docker', 'start', existing_container], check=True)
                _container_id = existing_container
        else:
            logger.info("No existing Redis container found. Starting a new one.")
            result = subprocess.run(
                [
                    'docker', 'run', '-d',
                    '--name', f'redis-{port}',
                    '-p', f'{port}:{port}',
                    '-v', f'{abs_data_dir}:/data',  # Use absolute path here
                    '--restart', 'unless-stopped',  # Add this line
                    'redis',
                    'redis-server', '--appendonly', 'yes'
                ],
                capture_output=True,
                text=True,
                check=True
            )
            _container_id = result.stdout.strip()
            logger.info(f"Redis Docker container started with ID: {_container_id}")

        # Wait for Redis to be ready
        max_retries = 30
        for _ in range(max_retries):
            try:
                import redis
                redis_client = redis.Redis(host=host, port=port, db=db)
                redis_client.ping()
                _process_started = True
                logger.info("Redis server is ready to accept connections")
                return
            except (redis.exceptions.ConnectionError, redis.exceptions.ResponseError):
                time.sleep(1)
        
        raise TimeoutError("Redis server did not start within the expected time")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start Redis Docker container: {e.stderr}", exc_info=True)
        raise RuntimeError("Failed to start Redis Docker container")
    except Exception as e:
        logger.error(f"Unexpected error while starting Redis Docker container: {str(e)}", exc_info=True)
        raise

