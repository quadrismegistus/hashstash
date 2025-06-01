from . import *

# Global variables
_process_started = False
_container_id = None

DEFAULT_NEO4J_DIR = os.path.join(DEFAULT_ROOT_DIR, ".neo4j")
DEFAULT_NEO4J_PORT = 7687
DEFAULT_NEO4J_HTTP_PORT = 7474
DEFAULT_NEO4J_USERNAME = "neo4j"
DEFAULT_NEO4J_PASSWORD = "hashstash"

def get_db_name(dbname: str) -> str:
    """Convert a string dbname to a unique database name for Neo4j."""
    hash_value = hashlib.md5(dbname.encode()).hexdigest()
    return f"hashstash_{hash_value[:10]}" 

def stream_subprocess_output(process):
    for line in iter(process.stdout.readline, b''):
        sys.stdout.write(line.decode())
        sys.stdout.flush()

class Neo4jHashStash(BaseHashStash):
    engine = 'neo4j'
    host = 'localhost'
    port = DEFAULT_NEO4J_PORT
    ensure_dir = False
    string_keys = True
    string_values = True
    dbname = 'hashstash'
    username = DEFAULT_NEO4J_USERNAME
    password = DEFAULT_NEO4J_PASSWORD

    def __init__(self, *args, host=None, port=None, username=None, password=None, **kwargs):
        if host is not None: self.host = host
        if port is not None: self.port = port
        if username is not None: self.username = username
        if password is not None: self.password = password
        # force b64 True for neo4j
        self.b64 = True
        super().__init__(*args, **kwargs)

    def _get_namespace(self):
        """Create a unique namespace for this cache instance"""
        # Combine name, dbname, and a hash of the path for uniqueness
        namespace_parts = [self.name]
        if self.dbname:
            namespace_parts.append(self.dbname)
        namespace_str = '/'.join(namespace_parts)
        # Add path hash to ensure uniqueness for sub-caches
        path_hash = hashlib.md5(self.path.encode()).hexdigest()[:8]
        return f"{namespace_str}_{path_hash}".replace('/', '_')

    @log.debug
    def get_db(self):
        from neo4j import GraphDatabase
        
        uri = f"bolt://{self.host}:{self.port}"
        driver = GraphDatabase.driver(uri, auth=(self.username, self.password))
        
        # Test connection
        driver.verify_connectivity()
        
        return driver
    
    @staticmethod
    def _close_connection(driver):
        driver.close()

    def _get_session_and_database(self):
        """Get session with database context for queries"""
        db_name = get_db_name(self.dbname)
        return self.db.session(database=db_name)

    def _set(self, encoded_key, encoded_value):
        with self.db as driver:
            with driver.session() as session:
                # Use MERGE to create or update the node
                session.run(
                    "MERGE (n:HashStashNode {key: $key, namespace: $namespace}) "
                    "SET n.value = $value, n.updated = timestamp()",
                    key=encoded_key,
                    value=encoded_value,
                    namespace=self._get_namespace()
                )

    def _get(self, encoded_key):
        with self.db as driver:
            with driver.session() as session:
                result = session.run(
                    "MATCH (n:HashStashNode {key: $key, namespace: $namespace}) "
                    "RETURN n.value as value",
                    key=encoded_key,
                    namespace=self._get_namespace()
                )
                record = result.single()
                return record["value"] if record else None

    def _has(self, encoded_key):
        with self.db as driver:
            with driver.session() as session:
                result = session.run(
                    "MATCH (n:HashStashNode {key: $key, namespace: $namespace}) "
                    "RETURN count(n) as count",
                    key=encoded_key,
                    namespace=self._get_namespace()
                )
                record = result.single()
                return record["count"] > 0 if record else False

    def _del(self, encoded_key: Union[str, bytes]) -> None:
        with self.db as driver:
            with driver.session() as session:
                session.run(
                    "MATCH (n:HashStashNode {key: $key, namespace: $namespace}) "
                    "DELETE n",
                    key=encoded_key,
                    namespace=self._get_namespace()
                )

    def clear(self):
        with self.db as driver:
            with driver.session() as session:
                # First delete all relationships for nodes in this namespace
                session.run(
                    "MATCH (n:HashStashNode {namespace: $namespace})-[r]-() "
                    "DELETE r",
                    namespace=self._get_namespace()
                )
                # Then delete the nodes themselves
                session.run(
                    "MATCH (n:HashStashNode {namespace: $namespace}) "
                    "DELETE n",
                    namespace=self._get_namespace()
                )
        return self

    def __len__(self):
        with self.db as driver:
            with driver.session() as session:
                result = session.run(
                    "MATCH (n:HashStashNode {namespace: $namespace}) "
                    "RETURN count(n) as count",
                    namespace=self._get_namespace()
                )
                record = result.single()
                return record["count"] if record else 0

    def _keys(self):
        with self.db as driver:
            with driver.session() as session:
                result = session.run(
                    "MATCH (n:HashStashNode {namespace: $namespace}) "
                    "RETURN n.key as key",
                    namespace=self._get_namespace()
                )
                for record in result:
                    yield record["key"]
        
    @property
    def filesize(self):
        # For Neo4j, we estimate size based on node properties
        # This is an approximation since Neo4j doesn't directly expose storage size per namespace
        with self.db as driver:
            with driver.session() as session:
                result = session.run(
                    "MATCH (n:HashStashNode {namespace: $namespace}) "
                    "RETURN sum(size(n.key) + size(n.value)) as total_size",
                    namespace=self._get_namespace()
                )
                record = result.single()
                return record["total_size"] if record and record["total_size"] else 0

    def link(self, node1_key, node2_key, relation, **properties):
        """Create a relationship between two nodes in the graph."""
        encoded_key1 = self.encode_key(node1_key)
        encoded_key2 = self.encode_key(node2_key)
        
        with self.db as driver:
            with driver.session() as session:
                # Create relationship between existing nodes
                query = """
                MATCH (n1:HashStashNode {key: $key1, namespace: $namespace})
                MATCH (n2:HashStashNode {key: $key2, namespace: $namespace})
                MERGE (n1)-[r:%s]->(n2)
                SET r._timestamp = timestamp()
                """ % relation
                
                if properties:
                    query += ", " + ", ".join([f"r.{k} = ${k}" for k in properties.keys()])
                
                query += " RETURN r"
                
                params = {
                    'key1': encoded_key1,
                    'key2': encoded_key2,
                    'namespace': self._get_namespace()
                }
                if properties:
                    params.update(properties)
                    
                session.run(query, **params)

    def get_links(self, node_key, direction='both', relation=None):
        """Get all relationships for a node."""
        encoded_key = self.encode_key(node_key)
        
        with self.db as driver:
            with driver.session() as session:
                if direction == 'outgoing':
                    pattern = "(n)-[r]->(m)"
                elif direction == 'incoming':
                    pattern = "(m)-[r]->(n)"
                else:  # both
                    pattern = "(n)-[r]-(m)"
                
                rel_filter = f":{relation}" if relation else ""
                
                query = f"""
                MATCH (n:HashStashNode {{key: $key, namespace: $namespace}})
                MATCH {pattern}
                WHERE m.namespace = $namespace
                RETURN type(r) as relation, m.key as connected_key, r as relationship
                """
                
                result = session.run(query, key=encoded_key, namespace=self._get_namespace())
                
                links = []
                for record in result:
                    links.append({
                        'relation': record['relation'],
                        'connected_key': self.decode_key(record['connected_key']),
                        **dict(record['relationship'])
                    })
                return links

    def unlink(self, node1_key, node2_key, relation=None):
        """Remove relationship between two nodes."""
        encoded_key1 = self.encode_key(node1_key)
        encoded_key2 = self.encode_key(node2_key)
        
        with self.db as driver:
            with driver.session() as session:
                rel_filter = f":{relation}" if relation else ""
                
                query = f"""
                MATCH (n1:HashStashNode {{key: $key1, namespace: $namespace}})
                MATCH (n2:HashStashNode {{key: $key2, namespace: $namespace}})
                MATCH (n1)-[r{rel_filter}]-(n2)
                DELETE r
                """
                
                session.run(query, key1=encoded_key1, key2=encoded_key2, namespace=self._get_namespace())

    def query_graph(self, cypher_query, **params):
        """Execute a custom Cypher query on the graph."""
        with self.db as driver:
            with driver.session() as session:
                # Add namespace filter to params if not already present
                if 'namespace' not in params:
                    params['namespace'] = self._get_namespace()
                
                result = session.run(cypher_query, **params)
                return [dict(record) for record in result]

    def debug_show_all_nodes(self):
        """Debug method to show all nodes in the database"""
        with self.db as driver:
            with driver.session() as session:
                result = session.run(
                    "MATCH (n:HashStashNode) "
                    "RETURN n.namespace as namespace, n.key as key, n.value as value"
                )
                nodes = []
                for record in result:
                    nodes.append({
                        'namespace': record['namespace'],
                        'key': record['key'],
                        'value': record['value']
                    })
                return nodes

    def get_link(self, node1_key, node2_key, relation):
        """Get properties of a specific relationship between two nodes."""
        encoded_key1 = self.encode_key(node1_key)
        encoded_key2 = self.encode_key(node2_key)
        
        with self.db as driver:
            with driver.session() as session:
                query = f"""
                MATCH (n1:HashStashNode {{key: $key1, namespace: $namespace}})
                MATCH (n2:HashStashNode {{key: $key2, namespace: $namespace}})
                MATCH (n1)-[r:{relation}]-(n2)
                RETURN r as relationship
                LIMIT 1
                """
                
                result = session.run(query, key1=encoded_key1, key2=encoded_key2, namespace=self._get_namespace())
                record = result.single()
                
                if record:
                    return dict(record['relationship'])
                else:
                    return None

def start_neo4j_server(host='localhost', port=DEFAULT_NEO4J_PORT, http_port=DEFAULT_NEO4J_HTTP_PORT, 
                      username=DEFAULT_NEO4J_USERNAME, password=DEFAULT_NEO4J_PASSWORD, 
                      data_dir=DEFAULT_NEO4J_DIR):
    global _process_started, _container_id

    if _process_started:
        return

    # Convert data_dir to absolute path
    abs_data_dir = os.path.abspath(data_dir)
    os.makedirs(abs_data_dir, exist_ok=True)

    try:
        # First, try to connect to Neo4j
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=(username, password))
        driver.verify_connectivity()
        driver.close()
        _process_started = True
        logger.info("Neo4j server is already running and accessible")
        return
    except Exception as e:
        logger.info(f"Unable to connect to Neo4j. Checking Docker container status. Error: {e}")

    try:
        # Check if a Neo4j container already exists
        result = subprocess.run(
            ['docker', 'ps', '-a', '--filter', f'name=neo4j-{port}', '--format', '{{.ID}}'],
            capture_output=True,
            text=True,
            check=True
        )
        existing_container = result.stdout.strip()

        if existing_container:
            logger.info(f"Existing Neo4j container found: {existing_container}")
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
                process = subprocess.Popen(['docker', 'start', existing_container], 
                                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                stream_subprocess_output(process)
                _container_id = existing_container
        else:
            logger.info("No existing Neo4j container found. Starting a new one.")
            process = subprocess.Popen(
                [
                    'docker', 'run', '-d',
                    '--name', f'neo4j-{port}',
                    '-p', f'{port}:{port}',  # Bolt port
                    '-p', f'{http_port}:{http_port}',  # HTTP port
                    '-v', f'{abs_data_dir}:/data',
                    '-v', f'{abs_data_dir}/logs:/logs',
                    '-e', f'NEO4J_AUTH={username}/{password}',
                    '--restart', 'unless-stopped',
                    'neo4j:latest'
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )
            stream_subprocess_output(process)
            _container_id = process.stdout.read().decode().strip()
            logger.info(f"Neo4j Docker container started with ID: {_container_id}")

        # Wait for Neo4j to be ready
        max_retries = 60 * 3  # Neo4j takes longer to start than MongoDB
        for _ in range(max_retries):
            try:
                from neo4j import GraphDatabase
                driver = GraphDatabase.driver(f"bolt://{host}:{port}", auth=(username, password))
                driver.verify_connectivity()
                driver.close()
                _process_started = True
                logger.info("Neo4j server is ready to accept connections")
                return
            except Exception:
                time.sleep(2)  # Check every 2 seconds
                sys.stdout.write(".")
                sys.stdout.flush()
        
        raise TimeoutError("Neo4j server did not start within the expected time")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start Neo4j Docker container: {e.stderr}", exc_info=True)
        raise RuntimeError("Failed to start Neo4j Docker container")
    except Exception as e:
        logger.error(f"Unexpected error while starting Neo4j Docker container: {str(e)}", exc_info=True)
        raise