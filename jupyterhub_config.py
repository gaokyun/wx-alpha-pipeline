import os
from urllib.parse import quote_plus
# from torrent_forward_fix import TorFwdFix if os.path.exists('torrent_forward_fix.py') else None

c = get_config()  # noqa: F821

# --- 1. Database Configuration ---
# Hardcoded to match your verified working setup
c.JupyterHub.db_url = "postgresql://airflow:airflow@postgres/jupyterhub"

c.JupyterHub.db_kwargs = {
    'connect_args': {'connect_timeout': 30},
    'pool_pre_ping': True
}

# --- 2. Spawner & Network Configuration ---
c.JupyterHub.spawner_class = 'dockerspawner.DockerSpawner'
c.DockerSpawner.image = 'custom-weather-jupyter:latest'

# # Hub Networking
# c.JupyterHub.hub_ip = '0.0.0.0'
# c.JupyterHub.bind_url = 'http://:8000' # The Proxy (What you browse)
# c.JupyterHub.hub_bind_url = 'http://0.0.0.0:8081' # The Internal API
# c.JupyterHub.hub_connect_ip = 'jupyterhub'
# c.JupyterHub.hub_port = 8081

# --- Networking Configuration ---
# The public-facing port (where the Proxy lives)
c.JupyterHub.ip = '0.0.0.0'
c.JupyterHub.port = 8000 
# The internal API port (where the Hub service lives)
# We use 8081 to avoid any potential conflict with Airflow's 8080
c.JupyterHub.hub_ip = '0.0.0.0'
c.JupyterHub.hub_port = 8080
# How the Proxy and Spawner find the Hub
c.JupyterHub.hub_connect_ip = 'jupyterhub' 
# Explicitly set the base URL to ensure redirects work
c.JupyterHub.base_url = '/'


# Spawner Networking
c.DockerSpawner.network_name = os.environ.get('DOCKER_NETWORK_NAME', 'default')
c.DockerSpawner.use_internal_ip = True
c.Spawner.ip = '0.0.0.0'

# Timeouts: Heavy custom images need more time to pull and initialize
c.Spawner.http_timeout = 180 
c.DockerSpawner.remove = False  # Keep False to allow 'docker logs' debugging

# --- 3. The "Airflow Entrypoint" Fix & Permissions ---
# extra_create_kwargs clears the image's default ENTRYPOINT ['airflow'] 
# and sets the correct user UID in one block.
c.DockerSpawner.extra_create_kwargs = {
    'user': '50000',
    'entrypoint': '' 
}

# This command now runs as the primary process instead of an airflow sub-command
# This tells Jupyter to execute your script specifically upon startup
c.DockerSpawner.cmd = [
    "jupyterhub-singleuser", 
    "--IPython.extra_extensions=['sql']", # Optional: pre-load SQL ext
    "--exec", "exec(open('/home/airflow/.ipython/profile_default/startup/00-duckdb-init.py').read())"
]

# Environment variables for OCI/AWS access
c.DockerSpawner.environment = {
    # FORCES IPython/Jupyter to recognize the home and profile path
    'IPYTHONDIR': '/home/airflow/.ipython',
    'JUPYTER_CONFIG_DIR': '/home/airflow/.jupyter',
    'HOME': '/home/airflow',  # Force IPython to look here
    'OCI_ACCESS_KEY': os.environ.get('OCI_ACCESS_KEY', ''),
    'OCI_SECRET_KEY': os.environ.get('OCI_SECRET_KEY', ''),
    'OCI_NAMESPACE': os.environ.get('OCI_NAMESPACE', 'idt2nq7cpbfu'),
    'OCI_REGION': os.environ.get('OCI_REGION', 'us-ashburn-1'),
    'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_ID', ''),
    'AWS_SECRET_ACCESS_KEY': os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
    'AWS_REGION': os.environ.get('AWS_REGION', 'us-east-1'),
    'ORACLE_MH_PASSWORD': os.environ.get('ORACLE_MH_PASSWORD', ''),
    'MYSQL_HOST': os.environ.get('MYSQL_HOST', ''),
    'MYSQL_USER': os.environ.get('MYSQL_USER', '')
}

# --- 4. Persistence ---
# Mount user directories for work persistence
notebook_dir = os.environ.get('DOCKER_NOTEBOOK_DIR', '/home/airflow/work')
c.DockerSpawner.notebook_dir = notebook_dir
c.DockerSpawner.volumes = { 'jupyterhub-user-{username}': notebook_dir }

c.DockerSpawner.volumes = {
    # 1. The standard persistent user volume (what you have now)
    'jupyterhub-user-{username}': notebook_dir,

    # 2. Add your Airflow Bind Mounts here
    '/home/airflow/dev/notebooks': {
        'bind': '/opt/airflow/notebooks',
        'mode': 'rw'
    },
    '/home/airflow/dev/wx-alpha-pipeline/data': {
        'bind': '/opt/airflow/data',
        'mode': 'rw'
    },
    '/home/airflow/.ssh': {
        'bind': '/opt/airflow/.ssh',
        'mode': 'ro'
    }
}

# Automatically remove containers when they are stopped
c.DockerSpawner.remove_containers = True

# Ensure the spawner uses the correct network name, not a hardcoded ID
# Replace 'jupyterhub_network' with the actual name of your docker network
c.DockerSpawner.network_name = 'wx-alpha-pipeline_default'

# 2. Automatically create the symlinks after the container starts
# This ensures they always appear in your Jupyter sidebar
c.DockerSpawner.post_start_cmd = 'bash -c "ln -snf /opt/airflow/notebooks /home/airflow/work/airflow_notebooks && ln -snf /opt/airflow/data /home/airflow/work/airflow_data"'

# --- 5. Authentication ---
c.JupyterHub.authenticator_class = 'nativeauthenticator.NativeAuthenticator'
c.Authenticator.admin_users = {'admin'}
c.Authenticator.allowed_users = {'admin', 'yunkgao', 'gaokyun'}
c.NativeAuthenticator.open_signup = True

# Tell JupyterHub to trust connections coming from the local Docker bridge gateway
c.JupyterHub.trusted_downstream_ips = ['127.0.0.1', 'localhost', '172.20.0.1']

# Clean up proxy startup command (drop the invalid redirect flag)
c.ConfigurableHTTPProxy.command = ['configurable-http-proxy']

# Safe conditional import block
import os
if os.path.exists('torrent_forward_fix.py'):
    from torrent_forward_fix import TorFwdFix

c.JupyterHub.tornado_settings = {
    'headers': {
        'Content-Security-Policy': "frame-ancestors 'self' https://*.yunkgao.com;",
    },
    # Crucial: Forces Tornado to evaluate and trust Cloudflare's X-Forwarded-For headers
    'trusted_proxies': True 
}