# Example configuration file

# Debug mode
DEBUG = False

# host and port. 127.0.0.1 will only serve locally - set to 0.0.0.0 (or iface IP) to have available externally.
HOST = '0.0.0.0'
PORT = 8765

# Number of workers. Each worker processes one job at a time.
WORKERS = 1

# Number of requests each worker should process before being restarted.
REQUESTS_PER_WORKER = 1000

# Secret key. This ensures only approved applications can use this. YOU MUST CHANGE THIS VALUE TO YOUR OWN SECRET!
SECRET = '8ba6d280d4ce9a416e9b604f3f0ebb'

