import os
import signal
import logging
import redis
from rq import Worker, Queue, Connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Queue names to listen to
LISTEN = ['default']

# Get Redis URL from environment variable, with a default fallback
REDIS_URL = os.environ.get('REDISTOGO_URL', 'redis://localhost:6379')

# Create Redis connection
conn = redis.from_url(REDIS_URL)

def shutdown_worker(signum, frame):
    """Gracefully shutdown the worker on signal."""
    logger.info("Received shutdown signal. Exiting...")
    worker.stop()

if __name__ == '__main__':
    # Register signal handlers
    signal.signal(signal.SIGINT, shutdown_worker)
    signal.signal(signal.SIGTERM, shutdown_worker)

    with Connection(conn):
        worker = Worker(list(map(Queue, LISTEN)))
        logger.info("Worker started, listening on queues: %s", LISTEN)
        worker.work()
