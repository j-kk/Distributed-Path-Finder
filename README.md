# Rust Pathfinder



Supported modes of connection between nodes:
- Redis
- ZMQ


Env vars
- GOOGLE_CLOUD_REGION
- GOOGLE_CLOUD_BUCKET
- GOOGLE_ACCESS_KEY
- GOOGLE_SECRET_KEY
- GROUP_ID
- REDIS_URL
- REDIS_CONNECTION_COUNT
- WORKER_COUNT

If utilising ZMQ connection mode, additional env vars must be set
- LISTEN_ADDR
- REPLY_ADDR
- ZMQ_MODE