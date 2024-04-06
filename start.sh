#!/bin/bash

# Run Alembic migrations
# alembic upgrade head

# Start FastAPI applicationnohup uvicorn main:api --host 0.0.0.0 --port 5000

uvicorn main:api --host 0.0.0.0 --port 5000

#--data ''

