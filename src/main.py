import sys
from pathlib import Path

# Add the src directory to the sys.path
sys.path.append(str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
import asyncio
from psycopg_pool import AsyncConnectionPool
import os
import logging
import atexit
import json
from dotenv import load_dotenv
from googleapiclient.discovery import Resource


# Local imports
from database.connection import connect_to_database, init_database
from gmail_api import connection
from service import executions_service, actions_service, classifiers_service
from actions import defined_actions
from dto import models

load_dotenv(".env")
logger = logging.getLogger("gmail_automation")


def setup_logging():
    log_dir_path = Path(__file__).parent.parent / "logs"
    log_dir_path.mkdir(exist_ok=True)

    config_file = Path(__file__).parent.parent / "log_config.json"
    logging.config.dictConfig(json.loads(config_file.read_text()))
    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)


# Create a global pool instance
pool: AsyncConnectionPool | None = None
gmail_resource: Resource | None = None


async def init_db():
    """Initialize the async connection pool."""
    global pool
    pool = await connect_to_database(os.environ["POSTGRES_URL"])
    await init_database(pool)


async def get_pool():
    return pool


def init_gmail_resource():
    global gmail_resource
    gmail_resource = connection.refresh_credentials(
        os.environ.get("GMAIL_CREDENTIALS_PATH")
    )
    

def get_gmail_resource():
    return gmail_resource


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown event for database connection pool."""
    setup_logging()
    await init_db()
    init_gmail_resource()
    
    await actions_service.load_actions(pool, defined_actions)
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/api/v1/run_classifiers", response_model=models.Execution)
async def run_all_classifiers_in_batch(userId:str = "me", pool: AsyncConnectionPool = Depends(get_pool), gmail_resource: Resource = Depends(get_gmail_resource)):
    """Endpoint to run all classifiers execution."""
    execution = await executions_service.run_in_batch(pool, gmail_resource, userId)
    return execution


@app.post("/api/v1/classifiers", response_model=models.Classifier, tags=["Classifier"])
async def create_classifier(classifier: models.ClassifierCreate, pool: AsyncConnectionPool = Depends(get_pool)):
    """Endpoint to create a new classifier."""
    new_classifier = await classifiers_service.create_classifier(pool, classifier)
    return new_classifier


@app.get("/api/v1/classifiers/{classifier_id}", response_model=models.Classifier, tags=["Classifier"])
async def read_classifier(classifier_id: int, pool: AsyncConnectionPool = Depends(get_pool)):
    """Endpoint to get a classifier by ID."""
    classifier = await classifiers_service.get_classifier_by_id(pool, classifier_id)
    return classifier


@app.put("/api/v1/classifiers/{classifier_id}", response_model=models.Classifier, tags=["Classifier"])
async def update_classifier(classifier_id: int, classifier: models.ClassifierUpdate, pool: AsyncConnectionPool = Depends(get_pool)):
    """Endpoint to update a classifier by ID."""
    updated_classifier = await classifiers_service.update_classifier(pool, classifier_id, classifier)
    return updated_classifier


@app.delete("/api/v1/classifiers/{classifier_id}", response_model=models.Classifier, tags=["Classifier"])
async def delete_classifier(classifier_id: int, pool: AsyncConnectionPool = Depends(get_pool)):
    """Endpoint to delete a classifier by ID."""
    deleted_classifier = await classifiers_service.delete_classifier(pool, classifier_id)
    return deleted_classifier