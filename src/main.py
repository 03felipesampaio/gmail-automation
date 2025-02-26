import sys
from pathlib import Path


# Add the src directory to the sys.path
sys.path.append(str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Depends, HTTPException
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
from gmail_api import connection, gmail_requests
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
async def run_all_classifiers_in_batch(
    userId: str = "me",
    pool: AsyncConnectionPool = Depends(get_pool),
    gmail_resource: Resource = Depends(get_gmail_resource),
):
    """Endpoint to run all classifiers execution."""
    execution = await executions_service.run_in_batch(pool, gmail_resource, userId)
    return execution


@app.get("/api/v1/{user_id}/labels", response_model=list[dict], tags=["User"])
async def get_user_labels(
    user_id: str, label_type: str|None = None, gmail_resource: Resource = Depends(get_gmail_resource)
):
    """Get user labels."""
    labels = gmail_requests.get_user_labels(gmail_resource, user_id)
    return labels


@app.get(
    "/api/v1/classifiers", response_model=list[models.Classifier], tags=["Classifier"]
)
async def read_all_classifiers(pool: AsyncConnectionPool = Depends(get_pool)):
    """Get all classifiers."""
    classifiers = await classifiers_service.get_classifiers(pool)
    return classifiers


@app.post("/api/v1/classifiers", response_model=models.Classifier, tags=["Classifier"])
async def create_classifier(
    classifier: models.ClassifierCreate, pool: AsyncConnectionPool = Depends(get_pool)
):
    """Endpoint to create a new classifier."""
    new_classifier = await classifiers_service.create_classifier(
        pool, classifier.classifier_name, classifier.gmail_query
    )
    return new_classifier


@app.get(
    "/api/v1/classifiers/{classifier_id}",
    response_model=models.Classifier,
    tags=["Classifier"],
)
async def read_classifier(
    classifier_id: int, pool: AsyncConnectionPool = Depends(get_pool)
):
    """Endpoint to get a classifier by ID."""
    classifier = await classifiers_service.get_classifier_by_id(pool, classifier_id)
    return classifier


@app.put(
    "/api/v1/classifiers/{classifier_id}",
    response_model=models.Classifier,
    tags=["Classifier"],
)
async def update_classifier(
    classifier_id: int,
    classifier: models.ClassifierUpdate,
    pool: AsyncConnectionPool = Depends(get_pool),
):
    """Endpoint to update a classifier by ID."""
    updated_classifier = await classifiers_service.update_classifier(
        pool, classifier_id, classifier.classifier_name, classifier.gmail_query
    )
    return updated_classifier


@app.delete(
    "/api/v1/classifiers/{classifier_id}",
    response_model=models.Classifier,
    tags=["Classifier"],
)
async def delete_classifier(
    classifier_id: int, pool: AsyncConnectionPool = Depends(get_pool)
):
    """Endpoint to delete a classifier by ID."""
    classifier = await classifiers_service.get_classifier_by_id(pool, classifier_id)
    if classifier is None:
        raise HTTPException(
            404,
            f"Failed to delete classifier with ID '{classifier_id}'. Classifier not found on database.",
        )
    deleted_classifier = await classifiers_service.delete_classifier(
        pool, classifier_id
    )
    return deleted_classifier


@app.get(
    "/api/v1/classifiers/{classifier_id}/messages",
    response_model=list[models.Message],
    tags=["Classifier", "Messages"],
)
async def read_classifier_messages(
    classifier_id: int,
    format: str = "minimal",
    gmail_user_id: str = "me",
    pool: AsyncConnectionPool = Depends(get_pool),
    gmail_resource: Resource = Depends(get_gmail_resource),
):
    """Endpoint to get messages from a classifier."""
    classifier = await classifiers_service.get_classifier_by_id(pool, classifier_id)
    if classifier is None:
        raise HTTPException(
            404,
            f"Failed to get messages for classifier with ID '{classifier_id}'. Classifier not found on database.",
        )

    # TODO: Implement pagination

    messages = await classifiers_service.get_classifier_messages(
        gmail_resource, gmail_user_id, classifier, format
    )
    return messages


@app.get(
    "/api/v1/actions", response_model=list[models.Action], tags=["Actions"]
)
async def read_all_actions(pool: AsyncConnectionPool = Depends(get_pool)):
    """Get all actions."""
    actions = await actions_service.get_all_actions(pool)
    return actions


@app.get(
    "/api/v1/actions/{action_name}",
    response_model=models.Action,
    tags=["Actions"],
)
async def read_action_by_name(
    action_name: str, pool: AsyncConnectionPool = Depends(get_pool)
):
    """Get an action by its name."""
    action = await actions_service.get_action_by_name(pool, action_name)
    return action


@app.get(
    "/api/v1/classifier_actions",
    response_model=list[models.ClassifierAction],
    tags=["Classifier actions"],
)
async def read_all_classifier_actions(
    classifier_id: int, pool: AsyncConnectionPool = Depends(get_pool)
):
    """Get all classifier actions."""
    classifier_actions = await actions_service.get_classifier_actions(
        pool, classifier_id
    )

    if not classifier_actions:
        raise HTTPException(
            404,
            f"Failed to get classifier actions for classifier ID '{classifier_id}'. Classifier not found on database.",
        )

    return classifier_actions


@app.post(
    "/api/v1/classifier_actions",
    response_model=models.ClassifierAction,
    tags=["Classifier actions"],
)
async def assign_classifier_action(
    classifier_action: models.ClassifierActionCreate,
    pool: AsyncConnectionPool = Depends(get_pool),
):
    """Endpoint to assign a new classifier action to a classifier."""
    classifier = await classifiers_service.get_classifier_by_id(
        pool, classifier_action.classifier_id
    )
    if classifier is None:
        raise HTTPException(
            404,
            f"Failed to assign action to classifier with ID '{classifier_action.classifier_id}'. Classifier not found on database.",
        )

    # action = await actions_service.get_action_by_id(pool, action_id)
    # if action is None:
    #     raise HTTPException(
    #         404,
    #         f"Failed to assign action to classifier with ID '{classifier_action.classifier_id}'. Action not found on database.",
    #     )

    new_classifier_action = await actions_service.assign_action_to_classifier(
        pool,
        classifier_action.classifier_id,
        classifier_action.action_name,
        classifier_action.parameters
    )
    return new_classifier_action


@app.put(
    "/api/v1/classifier_actions/{classifier_action_id}",
    response_model=models.ClassifierAction,
    tags=["Classifier actions"],
)
async def update_classifier_action(
    classifier_action_id: int,
    classifier_action: models.ClassifierActionUpdate,
    pool: AsyncConnectionPool = Depends(get_pool),
):
    """Endpoint to update a classifier action by ID."""
    updated_classifier_action = await actions_service.update_classifier_action(
        pool, classifier_action_id, classifier_action.parameters
    )
    return updated_classifier_action


@app.delete(
    "/api/v1/classifier_actions/{classifier_action_id}",
    response_model=models.ClassifierAction,
    tags=["Classifier actions"],
)
async def delete_classifier_action(
    classifier_action_id: int, pool: AsyncConnectionPool = Depends(get_pool)
):
    """Endpoint to delete a classifier action by ID."""
    deleted_classifier_action = await actions_service.delete_classifier_action(
        pool, classifier_action_id
    )
    return deleted_classifier_action
