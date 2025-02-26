from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from uuid import UUID


class Classifier(BaseModel):
    classifier_id: int|None
    classifier_name: str
    gmail_query: str
    created_at: datetime
    updated_at: datetime|None = None


class ClassifierCreate(BaseModel):
    classifier_name: str
    gmail_query: str


class ClassifierUpdate(BaseModel):
    classifier_name: Optional[str] = None
    gmail_query: Optional[str] = None


class ClassifierExecution(BaseModel):
    classifier_execution_id: UUID
    execution_id: UUID
    classifier_id: int
    started_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    status: str = "RUNNING"


class Execution(BaseModel):
    execution_id: UUID
    started_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    status: str = "RUNNING"


class ActionTemplate(BaseModel):
    action_name: str
    action_description: Optional[str] = None
    format: str
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None


class ActionParameterTemplate(BaseModel):
    action_name: str
    parameter_name: str
    parameter_type: str
    parameter_is_nullable: bool
    parameter_default: str
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None


class ClassifierAction(BaseModel):
    classifier_action_id: int
    classifier_id: int
    action_name: str
    parameters: dict


class ClassifierActionCreate(BaseModel):
    classifier_id: int
    action_name: str
    parameters: dict


class ClassifierActionUpdate(BaseModel):
    action_name: Optional[str] = None
    parameters: Optional[dict] = None


class ClassifierMessageExecution(BaseModel):
    message_id: str
    classifier_execution_id: UUID
    started_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    status: str = "RUNNING"
