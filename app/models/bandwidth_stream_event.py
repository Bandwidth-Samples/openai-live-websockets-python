"""
Pydantic models for Bandwidth streaming events and related data structures.
Used for validating and serializing Bandwidth WebSocket messages.
"""
from enum import Enum
from typing import List

from pydantic import BaseModel, Field, ConfigDict
from pydantic.alias_generators import to_camel


class TrackFormat(BaseModel):
    """
    Represents the format of an audio track in the Bandwidth stream.
    """
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    encoding: str = Field(default=None, alias="encoding")
    sample_rate: int = Field(default=None, alias="sampleRate")


class StreamTracks(BaseModel):
    """
    Represents a single track in the Bandwidth stream, including its format.
    """
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    name: str = Field(default=None, alias="name")
    media_format: TrackFormat = Field(default=None, alias="mediaFormat")


class StreamMetadata(BaseModel):
    """
    Metadata about the Bandwidth stream, including account and call IDs.
    """
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    account_id: str = Field(default=None, alias="accountId")
    call_id: str = Field(default=None, alias="callId")
    stream_id: str = Field(default=None, alias="streamId")
    stream_name: str = Field(default=None, alias="streamName")
    tracks: List[StreamTracks] = Field(default=None, alias="tracks")


class StreamEventType(str, Enum):
    """
    Enum of possible event types for Bandwidth streaming events.
    """
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    STREAM_STARTED = "start"
    STREAM_STOPPED = "stop"
    MEDIA = "media"
    PLAY_AUDIO = "playAudio"
    CLEAR = "clear"


class StreamMedia(BaseModel):
    """
    Represents media payloads (e.g., audio) sent over the Bandwidth stream.
    """
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    content_type: str = Field(default=None, alias="contentType")
    payload: str = Field(default=None, alias="payload")


class BandwidthStreamEvent(BaseModel):
    """
    Main event model for Bandwidth WebSocket messages.
    Used for all event types (start, stop, media, playAudio, clear).
    """
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    call_id: str = Field(default=None, alias="callId")
    event_type: StreamEventType = Field(default=None, alias="eventType")
    metadata: StreamMetadata = Field(default=None, alias="metadata")
    stream_params: dict = Field(default=None, alias="streamParams")
    payload: str = Field(default=None, alias="payload")
    media: StreamMedia = Field(default=None, alias="media")
