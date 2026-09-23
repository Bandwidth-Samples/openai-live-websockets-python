import asyncio
import base64
import http
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env", override=True)

from openai import AsyncOpenAI
from openai.resources.live.live import AsyncLiveConnection

from bandwidth import Configuration, ApiClient, CallsApi
from bandwidth.models import InitiateCallback, DisconnectCallback
from bandwidth.models.bxml import PhoneNumber, StartStream, StopStream, Transfer, Bxml
from pydantic import BaseModel
from rich import inspect
from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.text import Text
from fastapi import FastAPI, HTTPException, Response, WebSocket
import uvicorn

from models import BandwidthStreamEvent, StreamEventType, StreamMedia

# Load environment variables
console = Console()
try:
    BW_ACCOUNT = os.environ["BW_ACCOUNT_ID"]
    BW_CLIENT_ID = os.environ["BW_CLIENT_ID"]
    BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    TRANSFER_TO = os.environ["TRANSFER_TO"]
    BASE_URL = os.environ["BASE_URL"]
    LOG_LEVEL = os.environ["LOG_LEVEL"].upper()
    LOCAL_PORT = int(os.environ.get("LOCAL_PORT", 3000))
except KeyError as e:
    msg = Text(" Missing environment variables! ", style="bold white on red")
    details = f"Required key not set: [yellow]{e.args[0]}[/yellow]\n\n"
    details += "Make sure the following variables are defined:\n"
    details += "[cyan]BW_ACCOUNT_ID, BW_CLIENT_ID, BW_CLIENT_SECRET, OPENAI_API_KEY, TRANSFER_TO, BASE_URL, LOG_LEVEL, LOCAL_PORT[/cyan]"
    console.print(Panel(details, title=msg, expand=False, border_style="red"))
    sys.exit(1)

# Configure Logger
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(levelname)s %(asctime)s: %(message)s",
    datefmt="[%X]",
)
for name in ["websockets", "asyncio", "urllib3", "uvicorn", "fastapi", "openai", "httpx"]:
    logging.getLogger(name).setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# Bandwidth Client
bandwidth_config = Configuration(
    client_id=BW_CLIENT_ID,
    client_secret=BW_CLIENT_SECRET
)
bandwidth_client = ApiClient(bandwidth_config)
bandwidth_voice_api_instance = CallsApi(bandwidth_client)

# OpenAI Live Client
openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# OpenAI Live API Settings
OPENAI_LIVE_MODEL = "gpt-live-1"
OPENAI_RESPONSES_MODEL = "gpt-5.6-terra"
AGENT_VOICE = "alloy"
with open("sample-prompt.md", "r") as file:
    AGENT_PROMPT = file.read()

TOOLS = [
    {
        "type": "function",
        "name": "transfer_call",
        "description": "Transfer the call to a live human agent. ONLY call this when the caller uses explicit phrases like 'speak to a human', 'transfer me', 'get me an agent', 'talk to a person', or 'get me a manager'. Never call this for questions, information requests, or searches — use web_search for those.",
        "parameters": {"type": "object", "properties": {}}
    },
    {
        "type": "web_search",
    }
]

# Initialize FastAPI app
app = FastAPI()

# Active OpenAI Live connections keyed by call_id
# ponytail: module-level dict; per-instance storage if multi-worker
call_sessions: dict[str, AsyncLiveConnection] = {}
call_start_times: dict[str, datetime] = {}


def _print_transcript(speaker: str, text: str) -> None:
    """
    Print a complete transcript utterance with rich styling.
    :param speaker: 'input' (caller) or 'output' (AI)
    :param text: The complete utterance text
    :return: None
    """
    if speaker == "input":
        console.print(f"[bold cyan]  Caller[/bold cyan] │ {text}", highlight=False)
    else:
        console.print(f"[bold magenta]   Agent[/bold magenta] │ {text}", highlight=False)


def _print_call_start(call_id: str) -> None:
    """Print a call start panel with call ID and timestamp."""
    now = datetime.now().strftime("%H:%M:%S")
    console.print(Panel(
        f"[bold white]{call_id}[/bold white]   [dim]{now}[/dim]",
        title="[bold blue] Inbound Call [/bold blue]",
        border_style="blue",
    ))


def _print_call_end(call_id: str) -> None:
    """Print a call end panel with duration."""
    start = call_start_times.pop(call_id, None)
    if start:
        elapsed = int((datetime.now() - start).total_seconds())
        duration = f"{elapsed // 60}:{elapsed % 60:02d}"
        body = Text.assemble(
            ("Duration  ", "dim"),
            (duration, "bold white"),
        )
    else:
        body = Text("Session closed", style="dim")
    console.print(Panel(body, title="[bold red] Call Ended [/bold red]", border_style="dim red"))


class HoldRequest(BaseModel):
    """Request body for the hold/unhold endpoint."""

    call_id: str
    hold: bool


def log_inspect(obj, label=None):
    """
    Log the inspection of an object if debug logging is enabled.
    :param obj: The object to inspect
    :param label: An optional label for the inspection
    :return: None
    """
    if logger.isEnabledFor(logging.DEBUG):
        inspect(obj, title=label or repr(obj))


async def initialize_openai_session(connection: AsyncLiveConnection):
    """
    Initialize the OpenAI Live session. Sends session.start, waits for session.started,
    then triggers the opening greeting.
    :param connection: The OpenAI Live connection
    :return: None
    """
    await connection.session.start(session={
        "model": OPENAI_LIVE_MODEL,
        "instructions": AGENT_PROMPT,
        "audio": {
            "format": {"type": "audio/pcmu", "rate": 8000},
            "output": {"voice": AGENT_VOICE}
        },
        "delegation": {
            "type": "responses",
            "responses": {
                "model": OPENAI_RESPONSES_MODEL,
                "instructions": AGENT_PROMPT,
                "tools": TOOLS,
                "tool_choice": "auto"
            }
        }
    })
    async for event in connection:
        if event.type == "session.started":
            console.print(Rule("[dim green]AI ready[/dim green]"))
            break
        if event.type == "error":
            raise RuntimeError(f"OpenAI session start failed: {getattr(event, 'error', event)}")

    await connection.response.create()


async def receive_from_bandwidth_ws(bandwidth_websocket: WebSocket, connection: AsyncLiveConnection):
    """
    Receive messages from Bandwidth WebSocket and forward audio to OpenAI Live.
    :param bandwidth_websocket: The Bandwidth WebSocket connection
    :param connection: The OpenAI Live connection
    :return: None
    """
    bw_call_id: str | None = None
    try:
        async for message in bandwidth_websocket.iter_json():
            event = BandwidthStreamEvent.model_validate(message)
            match event.event_type:
                case StreamEventType.STREAM_STARTED:
                    bw_call_id = event.metadata.call_id
                    call_start_times[bw_call_id] = datetime.now()
                    _print_call_start(bw_call_id)
                case StreamEventType.MEDIA:
                    await connection.session.input_audio.append(audio=event.payload)
                case StreamEventType.STREAM_STOPPED:
                    return
                case _:
                    logger.warning(f"Unhandled event type: {event.event_type}")
    except Exception:
        pass
    finally:
        _cid = bw_call_id or ""
        _print_call_end(_cid)
        try:
            await bandwidth_websocket.close()
        except Exception:
            pass


async def receive_from_openai_ws(connection: AsyncLiveConnection, bandwidth_websocket: WebSocket, call_id: str):
    """
    Receive events from OpenAI Live and forward audio to Bandwidth.
    Transcript deltas are buffered per speaker and flushed when the speaker switches,
    so each printed line is a complete utterance rather than a word fragment.
    :param connection: The OpenAI Live connection
    :param bandwidth_websocket: The Bandwidth WebSocket connection
    :param call_id: The Bandwidth call ID
    :return: None
    """
    buf: dict[str, str] = {"input": "", "output": ""}
    last_speaker: str | None = None

    def flush(speaker: str) -> None:
        text = buf[speaker].strip()
        if text:
            _print_transcript(speaker, text)
            buf[speaker] = ""

    try:
        async for event in connection:
            match event.type:
                case "session.output_audio.delta":
                    delta = getattr(event, "delta", None)
                    if delta:
                        audio_payload = base64.b64encode(base64.b64decode(delta)).decode("utf-8")
                        media = StreamMedia(content_type="audio/pcmu", payload=audio_payload)
                        play_audio_event = BandwidthStreamEvent(
                            event_type=StreamEventType.PLAY_AUDIO, media=media
                        )
                        await bandwidth_websocket.send_text(
                            play_audio_event.model_dump_json(by_alias=True, exclude_none=True)
                        )
                case "session.input_transcript.delta":
                    delta = getattr(event, "delta", "")
                    if last_speaker == "output":
                        flush("output")
                    buf["input"] += delta
                    last_speaker = "input"
                case "session.output_transcript.delta":
                    delta = getattr(event, "delta", "")
                    if last_speaker == "input":
                        flush("input")
                    buf["output"] += delta
                    last_speaker = "output"
                    if buf["output"].rstrip().endswith((".", "!", "?", "…")):
                        flush("output")
                case "response.event":
                    await handle_response_event(event, connection, call_id)
                case "session.closed":
                    if last_speaker:
                        flush(last_speaker)
                    return
                case "error":
                    logger.error(f"OpenAI Error: {getattr(event, 'error', event)}")
                case "session.usage.updated":
                    pass  # billing telemetry, not actionable
                case _:
                    logger.debug(f"Unhandled OpenAI event: {event.type}")
    except Exception as e:
        msg = str(e)
        if msg:
            logger.error(f"OpenAI connection error: {msg}")


async def handle_response_event(event, connection: AsyncLiveConnection, call_id: str):
    """
    Handle response.event messages forwarded from the Responses delegation backend.
    Dispatches on the nested event type; executes tool calls when they complete.
    :param event: The response.event from OpenAI Live
    :param connection: The OpenAI Live connection
    :param call_id: The Bandwidth call ID
    :return: None
    """
    backend_event: dict = event.event
    event_type = backend_event.get("type")
    if event_type == "response.output_item.done":
        item = backend_event.get("item", {})
        item_type = item.get("type")
        name = item.get("name")
        logger.debug(f"response.output_item.done: item_type={item_type}" + (f" name={name}" if name else ""))
        if item_type == "function_call":
            await handle_tool_call(item, connection, call_id)


async def handle_tool_call(item: dict, connection: AsyncLiveConnection, call_id: str):
    """
    Handle tool calls from the Responses delegation backend.
    Always sends a function_call_output result back so the AI can respond.
    :param item: The function_call item from the Responses backend
    :param connection: The OpenAI Live connection
    :param call_id: The Bandwidth call ID
    :return: None
    """
    function_name = item.get("name")
    tool_call_id = item.get("id")

    result: str
    match function_name:
        case "transfer_call":
            logger.info(f"Transferring call — account: {BW_ACCOUNT}, call_id: {call_id}, to: {TRANSFER_TO}")
            transfer_bxml = Bxml([Transfer([PhoneNumber(TRANSFER_TO)])])
            try:
                bandwidth_voice_api_instance.update_call_bxml(BW_ACCOUNT, call_id, transfer_bxml.to_bxml())
                logger.info("Transfer BXML sent successfully")
                result = "success"
            except Exception as e:
                logger.error(f"Error transferring call: {e}")
                result = f"error: {e}"
        case _:
            logger.warning(f"Unhandled function call: {function_name}")
            result = f"error: unknown function {function_name}"

    await connection.response.item.create(item={
        "type": "function_call_output",
        "call_id": tool_call_id,
        "output": result,
    })
    await connection.response.create()


@app.get("/health", status_code=http.HTTPStatus.NO_CONTENT)
def health():
    """
    Health check endpoint.
    :return: None
    """
    return


@app.post("/webhooks/bandwidth/voice/initiate", status_code=http.HTTPStatus.OK)
def handle_initiate_event(callback: InitiateCallback) -> Response:
    """
    Handle the initiate event from Bandwidth.
    Responds with BXML to start a bidirectional audio stream to our WebSocket server.

    :param callback: The initiate callback data
    :return: BXML response with StartStream verb
    """
    call_id = callback.call_id
    logger.info(f"Received initiate event for call ID: {call_id}")

    websocket_url = f"wss://{BASE_URL.replace('https://', '').replace('http://', '')}/ws"
    start_stream = StartStream(
        destination=f"{websocket_url}?call_id={call_id}",
        mode="bidirectional",
        name=call_id,
        destination_username="foo",
        destination_password="bar"
    )
    stop_stream = StopStream(name=call_id, wait="true")
    bxml_response = Bxml(nested_verbs=[start_stream, stop_stream])

    return Response(status_code=http.HTTPStatus.OK, content=bxml_response.to_bxml(), media_type="application/xml")


@app.websocket("/ws")
async def handle_inbound_websocket(bandwidth_websocket: WebSocket, call_id: str = None):
    """
    Handle inbound WebSocket connections from Bandwidth and bridge to OpenAI Live.
    :param bandwidth_websocket: The incoming WebSocket connection from Bandwidth
    :param call_id: The Bandwidth call ID passed as a query parameter
    :return: None
    """
    await bandwidth_websocket.accept()

    if not call_id:
        logger.error("No call_id provided in WebSocket connection")
        await bandwidth_websocket.close(code=1008, reason="Missing call_id parameter")
        return

    try:
        async with openai_client.live.connect(max_retries=0) as connection:
            logger.info("Connected to OpenAI Live")
            call_sessions[call_id] = connection
            try:
                await initialize_openai_session(connection)
                bw_task = asyncio.create_task(receive_from_bandwidth_ws(bandwidth_websocket, connection))
                oai_task = asyncio.create_task(receive_from_openai_ws(connection, bandwidth_websocket, call_id))
                done, pending = await asyncio.wait(
                    [bw_task, oai_task], return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                for task in done:
                    task.result()
            finally:
                call_sessions.pop(call_id, None)
    except RuntimeError as e:
        logger.error(f"OpenAI session initialization failed: {e}")
        try:
            await bandwidth_websocket.close(code=1011, reason="Session initialization failed")
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Failed to connect to OpenAI Live: {e}")
        try:
            await bandwidth_websocket.close(code=1011, reason="Upstream connection failed")
        except Exception:
            pass


@app.post("/webhooks/bandwidth/voice/hold", status_code=http.HTTPStatus.NO_CONTENT)
async def handle_hold_event(request: HoldRequest) -> None:
    """
    Place or remove a hold on an active call by muting/unmuting audio input to OpenAI.
    Send hold=true to mute (hold), hold=false to unmute (resume).

    :param request: HoldRequest containing call_id and hold flag
    :return: None
    """
    connection = call_sessions.get(request.call_id)
    if not connection:
        raise HTTPException(status_code=404, detail=f"No active session for call_id: {request.call_id}")

    if request.hold:
        await connection.session.input_audio.mute()
    else:
        await connection.session.input_audio.unmute()
    logger.info(f"{'Muted' if request.hold else 'Unmuted'} audio for call ID: {request.call_id}")


@app.post("/webhooks/bandwidth/voice/status", status_code=http.HTTPStatus.NO_CONTENT)
def handle_disconnect_event(callback: DisconnectCallback) -> None:
    """
    Handle call status events from Bandwidth.

    :param callback: The disconnect callback data
    :return: None
    """
    call_id = callback.call_id
    disconnect_cause = callback.cause
    error_message = callback.error_message
    if error_message:
        logger.error(f"Call {call_id} ended with error — cause: {disconnect_cause}, error: {error_message}")
    else:
        logger.info(f"Call {call_id} ended — cause: {disconnect_cause}")
    return


def start_server(port: int) -> None:
    """
    Start the FastAPI server.

    :param port: The port to run the server on
    :return: None
    """
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        reload=True,
    )


if __name__ == "__main__":
    start_server(LOCAL_PORT)
