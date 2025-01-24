import ffmpeg
import struct
import threading
import random
import array
import uuid
import concurrent.futures
import requests
import pyogg
import datetime
import os
import boto3
import dotenv
import logging

from sqlalchemy import create_engine, Column, String, TIMESTAMP, JSON, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

dotenv.load_dotenv(".env")
dotenv.load_dotenv(".env.local")

METAR_URL = os.environ["METAR_URL"]
SAMPLE_RATE = int(os.environ["SAMPLE_RATE"])
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_ENDPOINT_URL = os.environ["S3_ENDPOINT_URL"] 
PUBLIC_S3_ENDPOINT_URL = os.environ["PUBLIC_S3_ENDPOINT_URL"]
WHISPER_API_URL = os.environ["WHISPER_API_URL"]
POSTGRES_URL = os.environ["POSTGRES_URL"]

Base = declarative_base()
class Recording(Base):
    __tablename__ = 'recordings'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, unique=True, nullable=False, name="id")
    timestamp = Column(TIMESTAMP(timezone=False), nullable=False, name="timestamp")
    length = Column(Integer, nullable=False, name="length")
    storage_key = Column(String, nullable=False, name="storageKey")
    whisper_data = Column(JSON, nullable=True, name="whisperData")

engine = create_engine(POSTGRES_URL)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

session = boto3.session.Session()
s3_client = session.client(service_name="s3", aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY, endpoint_url=S3_ENDPOINT_URL)

executor = concurrent.futures.ThreadPoolExecutor(max_workers=16)

def upload_to_s3(path, data, content_type):
    s3_client.put_object(Body=data, Bucket=S3_BUCKET_NAME, Key=path, ContentType=content_type)
    logger.info(f"uploaded {path}")

def process_voice(timestamp, length, ogg_file, path):
    session = Session()
    try:
        upload_to_s3(path, ogg_file, "application/ogg")
        recording = Recording(
            timestamp=timestamp,
            length=length,
            storage_key=path,
            whisper_data=None
        )
        session.add(recording)
        session.commit()
        data = {
            "url": f"{PUBLIC_S3_ENDPOINT_URL}/{path}",
            "task": "transcribe",
            "language": "en"
        }
        logger.info("parsing")
        result = requests.post(WHISPER_API_URL, json=data)
        result_data = result.json()
        parsed_text = result_data["output"]["text"] 
        logger.info(f"parsed: '{parsed_text}'")
        recording.whisper_data = result_data
        session.commit()
        upload_to_s3(f"{path}.whisper.json", result.text.encode("utf-8"), "application/json")
    except Exception as e:
        logger.error(e)

def get_audio_chunk(process, chunk_size):
    return process.stdout.read(chunk_size)

def build_ogg_file(samples):
    filename = f"/tmp/{uuid.uuid4()}"
    opus_encoder = pyogg.OpusBufferedEncoder()
    opus_encoder.set_application("audio")
    opus_encoder.set_sampling_frequency(SAMPLE_RATE)
    opus_encoder.set_channels(1)
    opus_encoder.set_frame_size(20)
    ogg_opus_writer = pyogg.OggOpusWriter(
        filename,
        opus_encoder
    )
    ogg_opus_writer.write(memoryview(bytearray(array.array("h", samples))))
    ogg_opus_writer.close()
    with open(filename, "rb") as f:
        data = f.read()
    os.unlink(filename)
    return data

def process_data(process):
    chunk_10ms_size = SAMPLE_RATE // 100

    current_buffer = []

    logger.info('reading')

    start_timestamp = datetime.datetime.now()

    while True:
        chunk = get_audio_chunk(process, chunk_10ms_size * 2)
        for (sample,) in struct.iter_unpack("<h", chunk):
            current_buffer.append(sample * 10)
        
        last_150ms = current_buffer[-(chunk_10ms_size * 30):]
        max_level = max(abs(min(last_150ms)), max(last_150ms))
        if max_level < 10000:
            if len(current_buffer) >= chunk_10ms_size * 1500:
                logger.info("saving")
                samples = current_buffer[:-(chunk_10ms_size * 15)]
                datetime_string = datetime.datetime.now(datetime.timezone.utc).isoformat().split("+")[0].replace(":", "_").replace(".", "_")
                date_string = datetime_string.split("T")[0]
                length_seconds = len(samples) / SAMPLE_RATE
                random_id = str(uuid.uuid4())
                path = f"{date_string}/{datetime_string}-{length_seconds:.2f}-{random_id}.ogg"
                ogg_file = build_ogg_file(samples)
                executor.submit(process_voice, start_timestamp, round(length_seconds * 1000), ogg_file, path)
                start_timestamp = datetime.datetime.now()
                current_buffer = current_buffer[-(chunk_10ms_size * 15):]

process = (
    ffmpeg
    .input(METAR_URL)
    .output("pipe:", format="s16le", acodec="pcm_s16le", ac=1, ar=SAMPLE_RATE, loglevel="quiet")
    .run_async(pipe_stdout=True)
)

thread = threading.Thread(target=process_data, args=(process,), daemon=True)
thread.start()
thread.join()