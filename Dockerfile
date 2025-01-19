FROM python:3.13
WORKDIR /app
COPY . .
RUN apt update && apt -y install libopus0 libopusfile0 libopusenc0 ffmpeg
RUN pip3 install -r requirements.txt
CMD python3 main.py