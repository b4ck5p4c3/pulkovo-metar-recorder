FROM python:3.13
WORKDIR /app
RUN apt update && apt -y install libopus0 libopusfile0 libopusenc0 ffmpeg
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .
CMD python3 -u main.py