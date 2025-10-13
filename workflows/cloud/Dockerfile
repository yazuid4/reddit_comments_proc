FROM python:3.11-slim

WORKDIR /app

COPY decompressor_stream.py .

RUN pip install --no-cache-dir zstandard google-cloud-storage gcsfs Flask

EXPOSE 8080

ENV PORT=8080

CMD ["python", "decompressor_stream.py"]