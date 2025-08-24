FROM python:3.12-slim
WORKDIR /app
RUN pip install fastapi uvicorn[standard] psycopg[binary]
RUN pip install prometheus-fastapi-instrumentator
COPY app/ /app
ENV DB_HOST=postgres DB_PORT=5432 DB_USER=amuser DB_PASSWORD=ampass DB_NAME=amdb
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]