FROM python:3.9-alpine3.16

ENV PYTHONDONTWRITEBYTECODE=1 
ENV PYTHONUNBUFFERED=1 
WORKDIR /app
COPY requirements.txt /app/
RUN apk add postgresql-client build-base postgresql-dev
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY . . 

CMD ["python3" , "index.py"]