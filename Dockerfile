FROM python:3

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

COPY . /app/

EXPOSE 5000

CMD [ "python", "./app.py" ]