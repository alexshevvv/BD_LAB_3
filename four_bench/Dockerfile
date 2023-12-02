FROM python:3.11

RUN pip install poetry==1.7.1

WORKDIR /app

COPY ./pyproject.toml ./four_bench ./main.py ./

RUN poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi

CMD ["python", "main.py"]