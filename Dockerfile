# Use a Python 3.11 base image
FROM python:3.11-alpine
WORKDIR /app
COPY . /app
RUN pip install -r requirement.txt
RUN pip install .
CMD ["python", "src/trading_algo/trade_algo_structure.py"]