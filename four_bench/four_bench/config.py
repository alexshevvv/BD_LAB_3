from dataclasses import dataclass


@dataclass
class Config:
    columns = [
        "ID INTEGER PRIMARY KEY",
        "VendorID INTEGER",
        "tpep_pickup_datetime DATETIME",
        "tpep_dropoff_datetime DATETIME",
        "passenger_count FLOAT",
        "trip_distance FLOAT",
        "RatecodeID FLOAT",
        "store_and_fwd_flag TEXT",
        "PULocationID INTEGER",
        "DOLocationID INTEGER",
        "payment_type INTEGER",
        "fare_amount FLOAT8",
        "extra FLOAT8",
        "mta_tax FLOAT8",
        "tip_amount FLOAT8",
        "tolls_amount FLOAT8",
        "improvement_surcharge FLOAT8",
        "total_amount FLOAT8",
        "congestion_surcharge FLOAT8",
        "airport_fee FLOAT8",
        "another_airport_fee FLOAT8",
    ]

    date_time_format = r"%Y-%m-%d %H:%M:%S"
