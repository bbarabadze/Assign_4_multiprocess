from datetime import datetime
from pydantic import BaseModel


class DriveData(BaseModel):
    vendorid: int
    tpep_pickup_datetime: datetime
    tpep_dropoff_datetime: datetime
    passenger_count: int
    trip_distance: float
    pickup_longitude: float
    pickup_latitude: float
    ratecodeid: int
    store_and_fwd_flag: str
    dropoff_longitude: float
    dropoff_latitude: float
    payment_type: int
    fare_amount: float
    extra: float
    mta_tax: float
    tip_amount: float
    tolls_amount: float
    improvement_surcharge: float
    total_amount: float
