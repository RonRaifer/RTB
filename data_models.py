from pydantic import BaseModel


class UserStats(BaseModel):
    num_of_requests: int
    num_of_impressions: int
    num_of_clicks: int
    avg_bid_price: float
    median_impression_duration: float
    max_time_till_click: float


class SessionStats(BaseModel):
    begin: int
    finish: int
    partner_name: str
