from pydantic import BaseModel


class UserStats(BaseModel):
    num_of_requests: int
    num_of_impressions: int
    num_of_clicks: int
    avg_bid_price: float
    max_time_till_click: float


class SessionStats(BaseModel):
    begin: float
    finish: float
    partner_name: str
