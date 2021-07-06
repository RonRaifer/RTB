from data_models import UserStats
from data_processor import DataProcessor
'''
    Run with pytest
'''
data_processor = DataProcessor(req="test_data/requests.csv",
                               clicks="test_data/clicks.csv",
                               imp="test_data/impressions.csv")


def test_data():
    users_pivot_table = data_processor.get_users_pivot()
    user_stats = users_pivot_table.loc['efb64b4e-3655-4a4a-af2d-4d62945eb6d0'].to_dict()
    user_stats = UserStats(**user_stats)
    assert user_stats.num_of_requests == 2
    assert user_stats.num_of_impressions == 2
    assert user_stats.num_of_clicks == 1
    assert user_stats.avg_bid_price == 2.1
    assert user_stats.median_impression_duration == 21.0
    assert user_stats.max_time_till_click == 63.0
