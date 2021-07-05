from fastapi import FastAPI, Depends
import uvicorn
from data_loader import get_users_pivot, get_session_pivot
from data_models import UserStats, SessionStats

app = FastAPI()
users_pivot_table = None
session_pivot_table = None


def load_pivot_tables():
    global users_pivot_table, session_pivot_table
    if users_pivot_table is None:
        users_pivot_table = get_users_pivot()
    if session_pivot_table is None:
        session_pivot_table = get_session_pivot()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/userStats/", response_model=UserStats, dependencies=[Depends(load_pivot_tables)])
def read_user_stats(user_id: str):
    user_stats = users_pivot_table.loc[user_id].to_dict()
    return UserStats(**user_stats)


@app.get("/sessionId/", response_model=SessionStats, dependencies=[Depends(load_pivot_tables)])
def read_session_stats(session_id: str):
    session_stats = session_pivot_table.loc[session_id].to_dict()
    return SessionStats(**session_stats)


@app.get("/keepalive")
def check_service_status():
    return {"Alive": users_pivot_table is not None and session_pivot_table is not None}


if __name__ == '__main__':
    # users_pivot_table = get_users_pivot()
    uvicorn.run(app, host='0.0.0.0', port=8000)
