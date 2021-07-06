import uvicorn
from simple_background_task import BackgroundTask
from simple_background_task import defer
from fastapi import FastAPI, Depends, HTTPException
from starlette import status
from data_processor import DataProcessor
from data_models import UserStats, SessionStats

data_processor = None
users_pivot_table = None
session_pivot_table = None

app = FastAPI()
BackgroundTask().start()


def load_pivot_tables(req="data/requests.csv", clicks="data/clicks.csv", imp="data/impressions.csv"):
    global data_processor, users_pivot_table, session_pivot_table
    if data_processor is None:
        data_processor = DataProcessor(req, clicks, imp)
    if users_pivot_table is None:
        users_pivot_table = data_processor.get_users_pivot()
    if session_pivot_table is None:
        session_pivot_table = data_processor.get_session_pivot()


defer(
    func=load_pivot_tables,
    arguments={
        "args": ["data/requests.csv", "data/clicks.csv", "data/impressions.csv"],
        "kwargs": {}
    }
)


@app.get("/")
async def read_root():
    return {"Hello": "World :D"}


@app.get("/userStats/", response_model=UserStats)
def read_user_stats(user_id: str, is_ready: bool = Depends(lambda: users_pivot_table is not None)):
    if is_ready:
        try:
            user_stats = users_pivot_table.loc[user_id].to_dict()
        except KeyError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"The user id specified does not exist, please enter an existing user id."
            )
        return UserStats(**user_stats)
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Data is not ready yet, please use keepalive to check when ready"
        )


@app.get("/sessionId/", response_model=SessionStats)
def read_session_stats(session_id: str, is_ready: bool = Depends(lambda: session_pivot_table is not None)):
    if is_ready:
        try:
            session_stats = session_pivot_table.loc[session_id].to_dict()
        except KeyError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"The session id specified does not exist, please enter an existing session id."
            )
        return SessionStats(**session_stats)
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Data is not ready yet, please use keepalive to check when ready"
        )


@app.get("/keepalive")
def check_service_status():
    return {"Alive": users_pivot_table is not None and session_pivot_table is not None}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
