# RTB Task, Ron Raifer.
## _Given By [Start.io](https://start.io/)_
This assignment was given to me by Start.io, to test my capabilities.
I asked to implement a service that loads the given csv files and serves REST API requests.
During the implementation, I reviewed the given data and looked for the most relevant tools to accomplish the best results. It was a great opportunity to learn new technologies!.  



## Tech

RTB uses a number of open source projects to work properly:

- [Python](https://www.python.org/) - the programming language
- [FastAPI](https://fastapi.tiangolo.com/) - fast (high-performance), web framework for building APIs with Python 3.6+
- [DASK](https://dask.org/) - provides advanced parallelism for analytics
- [pydantic](https://pydantic-docs.helpmanual.io/) - enforces type hints at runtime, and provides user friendly errors when data is invalid
- [uvicorn](https://www.uvicorn.org/) - a lightning-fast ASGI server implementation, using uvloop and httptools
- [pytest](pytest.org) - framework makes it easy to write small tests


## Installation

This project requires [Python](https://www.python.org/) v3.6+ to run.

Clone the project or download it directly:
```bash
git clone https://github.com/ronraifer/RTB.git
cd RTB
```

Install the dependencies, using [pip](https://pypi.org/project/pip/), via running the next command:
```bash
 pip install -r requirements.txt
```

## Running the App

RTB Application works in two ways: running `main.py`, or via terminal using `uvicorn`.
This section will guide you and explain how to run the app in each mode.

#### !Run\Debug `main.py`

- Make sure you have [Insomnia](https://insomnia.rest/) or equivalent.
- Next, through your IDE or CMD, run ``main.py``
- The API is now reachable through ``http://127.0.0.1:8000``
- Open Insomnia, and add GET requests as your wish.
- Now tick the 'Send' button, and review the response.
- Example "degifted" below

![how_to_debug](guide_files/how_to_debug.gif)

#### !Run Via Terminal

Via terminal (IDE/cmd), enter the following:
```bash 
uvicorn main:app --reload 
```

Next, open your browser (or Insomnia), and navigate to:
```bash 
http://127.0.0.1:8000
```

Then, navigate to the desired request, with the relevant arguments:
```bash 
http://127.0.0.1:8000/keepalive
http://127.0.0.1:8000/userStats/?user_id=YOUR_USER_ID
http://127.0.0.1:8000/sessionId/?session_id=YOUR_SESSION_ID
```

## Swagger

To access the interactive API documentation, make sure the app is running, and then navigate to:
```bash 
http://127.0.0.1:8000/docs
```
![how_to_docs](guide_files/how_to_docs.gif)

## Further Improvements

#### Tests
- Add more test cases to cover all the possibilities. 
- Add tests to the session data.

#### Tech
[Redis](https://redis.io/) might a good idea to use. 

It is possible to keep the Pivots data in Redis. By that, the data is loaded into the Cache, and we can use it as a key value storage. 
This allows us to maintain the data in Real-Time, instead of loading the whole data each time (locally).

## Contact Info
Mail me: ronraifer@gmail.com


