import uvicorn

from api_service.app import create_app

app = create_app()


def run() -> None:
    uvicorn.run("api_service.main:app", host="0.0.0.0", port=8080, log_config=None)


if __name__ == "__main__":
    run()
