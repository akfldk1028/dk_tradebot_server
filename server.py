from fastapi import FastAPI

app = FastAPI(
    title="FastAPI Demo",
    description="This is a very fancy project, with auto docs for the API and everything",
)


@app.get("/")
def aa():
    return "보낼 값"
