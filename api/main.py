from fastapi import FastAPI
from pydantic import BaseModel
from sql_agent import ask

app = FastAPI(title="Flight Delay Data Agent")

class QuestionRequest(BaseModel):
    question: str

@app.post("/ask")
def ask_endpoint(request: QuestionRequest):
    result = ask(request.question)
    return result

@app.get("/health")
def health():
    return {"status": "ok"}