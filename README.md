# To run:

python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000
