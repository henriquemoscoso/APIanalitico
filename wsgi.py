# wsgi.py
try:
    from a2wsgi import ASGIMiddleware
except Exception as e:
    raise RuntimeError(
        "Falha ao importar a2wsgi. Instale com: python -m pip install a2wsgi"
    ) from e

from main import app  # seu FastAPI app

# callable WSGI que o IIS/wfastcgi vai carregar
application = ASGIMiddleware(app)