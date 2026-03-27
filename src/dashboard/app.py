"""
Entry point da aplicação Dash.
"""

import dash
import dash_bootstrap_components as dbc

from src.dashboard.layout import build_layout
from src.dashboard.explorer import register_callbacks

app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.FLATLY,
        "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap",
    ],
    title="ANVISA VigiMed — Dashboard de Farmacovigilancia",
    suppress_callback_exceptions=True,
)

server = app.server  # Para deploy com gunicorn

app.layout = build_layout()

# Registrar callbacks do Dashboard 2
register_callbacks(app)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
