"""
Componentes reutilizáveis do dashboard.
"""

from dash import html
import dash_bootstrap_components as dbc

from src.dashboard.styles import (
    KPI_CARD_STYLE, KPI_VALUE_STYLE, KPI_LABEL_STYLE, KPI_SUBTITLE_STYLE,
    INSIGHT_BOX_STYLE, INSIGHT_TITLE_STYLE, INSIGHT_ITEM_STYLE,
)


def kpi_card(label: str, value: str, subtitle: str = "", color: str = None) -> dbc.Col:
    """Cria um card de KPI."""
    style = KPI_CARD_STYLE.copy()
    value_style = KPI_VALUE_STYLE.copy()
    if color:
        value_style["color"] = color

    return dbc.Col(
        html.Div(
            [
                html.P(label, style=KPI_LABEL_STYLE),
                html.H2(value, style=value_style),
                html.P(subtitle, style=KPI_SUBTITLE_STYLE) if subtitle else None,
            ],
            style=style,
        ),
        md=3,
        sm=6,
        className="mb-3",
    )


def insight_box(title: str, bullets: list[str]) -> html.Div:
    """Cria box de insights com bullets."""
    return html.Div(
        [
            html.H4(title, style=INSIGHT_TITLE_STYLE),
            html.Ul(
                [html.Li(b, style=INSIGHT_ITEM_STYLE) for b in bullets],
                style={"listStyleType": "none", "paddingLeft": "0"},
            ),
        ],
        style=INSIGHT_BOX_STYLE,
    )


def section_title(text: str) -> html.H5:
    """Título de seção dos gráficos."""
    return html.H5(
        text,
        style={
            "fontSize": "1rem",
            "fontWeight": "600",
            "color": "#2C3E50",
            "marginBottom": "4px",
        },
    )
