"""
Paleta de cores e estilos do dashboard.
Baseado em princípios de Storytelling with Data (Cole Nussbaumer Knaflic).
"""

# Paleta de cores healthcare
COLORS = {
    "primary": "#1B4F72",
    "secondary": "#2E86C1",
    "accent": "#17A589",
    "danger": "#E74C3C",
    "warning": "#F39C12",
    "success": "#27AE60",
    "neutral": "#95A5A6",
    "bg": "#F4F6F9",
    "card": "#FFFFFF",
    "text": "#2C3E50",
    "text_secondary": "#7F8C8D",
    "border": "#E5E8EC",
}

# Paleta sequencial para gráficos
CHART_COLORS = [
    "#2E86C1", "#1B4F72", "#17A589", "#F39C12", "#E74C3C",
    "#8E44AD", "#27AE60", "#D35400", "#2C3E50", "#16A085",
]

# Template Plotly padrão (sem margin — sempre passado explicitamente)
_BASE_LAYOUT = {
    "font": {"family": "Inter, Segoe UI, sans-serif", "color": COLORS["text"]},
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "xaxis": {"showgrid": False, "showline": True, "linecolor": COLORS["border"]},
    "yaxis": {"showgrid": True, "gridcolor": "#F0F0F0", "showline": False},
    "colorway": CHART_COLORS,
    "hoverlabel": {"bgcolor": "white", "font_size": 12},
}

# Manter para retrocompat, mas sem margin
PLOTLY_TEMPLATE = {"layout": _BASE_LAYOUT}

DEFAULT_MARGIN = {"l": 40, "r": 20, "t": 50, "b": 40}

# Estilo dos KPI cards
KPI_CARD_STYLE = {
    "backgroundColor": COLORS["card"],
    "borderRadius": "12px",
    "padding": "24px",
    "boxShadow": "0 2px 8px rgba(0,0,0,0.06)",
    "textAlign": "center",
    "border": f"1px solid {COLORS['border']}",
}

KPI_VALUE_STYLE = {
    "fontSize": "2.2rem",
    "fontWeight": "700",
    "color": COLORS["primary"],
    "margin": "8px 0",
    "lineHeight": "1",
}

KPI_LABEL_STYLE = {
    "fontSize": "0.85rem",
    "color": COLORS["text_secondary"],
    "textTransform": "uppercase",
    "letterSpacing": "0.5px",
    "fontWeight": "500",
}

KPI_SUBTITLE_STYLE = {
    "fontSize": "0.8rem",
    "color": COLORS["neutral"],
    "marginTop": "4px",
}

# Estilo dos gráficos
CHART_CARD_STYLE = {
    "backgroundColor": COLORS["card"],
    "borderRadius": "12px",
    "padding": "16px",
    "boxShadow": "0 2px 8px rgba(0,0,0,0.06)",
    "border": f"1px solid {COLORS['border']}",
    "marginBottom": "16px",
}

# Header
HEADER_STYLE = {
    "backgroundColor": COLORS["primary"],
    "color": "white",
    "padding": "20px 32px",
    "marginBottom": "24px",
}

HEADER_TITLE_STYLE = {
    "fontSize": "1.5rem",
    "fontWeight": "700",
    "margin": "0",
    "color": "white",
}

HEADER_SUBTITLE_STYLE = {
    "fontSize": "0.9rem",
    "color": "rgba(255,255,255,0.7)",
    "margin": "4px 0 0 0",
}

# Insight bullets
INSIGHT_BOX_STYLE = {
    "backgroundColor": "#EBF5FB",
    "borderRadius": "12px",
    "padding": "20px 24px",
    "borderLeft": f"4px solid {COLORS['secondary']}",
    "marginTop": "16px",
}

INSIGHT_TITLE_STYLE = {
    "fontSize": "1rem",
    "fontWeight": "600",
    "color": COLORS["primary"],
    "marginBottom": "12px",
}

INSIGHT_ITEM_STYLE = {
    "fontSize": "0.9rem",
    "color": COLORS["text"],
    "marginBottom": "6px",
    "lineHeight": "1.5",
}

# Filtros
FILTER_LABEL_STYLE = {
    "fontSize": "0.8rem",
    "fontWeight": "600",
    "color": COLORS["text_secondary"],
    "textTransform": "uppercase",
    "letterSpacing": "0.5px",
    "marginBottom": "4px",
}
