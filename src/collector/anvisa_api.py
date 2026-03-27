"""
Cliente OAuth2 para a API da ANVISA.
Usa Keycloak (OpenID Connect) com client credentials flow.
Baseado na implementação do MedReport.
"""

import os
import time
import logging
import json

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Endpoints reais da ANVISA (Keycloak PRD)
TOKEN_URL = "https://acesso.prd.apps.anvisa.gov.br/auth/realms/externo/protocol/openid-connect/token"
GATEWAY_URL = "https://api-gateway.prd.apps.anvisa.gov.br/consultas-externas-api/api/v1"


class AnvisaClient:
    def __init__(self):
        self.client_id = os.getenv("ANVISA_CLIENT_ID")
        self.client_secret = os.getenv("ANVISA_CLIENT_SECRET")
        self._token = None
        self._expires_at = 0

        if not self.client_id or not self.client_secret:
            raise ValueError(
                "ANVISA_CLIENT_ID e ANVISA_CLIENT_SECRET devem estar definidos no .env"
            )

    def _get_token(self):
        """Obtém ou renova o token OAuth2 via Keycloak client credentials."""
        if self._token and time.time() < self._expires_at - 60:
            return self._token

        logger.info("Obtendo token OAuth2 da ANVISA (Keycloak)...")
        resp = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._expires_at = time.time() + data.get("expires_in", 3600)
        logger.info("Token obtido com sucesso (expira em %ds)", data.get("expires_in", 3600))
        return self._token

    def _clear_token(self):
        """Limpa token em caso de 401."""
        self._token = None
        self._expires_at = 0

    def get(self, endpoint: str, params: dict | None = None) -> requests.Response:
        """GET autenticado no API Gateway da ANVISA."""
        token = self._get_token()
        url = f"{GATEWAY_URL}{endpoint}" if endpoint.startswith("/") else endpoint
        resp = requests.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        # Se 401, tentar renovar token uma vez
        if resp.status_code == 401:
            self._clear_token()
            token = self._get_token()
            resp = requests.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {token}"},
                timeout=15,
            )
        resp.raise_for_status()
        return resp

    def post(self, endpoint: str, json_data: dict | None = None) -> requests.Response:
        """POST autenticado no API Gateway da ANVISA."""
        token = self._get_token()
        url = f"{GATEWAY_URL}{endpoint}" if endpoint.startswith("/") else endpoint
        resp = requests.post(
            url,
            json=json_data,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=15,
        )
        if resp.status_code == 401:
            self._clear_token()
            token = self._get_token()
            resp = requests.post(
                url,
                json=json_data,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                timeout=15,
            )
        resp.raise_for_status()
        return resp

    def query_produto_saude(self, numero_registro: str) -> dict | None:
        """Consulta um produto de saúde pelo número de registro."""
        try:
            resp = self.post(
                "/consulta/saude",
                json_data={
                    "filter": {"numeroRegistro": numero_registro},
                    "page": 1,
                    "pageSize": 10,
                },
            )
            data = resp.json()
            # Formato paginado: {"content": [...], "pageable": {...}}
            items = data.get("content", []) if isinstance(data, dict) else data
            if items:
                return {"registro": numero_registro, "items": items, "raw": data}
            return None
        except Exception as e:
            logger.debug("Consulta produto %s falhou: %s", numero_registro, e)
            return None

    def query_medicamento(self, numero_registro: str) -> dict | None:
        """Alias para query_produto_saude."""
        return self.query_produto_saude(numero_registro)

    def test_connection(self) -> bool:
        """Testa se a conexão com a API está funcionando."""
        try:
            self._get_token()
            logger.info("Conexao com API ANVISA OK")
            return True
        except Exception as e:
            logger.warning("Falha na conexao com API ANVISA: %s", e)
            return False
