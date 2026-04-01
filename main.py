"""
Traceability Agent — Operator Lookup + Judy Slack Bot
Verifica cadastro ativo de transportadores e receptores nos portais regulatórios MTR.
Bot @judy responde no Slack com resultados por órgão emissor.
"""

import json
import os
import re
import asyncio
import time
from io import BytesIO
from typing import Literal

import openpyxl

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from pydantic import BaseModel, field_validator
from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.fastapi.async_handler import AsyncSlackRequestHandler

load_dotenv()

# ---------------------------------------------------------------------------
# Slack App (async — compatível com FastAPI/uvicorn)
# ---------------------------------------------------------------------------

slack_app = AsyncApp(
    token=os.environ["SLACK_BOT_TOKEN"],
    signing_secret=os.environ["SLACK_SIGNING_SECRET"],
)
slack_handler = AsyncSlackRequestHandler(slack_app)

# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Traceability Agent — Operator Lookup",
    version="1.3.0",
    description="Lookup de cadastro ativo de operadores nos portais MTR + Judy Slack Bot.",
)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

ENTITY_TYPE_CODE = {
    "generator": "J",
    "transporter": "T",
    "receiver": "D",
}

ISSUER_LABELS = {
    "brasilapi_cnpj": "Cartão CNPJ (Receita Federal)",
    "rntrc": "RNTRC (ANTT)",
    "ctf_ibama": "CTF/APP IBAMA — Certificado de Regularidade",
    "cetesb_lo": "CETESB — Licença de Operação / Dispensa (e-CETESB)",
    "spregula": "SP Regula — Operadores Municipais (Prefeitura SP)",
    "sigor_sp": "(SIGOR) CETESB — São Paulo",
    "sinir": "(SINIR) IBAMA — Nacional",
    "inea_rj": "Instituto Estadual do Ambiente — Rio de Janeiro",
    "semad_go": "Secretaria de Meio Ambiente e Desenvolvimento Sustentável — Goiás",
    "semad_mg": "Secretaria de Meio Ambiente e Desenvolvimento Sustentável — Minas Gerais",
    "fepam_rs": "Fundação Estadual de Proteção Ambiental — Rio Grande do Sul",
    "iema_es": "Instituto Estadual de Meio Ambiente e Recursos Hídricos — Espírito Santo",
    "ima_al": "Instituto de Meio Ambiente — Alagoas",
}

SUPPORTED_ISSUERS = {
    "brasilapi_cnpj", "rntrc", "ctf_ibama", "cetesb_lo", "spregula",
    "sigor_sp", "sinir", "inea_rj", "semad_go", "semad_mg", "fepam_rs", "iema_es",
}
NOT_SUPPORTED_ISSUERS = {
    "ima_al": "Instituto de Meio Ambiente de Alagoas requer CPF para lookup de unidades — consulta só por CNPJ não suportada.",
}

SIGOR_ORIGEM_LABELS = {
    0: "CETESB",
    2: "Municipal",
    3: "IBAMA",
    4: "Isento",
    5: "CETESB",
}

HTTPX_TIMEOUT = 20.0

CNPJ_REGEX = re.compile(r"\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2}")

# ---------------------------------------------------------------------------
# RNTRC — resource_id cache (CKAN dataset)
# ---------------------------------------------------------------------------

_rntrc_resource_id: str | None = None

RNTRC_PACKAGE_URL = "https://dados.antt.gov.br/api/3/action/package_show?id=rntrc"
RNTRC_DATASTORE_URL = "https://dados.antt.gov.br/api/3/action/datastore_search"


async def _get_rntrc_resource_id(client: httpx.AsyncClient) -> str | None:
    """Obtém o resource_id do arquivo CSV mais recente do dataset RNTRC via CKAN API."""
    global _rntrc_resource_id
    if _rntrc_resource_id:
        return _rntrc_resource_id
    try:
        resp = await client.get(RNTRC_PACKAGE_URL, timeout=15.0)
        data = resp.json()
        resources = data.get("result", {}).get("resources", [])
        csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]
        if not csv_resources:
            return None
        latest = sorted(csv_resources, key=lambda r: r.get("last_modified", ""), reverse=True)[0]
        _rntrc_resource_id = latest["id"]
        return _rntrc_resource_id
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Helpers — formatação de documentos
# ---------------------------------------------------------------------------

def strip_doc(value: str) -> str:
    return re.sub(r"\D", "", value)


def fmt_cnpj(value: str) -> str:
    d = strip_doc(value).zfill(14)
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

EntityType = Literal["transporter", "receiver"]
VALID_ISSUERS = list(ISSUER_LABELS.keys())


class LookupRequest(BaseModel):
    entity_type: EntityType
    document: str
    issuer: str

    @field_validator("document")
    @classmethod
    def validate_document(cls, v: str) -> str:
        digits = strip_doc(v)
        if len(digits) != 14:
            raise ValueError(f"CNPJ inválido — esperado 14 dígitos, recebido {len(digits)}.")
        return digits

    @field_validator("issuer")
    @classmethod
    def validate_issuer(cls, v: str) -> str:
        if v not in ISSUER_LABELS:
            raise ValueError(f"issuer '{v}' inválido. Valores aceitos: {VALID_ISSUERS}")
        return v


class MultiLookupRequest(BaseModel):
    entity_type: EntityType
    document: str
    issuers: list[str]

    @field_validator("document")
    @classmethod
    def validate_document(cls, v: str) -> str:
        digits = strip_doc(v)
        if len(digits) != 14:
            raise ValueError(f"CNPJ inválido — esperado 14 dígitos, recebido {len(digits)}.")
        return digits

    @field_validator("issuers")
    @classmethod
    def validate_issuers(cls, v: list[str]) -> list[str]:
        invalid = [i for i in v if i not in ISSUER_LABELS]
        if invalid:
            raise ValueError(f"issuers inválidos: {invalid}. Valores aceitos: {VALID_ISSUERS}")
        if not v:
            raise ValueError("issuers não pode ser vazio.")
        return v


# ---------------------------------------------------------------------------
# Lookup — Cartão CNPJ via BrasilAPI (Receita Federal)
# ---------------------------------------------------------------------------
# Endpoint público: GET https://brasilapi.com.br/api/cnpj/v1/{cnpj}
# Sem autenticação, sem captcha.
# ---------------------------------------------------------------------------

async def lookup_brasilapi_cnpj(document: str, entity_type: str) -> dict:
    url = f"https://brasilapi.com.br/api/cnpj/v1/{document}"

    async with httpx.AsyncClient(timeout=HTTPX_TIMEOUT) as client:
        try:
            resp = await client.get(url, headers={"User-Agent": "judy-bot/1.2"})
        except httpx.TimeoutException:
            return _error("TIMEOUT", "BrasilAPI não respondeu em 20 segundos.")
        except httpx.RequestError as exc:
            return _error("CONNECTION_ERROR", f"Erro de conexão com BrasilAPI: {exc}")

        if resp.status_code == 404:
            return _not_found("CNPJ não encontrado na Receita Federal.")
        if resp.status_code != 200:
            return _error(f"HTTP_{resp.status_code}", f"BrasilAPI retornou HTTP {resp.status_code}.")

        try:
            data = resp.json()
        except Exception:
            return _error("PARSE_ERROR", "Resposta da BrasilAPI não é JSON válido.")

        situacao = data.get("descricao_situacao_cadastral") or data.get("situacao_cadastral") or ""
        is_active = situacao.strip().upper() == "ATIVA"

        cnae_cod = data.get("cnae_fiscal", "")
        cnae_desc = data.get("cnae_fiscal_descricao", "")
        cnae = f"{cnae_cod} — {cnae_desc}".strip(" —") if cnae_cod or cnae_desc else ""

        unit = {
            "unit_id": document,
            "name": data.get("razao_social", ""),
            "fantasy_name": data.get("nome_fantasia") or "",
            "status": situacao,
            "cnae": cnae,
            "porte": data.get("porte") or "",
            "municipio": data.get("municipio") or "",
            "uf": data.get("uf") or "",
            "capital_social": data.get("capital_social"),
        }

        if is_active:
            return _found([unit])
        else:
            return {
                "registered": False,
                "units": [unit],
                "code": "INACTIVE",
                "message": f"Situação cadastral: {situacao or 'desconhecida'}",
            }


# ---------------------------------------------------------------------------
# Lookup — RNTRC via CKAN Datastore (dados.antt.gov.br)
# ---------------------------------------------------------------------------
# Query em tempo real pelo CNPJ no dataset aberto da ANTT.
# Atualizado mensalmente pela ANTT.
# ---------------------------------------------------------------------------

async def lookup_rntrc(document: str, entity_type: str) -> dict:
    cnpj_fmt = fmt_cnpj(document)

    async with httpx.AsyncClient(verify=False, timeout=HTTPX_TIMEOUT) as client:
        resource_id = await _get_rntrc_resource_id(client)
        if not resource_id:
            return _error("CONFIG_ERROR", "Não foi possível localizar o dataset RNTRC no portal da ANTT.")

        try:
            resp = await client.get(
                RNTRC_DATASTORE_URL,
                params={
                    "resource_id": resource_id,
                    "filters": json.dumps({"cpfcnpjtransportador": cnpj_fmt}),
                    "limit": 20,
                },
            )
        except httpx.TimeoutException:
            return _error("TIMEOUT", "ANTT dados abertos não respondeu em 20 segundos.")
        except httpx.RequestError as exc:
            return _error("CONNECTION_ERROR", f"Erro de conexão com ANTT dados abertos: {exc}")

        if resp.status_code != 200:
            return _error(f"HTTP_{resp.status_code}", f"ANTT dados abertos retornou HTTP {resp.status_code}.")

        try:
            data = resp.json()
        except Exception:
            return _error("PARSE_ERROR", "Resposta do ANTT dados abertos não é JSON válido.")

        if not data.get("success"):
            return _error("API_ERROR", "CKAN datastore retornou erro — dataset pode não estar indexado.")

        records = data.get("result", {}).get("records") or []
        if not records:
            return _not_found("CNPJ não encontrado no RNTRC.")

        units = []
        has_active = False
        for r in records:
            situacao = (r.get("situacao_rntrc") or "").strip().upper()
            if situacao == "ATIVO":
                has_active = True
            units.append({
                "unit_id": r.get("numero_rntrc", ""),
                "name": r.get("nome_transportador", ""),
                "status": situacao,
                "category": r.get("categoria_transportador", ""),
                "municipio": r.get("municipio", ""),
                "uf": r.get("uf", ""),
                "data_cadastro": r.get("data_primeiro_cadastro", ""),
                "data_situacao": r.get("data_situacao_rntrc", ""),
            })

        if has_active:
            return _found(units)
        else:
            first_status = units[0].get("status", "INATIVO") if units else "INATIVO"
            return {
                "registered": False,
                "units": units,
                "code": "INACTIVE",
                "message": f"CNPJ encontrado no RNTRC mas com situação: {first_status}",
            }


# ---------------------------------------------------------------------------
# Lookup — SIGOR-SP (CETESB)
# ---------------------------------------------------------------------------
# Endpoint público: GET https://mtrr.cetesb.sp.gov.br/api/cadastro/pesquisaCadastroUnico/{cnpj}
# Não requer autenticação.
# Nota: não filtra por entity_type — retorna todos os empreendimentos do CNPJ.
# ---------------------------------------------------------------------------

async def lookup_sigor_sp(document: str, entity_type: str) -> dict:
    url = f"https://mtrr.cetesb.sp.gov.br/api/cadastro/pesquisaCadastroUnico/{document}"

    async with httpx.AsyncClient(verify=False, timeout=HTTPX_TIMEOUT) as client:
        try:
            resp = await client.get(url)
        except httpx.TimeoutException:
            return _error("TIMEOUT", "SIGOR-SP não respondeu em 20 segundos.")
        except httpx.RequestError as exc:
            return _error("CONNECTION_ERROR", f"Erro de conexão com SIGOR-SP: {exc}")

        if resp.status_code != 200:
            return _error(f"HTTP_{resp.status_code}", f"SIGOR-SP retornou HTTP {resp.status_code}.")

        try:
            data = resp.json()
        except Exception:
            return _error("PARSE_ERROR", "Resposta do SIGOR-SP não é JSON válido.")

        if data.get("erro"):
            return _error("API_ERROR", data.get("mensagem") or "Erro retornado pelo SIGOR-SP.")

        empreendimentos = data.get("objetoResposta") or []
        if not empreendimentos:
            return _not_found("CNPJ não encontrado no SIGOR-SP.")

        units = []
        for emp in empreendimentos:
            endereco = emp.get("enderecoDTO") or {}
            municipio = endereco.get("desMunicpEndrEmprn") or ""
            bairro = endereco.get("desBairroEndrEmprn") or ""
            origem_cod = emp.get("origemCadastro")
            origem_label = SIGOR_ORIGEM_LABELS.get(origem_cod, str(origem_cod))
            units.append({
                "unit_id": str(emp.get("parCodigo", "")),
                "name": emp.get("desEmprn", ""),
                "origin": origem_label,
                "address": f"{bairro} / {municipio}".strip(" /") or None,
                "status": "active",
            })

        return _found(units)


# ---------------------------------------------------------------------------
# Lookup — SINIR (IBAMA Federal)
# ---------------------------------------------------------------------------
# Endpoint público: GET /api/mtr/consultaParceiro/{tipo}/{cnpj}
# tipo: J=gerador, T=transportador, D=destinador/receptor
# ---------------------------------------------------------------------------

async def lookup_sinir(document: str, entity_type: str) -> dict:
    tipo = ENTITY_TYPE_CODE[entity_type]
    url = f"https://mtr.sinir.gov.br/api/mtr/consultaParceiro/{tipo}/{document}"

    async with httpx.AsyncClient(verify=False, timeout=HTTPX_TIMEOUT) as client:
        try:
            resp = await client.get(url)
        except httpx.TimeoutException:
            return _error("TIMEOUT", "SINIR não respondeu em 20 segundos.")
        except httpx.RequestError as exc:
            return _error("CONNECTION_ERROR", f"Erro de conexão com SINIR: {exc}")

        if resp.status_code == 404:
            return _not_found("CNPJ não cadastrado no SINIR para o tipo informado.")

        if resp.status_code != 200:
            return _error(f"HTTP_{resp.status_code}", f"SINIR retornou HTTP {resp.status_code}.")

        try:
            data = resp.json()
        except Exception:
            return _error("PARSE_ERROR", "Resposta do SINIR não é JSON válido.")

        parceiros = data.get("objetoResposta") or []
        if not parceiros:
            return _not_found("CNPJ não encontrado no SINIR.")

        units = [
            {"unit_id": str(p.get("parCodigo", "")), "name": p.get("parNome", ""), "status": "active"}
            for p in parceiros
        ]
        return _found(units)


# ---------------------------------------------------------------------------
# Lookup — Família Servlet
# (INEA-RJ, SEMAD-GO, SEMAD-MG, FEPAM-RS, IEMA-ES)
# ---------------------------------------------------------------------------

SERVLET_PORTALS = {
    "inea_rj":  {"url": "https://mtr.inea.rj.gov.br/ControllerServlet",             "cnpj_mask": True},
    "semad_go": {"url": "https://mtr.meioambiente.go.gov.br/ControllerServlet",      "cnpj_mask": False},
    "semad_mg": {"url": "https://mtr.meioambiente.mg.gov.br/ControllerServlet",      "cnpj_mask": True,
                 "found_dict_key": "usuario", "found_dict_val": "S"},  # resposta própria do portal
    "fepam_rs": {"url": "https://mtr.fepam.rs.gov.br/ControllerServlet",             "cnpj_mask": True},
    "iema_es":  {"url": "https://mtr.iema.es.gov.br/ControllerServlet",              "cnpj_mask": False},
}


async def lookup_servlet(issuer: str, document: str, entity_type: str) -> dict:
    config = SERVLET_PORTALS[issuer]
    url = config["url"]
    cnpj = fmt_cnpj(document) if config["cnpj_mask"] else document
    tipo = ENTITY_TYPE_CODE[entity_type]

    payload = {"acao": "buscaCpfCnpjUnidades", "cnpj": cnpj, "tipoPessoaSociedade": tipo}

    async with httpx.AsyncClient(verify=False, timeout=HTTPX_TIMEOUT) as client:
        try:
            resp = await client.post(
                url, data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
            )
        except httpx.TimeoutException:
            return _error("TIMEOUT", f"{ISSUER_LABELS[issuer]} não respondeu em 20 segundos.")
        except httpx.RequestError as exc:
            return _error("CONNECTION_ERROR", f"Erro de conexão com {ISSUER_LABELS[issuer]}: {exc}")

        if resp.status_code == 403:
            return _not_found("Acesso negado — CNPJ não encontrado ou bloqueado.")

        if resp.status_code not in (200, 302):
            return _error(f"HTTP_{resp.status_code}", f"{ISSUER_LABELS[issuer]} retornou HTTP {resp.status_code}.")

        body = resp.text.strip()

        if body and body[0] in ("{", "["):
            try:
                data = resp.json()
            except Exception:
                return _error("PARSE_ERROR", "Resposta JSON inválida.")

            if isinstance(data, list):
                if not data:
                    return _not_found("CNPJ não encontrado no portal.")
                return _found(_parse_servlet_units(data))

            if isinstance(data, dict):
                sucesso = str(data.get("sucesso", "")).lower()
                if sucesso in ("n", "false"):
                    return _not_found(data.get("msg") or "CNPJ não encontrado no portal.")
                if sucesso in ("s", "true") or data.get("unidades"):
                    return _found(_parse_servlet_units(data.get("unidades") or [data]))
                # Verifica padrão de "encontrado" específico do portal (ex: SEMAD-MG usa {"usuario":"S"})
                found_key = config.get("found_dict_key")
                found_val = config.get("found_dict_val")
                if found_key and str(data.get(found_key, "")) == str(found_val):
                    return _found([])
                # Dict sem indicador reconhecido (ex: {}) → não encontrado
                return _not_found("CNPJ não encontrado no portal.")

        error_keywords = ["erro", "inválid", "incorret", "não encontrad", "nao encontrad"]
        if any(k in body.lower() for k in error_keywords):
            return _not_found("CNPJ não encontrado no portal.")

        return _not_found("CNPJ não encontrado no portal.")


def _parse_servlet_units(items: list) -> list[dict]:
    units = []
    for item in items:
        if not isinstance(item, dict):
            continue
        units.append({
            "unit_id": str(item.get("codUnidade") or item.get("codigo") or item.get("id") or ""),
            "name": item.get("nomeUnidade") or item.get("nome") or item.get("razaoSocial") or "",
            "status": "active",
        })
    return units


# ---------------------------------------------------------------------------
# Lookup — CTF IBAMA (Cadastro Técnico Federal / Certificado de Regularidade)
# ---------------------------------------------------------------------------
# Endpoint: POST https://servicos.ibama.gov.br/ctf/publico/certificado_regularidade_consulta.php
# O reCAPTCHA é validado apenas no frontend (JS) — o backend PHP não exige o token.
# Resposta em HTML; extraímos os campos via regex nos value= dos inputs.
# ---------------------------------------------------------------------------

CTF_IBAMA_URL = "https://servicos.ibama.gov.br/ctf/publico/certificado_regularidade_consulta.php"

_CTF_FIELD_RE = re.compile(r'id="([^"]+)"\s+name="\1"\s+value="([^"]*)"')
_CTF_AVISO_RE = re.compile(r'id="campo_aviso"[^>]*>(.*?)</span>', re.DOTALL)


async def lookup_ctf_ibama(document: str, entity_type: str) -> dict:
    async with httpx.AsyncClient(verify=False, timeout=HTTPX_TIMEOUT) as client:
        try:
            resp = await client.post(
                CTF_IBAMA_URL,
                data={
                    "num_cpf_cnpj": document,
                    "formDinAcao": "Consultar",
                    "formDinPosVScroll": "",
                    "formDinPosHScroll": "",
                    "num_pessoa": "",
                },
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "User-Agent": "Mozilla/5.0",
                },
            )
        except httpx.TimeoutException:
            return _error("TIMEOUT", "CTF IBAMA não respondeu em 20 segundos.")
        except httpx.RequestError as exc:
            return _error("CONNECTION_ERROR", f"Erro de conexão com CTF IBAMA: {exc}")

        if resp.status_code != 200:
            return _error(f"HTTP_{resp.status_code}", f"CTF IBAMA retornou HTTP {resp.status_code}.")

        html = resp.text

        # Extrai campos dos inputs readonly (encoding Latin-1 do portal → decode automático httpx)
        fields: dict[str, str] = {}
        for m in _CTF_FIELD_RE.finditer(html):
            fields[m.group(1)] = m.group(2)

        nome = fields.get("nom_pessoa", "")
        num_registro = fields.get("num_registro", "")
        dat_validade = fields.get("dat_validade", "")
        dat_constituicao = fields.get("dat_constituicao", "")

        # Determina status pela mensagem do campo aviso
        aviso_match = _CTF_AVISO_RE.search(html)
        aviso_text = aviso_match.group(1).lower() if aviso_match else html.lower()

        nao_possui = "não possui" in aviso_text or "nao possui" in aviso_text or "n\u00e3o possui" in aviso_text
        possui = "possui" in aviso_text and not nao_possui

        if not nome and not num_registro:
            # Sem dados — provavelmente CNPJ não encontrado
            if nao_possui or not possui:
                return _not_found("CNPJ não possui Certificado de Regularidade no CTF/APP IBAMA.")
            return _not_found("CNPJ não encontrado no CTF/APP IBAMA.")

        unit = {
            "unit_id": num_registro,
            "name": nome,
            "status": "Regular" if possui else "Irregular",
            "dat_validade": dat_validade,
            "dat_constituicao": dat_constituicao,
        }

        if possui:
            return _found([unit])
        return {
            "registered": False,
            "units": [unit],
            "code": "INACTIVE",
            "message": "Empresa encontrada no CTF IBAMA mas sem Certificado de Regularidade ativo.",
        }


# ---------------------------------------------------------------------------
# Lookup — CETESB Licença de Operação / Dispensa (e-CETESB)
# ---------------------------------------------------------------------------
# Endpoint público: GET https://e.cetesb.sp.gov.br/portal-servicos-backend/v1/public/requisitions/
# Sem autenticação. Filtra por CNPJ (14 dígitos, sem máscara).
# Retorna processos de licenciamento ambiental (LO, CDL, VRA, DAIL etc.)
# ---------------------------------------------------------------------------

CETESB_LO_API = "https://e.cetesb.sp.gov.br/portal-servicos-backend/v1/public/requisitions/"

CETESB_LO_KEYWORDS = [
    "licença de operação", "renovação de licença de operação",
    "dispensa", "certificado de dispensa",
    "via rápida ambiental", "vra",
    "declaração de atividade isenta", "dail",
]

CETESB_ACTIVE_STATUSES = {"emitido", "em análise", "aguardando pagamento", "aguardando documentos"}


async def lookup_cetesb_lo(document: str, entity_type: str) -> dict:
    async with httpx.AsyncClient(verify=False, timeout=HTTPX_TIMEOUT) as client:
        try:
            resp = await client.get(
                CETESB_LO_API,
                params={"cnpj": document, "page": 0, "size": 100},
            )
        except httpx.TimeoutException:
            return _error("TIMEOUT", "e-CETESB não respondeu em 20 segundos.")
        except httpx.RequestError as exc:
            return _error("CONNECTION_ERROR", f"Erro de conexão com e-CETESB: {exc}")

        if resp.status_code != 200:
            return _error(f"HTTP_{resp.status_code}", f"e-CETESB retornou HTTP {resp.status_code}.")

        try:
            data = resp.json()
        except Exception:
            return _error("PARSE_ERROR", "Resposta do e-CETESB não é JSON válido.")

        content = data.get("content") or []
        if not content:
            return _not_found(
                "Nenhum processo encontrado no e-CETESB para este CNPJ. "
                "Nota: o campo CNPJ nem sempre está preenchido nos registros — "
                "tente buscar pelo nome da empresa em e.cetesb.sp.gov.br."
            )

        # Filtrar por processos relevantes (LO, Dispensa, VRA, DAIL)
        relevant = [
            r for r in content
            if any(kw in (r.get("object") or "").lower() for kw in CETESB_LO_KEYWORDS)
        ]
        if not relevant:
            relevant = content  # retorna tudo se nenhum for LO específico

        has_active = any(
            (r.get("status") or "").lower() in CETESB_ACTIVE_STATUSES
            for r in relevant
        )

        units = []
        for r in relevant:
            units.append({
                "unit_id": str(r.get("requisitionNumber") or r.get("id") or ""),
                "name": r.get("empreendimentoNome") or r.get("stakeholder") or "",
                "status": r.get("status") or "",
                "object": r.get("object") or "",
                "submitted_at": r.get("submittedAt") or "",
                "num_e_ambiente": r.get("numEAmbiente") or "",
            })

        if has_active:
            return _found(units)
        return {
            "registered": False,
            "units": units,
            "code": "INACTIVE",
            "message": "Processos encontrados no e-CETESB mas nenhum com status ativo/emitido.",
        }


# ---------------------------------------------------------------------------
# Lookup — SP Regula (Prefeitura de São Paulo)
# ---------------------------------------------------------------------------
# Dados publicados como XLSX na página da Prefeitura SP (sem API).
# 4 arquivos: RCC Transportadores, RCC Destinos Finais,
#             RGG Transportadores, RGG Destinos Finais.
# URLs dos arquivos mudam mensalmente (data no slug) — busca os links atuais
# antes de baixar. Cache em memória por 24h.
# ---------------------------------------------------------------------------

SPREGULA_PAGE_URL = "https://prefeitura.sp.gov.br/web/spregula/w/residuos_solidos/311649"
SPREGULA_BASE_URL = "https://prefeitura.sp.gov.br"
SPREGULA_CACHE_TTL = 86400  # 24 horas

SPREGULA_CATEGORY_LABELS = {
    "rcc_transportador": "Transportador RCC",
    "rcc_destino":       "Destino Final RCC",
    "rgg_transportador": "Transportador RGG",
    "rgg_destino":       "Destino Final RGG",
}

_spregula_data: dict[str, list[dict]] | None = None
_spregula_loaded_at: float = 0.0


def _categorize_spregula_url(url: str) -> str | None:
    u = url.lower()
    if "transportadores-rcc" in u or "transportador-rcc" in u:
        return "rcc_transportador"
    if "destinos-finais-de-rcc" in u or "destino-final-rcc" in u or "destinos-finais-rcc" in u:
        return "rcc_destino"
    if "transportadores-rgg" in u or "transportador-rgg" in u:
        return "rgg_transportador"
    if "destinos-finais-rgg" in u or "destino-final-rgg" in u or "destinos-rgg" in u:
        return "rgg_destino"
    return None


def _parse_spregula_xlsx(content: bytes, category: str) -> dict[str, list[dict]]:
    """Parseia um XLSX do SP Regula e retorna dict indexado por CNPJ (14 dígitos)."""
    result: dict[str, list[dict]] = {}
    try:
        wb = openpyxl.load_workbook(BytesIO(content), read_only=True, data_only=True)
        sheet = wb.active
        rows = list(sheet.iter_rows(values_only=True))
        wb.close()
    except Exception:
        return result

    if not rows:
        return result

    # Localizar linha de cabeçalho (primeira linha que tem "cnpj" em alguma célula)
    header_idx, cnpj_col = None, None
    for i, row in enumerate(rows[:10]):
        for j, cell in enumerate(row):
            if cell and "cnpj" in str(cell).lower():
                header_idx, cnpj_col = i, j
                break
        if header_idx is not None:
            break

    if header_idx is None or cnpj_col is None:
        return result

    headers = [str(h).strip() if h else f"col_{j}" for j, h in enumerate(rows[header_idx])]

    for row in rows[header_idx + 1:]:
        if not row or cnpj_col >= len(row) or not row[cnpj_col]:
            continue
        cnpj_digits = re.sub(r"\D", "", str(row[cnpj_col]))
        if len(cnpj_digits) != 14:
            continue

        record = {"category": category}
        for j, header in enumerate(headers):
            if j < len(row) and row[j] is not None:
                record[header] = str(row[j]).strip()

        result.setdefault(cnpj_digits, []).append(record)

    return result


async def _load_spregula_cache(client: httpx.AsyncClient) -> None:
    """Baixa os 4 XLSXs do SP Regula e indexa por CNPJ."""
    global _spregula_data, _spregula_loaded_at

    if _spregula_data is not None and (time.time() - _spregula_loaded_at) < SPREGULA_CACHE_TTL:
        return

    # 1. Busca os links atuais na página
    try:
        resp = await client.get(SPREGULA_PAGE_URL, follow_redirects=True, timeout=30.0)
        raw_links = re.findall(r"/documents/d/spregula/[^\s\"'<>]+", resp.text)
    except Exception:
        return

    file_map: dict[str, str] = {}
    for link in raw_links:
        cat = _categorize_spregula_url(link)
        if cat and cat not in file_map:
            file_map[cat] = SPREGULA_BASE_URL + link

    if not file_map:
        return

    # 2. Baixa e parseia cada arquivo
    merged: dict[str, list[dict]] = {}
    for category, url in file_map.items():
        try:
            r = await client.get(url, follow_redirects=True, timeout=45.0)
            if r.status_code != 200:
                continue
            for cnpj, records in _parse_spregula_xlsx(r.content, category).items():
                merged.setdefault(cnpj, []).extend(records)
        except Exception:
            continue

    if merged:
        _spregula_data = merged
        _spregula_loaded_at = time.time()


async def lookup_spregula(document: str, entity_type: str) -> dict:
    async with httpx.AsyncClient(verify=False, timeout=50.0, follow_redirects=True) as client:
        await _load_spregula_cache(client)

    if _spregula_data is None:
        return _error("CONFIG_ERROR", "Não foi possível carregar os dados do SP Regula.")

    records = _spregula_data.get(document, [])
    if not records:
        return _not_found("CNPJ não encontrado nas listas de operadores do SP Regula.")

    units = []
    has_active = False
    for rec in records:
        category = rec.get("category", "")
        cat_label = SPREGULA_CATEGORY_LABELS.get(category, category)
        name = (
            rec.get("Razao Social") or rec.get("Razão Social") or
            rec.get("NOME FANTASIA") or rec.get("DESTINO FINAL") or ""
        )
        validade = rec.get("VALIDADE") or rec.get("Validade") or ""
        codigo = rec.get("Número Cadastro") or rec.get("CÓDIGO CADASTRO") or rec.get("Código Cadastro") or ""
        status = rec.get("Status") or rec.get("STATUS") or "Ativo"
        modalidades = rec.get("Modalidades") or rec.get("MODALIDADES") or ""

        if status.strip().lower() in ("ativo", "active", ""):
            has_active = True

        units.append({
            "unit_id": codigo,
            "name": name,
            "category": cat_label,
            "status": status,
            "validade": validade,
            "modalidades": modalidades,
        })

    if has_active:
        return _found(units)
    return {
        "registered": False,
        "units": units,
        "code": "INACTIVE",
        "message": "Empresa encontrada no SP Regula mas com status inativo.",
    }


# ---------------------------------------------------------------------------
# Portais não suportados
# ---------------------------------------------------------------------------

def lookup_not_supported(issuer: str) -> dict:
    reason = NOT_SUPPORTED_ISSUERS.get(issuer, "Portal não suportado para lookup público.")
    return {"registered": None, "units": [], "code": "NOT_SUPPORTED", "message": reason}


# ---------------------------------------------------------------------------
# Builders de resultado
# ---------------------------------------------------------------------------

def _found(units: list[dict]) -> dict:
    return {"registered": True, "units": units, "code": "FOUND", "message": "Cadastro encontrado."}


def _not_found(message: str) -> dict:
    return {"registered": False, "units": [], "code": "NOT_FOUND", "message": message}


def _error(code: str, message: str) -> dict:
    return {"registered": None, "units": [], "code": code, "message": message}


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

async def run_lookup(issuer: str, document: str, entity_type: str) -> dict:
    if issuer in NOT_SUPPORTED_ISSUERS:
        return lookup_not_supported(issuer)
    if issuer == "brasilapi_cnpj":
        return await lookup_brasilapi_cnpj(document, entity_type)
    if issuer == "rntrc":
        return await lookup_rntrc(document, entity_type)
    if issuer == "ctf_ibama":
        return await lookup_ctf_ibama(document, entity_type)
    if issuer == "cetesb_lo":
        return await lookup_cetesb_lo(document, entity_type)
    if issuer == "spregula":
        return await lookup_spregula(document, entity_type)
    if issuer == "sigor_sp":
        return await lookup_sigor_sp(document, entity_type)
    if issuer == "sinir":
        return await lookup_sinir(document, entity_type)
    if issuer in SERVLET_PORTALS:
        return await lookup_servlet(issuer, document, entity_type)
    return lookup_not_supported(issuer)


async def run_lookup_all_issuers(document: str, entity_types: list[str]) -> dict:
    """
    Consulta todos os portais suportados para os entity_types informados.
    BrasilAPI e RNTRC são consultados uma única vez (independente de entity_type).
    Para os portais MTR, mescla os resultados (registered=True prevalece).
    """
    all_issuers = list(ISSUER_LABELS.keys())

    # Portais independentes de entity_type (consulta uma única vez)
    entity_independent = {"brasilapi_cnpj", "rntrc", "ctf_ibama", "cetesb_lo", "spregula"}

    async def lookup_for_type(issuer: str, entity_type: str) -> tuple[str, str, dict]:
        result = await run_lookup(issuer, document, entity_type)
        return issuer, entity_type, result

    tasks = []
    seen_independent = set()
    for issuer in all_issuers:
        if issuer in entity_independent:
            if issuer not in seen_independent:
                seen_independent.add(issuer)
                tasks.append(lookup_for_type(issuer, entity_types[0]))
        else:
            for entity_type in entity_types:
                tasks.append(lookup_for_type(issuer, entity_type))

    raw = await asyncio.gather(*tasks)

    # Mescla por portal: registered=True vence
    merged: dict[str, dict] = {}
    for issuer, entity_type, result in raw:
        if issuer not in merged:
            merged[issuer] = result
        else:
            existing = merged[issuer]
            if result.get("registered") is True and existing.get("registered") is not True:
                merged[issuer] = result

    return merged


# ---------------------------------------------------------------------------
# Slack Bot — formatação e handler
# ---------------------------------------------------------------------------

ENTITY_TYPE_LABELS = {"transporter": "transportador", "receiver": "receptor"}

ISSUER_ICONS = {
    True: "✅",
    False: "❌",
    None: "⚠️",
}


def _detect_entity_types(text: str) -> list[str]:
    """Detecta entity_type(s) a partir do texto da mensagem."""
    text_lower = text.lower()
    types = []
    if any(w in text_lower for w in ("transportador", "transporter")):
        types.append("transporter")
    if any(w in text_lower for w in ("receptor", "destinador", "receiver")):
        types.append("receiver")
    return types or ["transporter", "receiver"]


def _format_unit(unit: dict) -> str:
    parts = []
    if unit.get("name"):
        parts.append(unit["name"])
    if unit.get("address"):
        parts.append(unit["address"])
    if unit.get("origin") and unit["origin"] not in (unit.get("name") or ""):
        parts.append(f"origem: {unit['origin']}")
    if unit.get("unit_id"):
        parts.append(f"ID: {unit['unit_id']}")
    return " | ".join(p for p in parts if p)


def _format_brasilapi_line(result: dict) -> str:
    """Formata linha(s) do Cartão CNPJ com mais detalhes."""
    registered = result.get("registered")
    icon = ISSUER_ICONS.get(registered, "⚠️")
    label = ISSUER_LABELS["brasilapi_cnpj"]
    units = result.get("units") or []

    if registered is None:
        return f"{icon} *{label}* — {result.get('message', 'erro na consulta')}"

    if not units:
        return f"{icon} *{label}* — {result.get('message', 'não encontrado')}"

    u = units[0]
    situacao = u.get("status", "")
    name = u.get("name", "")
    fantasy = u.get("fantasy_name", "")
    cnae = u.get("cnae", "")
    porte = u.get("porte", "")
    municipio = u.get("municipio", "")
    uf = u.get("uf", "")

    display_name = f"{name} ({fantasy})" if fantasy and fantasy.upper() != name.upper() else name
    loc = f"{municipio}/{uf}" if municipio and uf else municipio or uf
    details = " | ".join(p for p in [loc, porte] if p)

    lines = [f"{icon} *{label}* — {situacao}"]
    if display_name:
        lines.append(f"   {display_name}")
    if cnae:
        lines.append(f"   CNAE: {cnae}")
    if details:
        lines.append(f"   {details}")
    return "\n".join(lines)


def _format_rntrc_line(result: dict) -> str:
    """Formata linha do RNTRC com status e número de registro."""
    registered = result.get("registered")
    icon = ISSUER_ICONS.get(registered, "⚠️")
    label = ISSUER_LABELS["rntrc"]
    units = result.get("units") or []

    if registered is None:
        return f"{icon} *{label}* — {result.get('message', 'erro na consulta')}"

    if not units:
        return f"{icon} *{label}* — não registrado"

    u = units[0]
    name = u.get("name", "")
    numero = u.get("unit_id", "")
    categoria = u.get("category", "")
    municipio = u.get("municipio", "")
    uf = u.get("uf", "")
    situacao = u.get("status", "")
    data_cadastro = u.get("data_cadastro", "")

    loc = f"{municipio}/{uf}" if municipio and uf else municipio or uf
    rntrc_info = f"RNTRC nº {numero}" if numero else ""
    details = " | ".join(p for p in [rntrc_info, categoria, loc] if p)

    lines = [f"{icon} *{label}* — {situacao}"]
    if name:
        lines.append(f"   {name}")
    if details:
        lines.append(f"   {details}")
    if data_cadastro:
        lines.append(f"   Cadastro: {data_cadastro}")
    if len(units) > 1:
        lines.append(f"   (+{len(units)-1} registros adicionais)")
    return "\n".join(lines)


def _format_ctf_ibama_line(result: dict) -> str:
    """Formata resultado do CTF IBAMA com número de registro e validade do CR."""
    registered = result.get("registered")
    icon = ISSUER_ICONS.get(registered, "⚠️")
    label = ISSUER_LABELS["ctf_ibama"]
    units = result.get("units") or []

    if registered is None:
        return f"{icon} *{label}* — {result.get('message', 'erro na consulta')}"
    if not units:
        return f"{icon} *{label}* — {result.get('message', 'não encontrado')}"

    u = units[0]
    status = u.get("status", "")
    num = u.get("unit_id", "")
    validade = u.get("dat_validade", "")
    name = u.get("name", "")

    lines = [f"{icon} *{label}* — {status}"]
    if name:
        lines.append(f"   {name}")
    detail = " | ".join(p for p in [f"Registro nº {num}" if num else "", f"CR válido até: {validade}" if validade else ""] if p)
    if detail:
        lines.append(f"   {detail}")
    return "\n".join(lines)


def _format_cetesb_lo_line(result: dict) -> str:
    """Formata resultado do e-CETESB com tipo de processo e status."""
    registered = result.get("registered")
    icon = ISSUER_ICONS.get(registered, "⚠️")
    label = ISSUER_LABELS["cetesb_lo"]
    units = result.get("units") or []

    if registered is None:
        return f"{icon} *{label}* — {result.get('message', 'erro na consulta')}"
    if not units:
        return f"{icon} *{label}* — {result.get('message', 'não encontrado')}"

    # Agrupar por status para resumo
    emitidos = [u for u in units if (u.get("status") or "").lower() == "emitido"]
    pendentes = [u for u in units if (u.get("status") or "").lower() in ("em análise", "aguardando pagamento", "aguardando documentos")]

    lines = [f"{icon} *{label}*"]
    if emitidos:
        u = emitidos[0]
        lines.append(f"   ✔ {u.get('object', '')} — *{u.get('status', '')}*")
        if u.get("num_e_ambiente"):
            lines.append(f"   Nº e-Ambiente: {u['num_e_ambiente']}")
        if len(emitidos) > 1:
            lines.append(f"   (+{len(emitidos)-1} processo(s) emitido(s))")
    if pendentes:
        lines.append(f"   ⏳ {len(pendentes)} processo(s) em andamento")
    if not emitidos and not pendentes and units:
        u = units[0]
        lines.append(f"   {u.get('object', '')} — {u.get('status', '')}")

    return "\n".join(lines)


def _format_spregula_line(result: dict) -> str:
    """Formata resultado do SP Regula com categorias e validade."""
    registered = result.get("registered")
    icon = ISSUER_ICONS.get(registered, "⚠️")
    label = ISSUER_LABELS["spregula"]
    units = result.get("units") or []

    if registered is None:
        return f"{icon} *{label}* — {result.get('message', 'erro na consulta')}"
    if not units:
        return f"{icon} *{label}* — {result.get('message', 'não encontrado')}"

    lines = [f"{icon} *{label}*"]
    for u in units:
        cat = u.get("category", "")
        validade = u.get("validade", "")
        codigo = u.get("unit_id", "")
        status = u.get("status", "")
        modalidades = u.get("modalidades", "")
        detail_parts = [p for p in [cat, f"Cód. {codigo}" if codigo else "", f"Validade: {validade}" if validade else "", status] if p]
        lines.append(f"   • {' | '.join(detail_parts)}")
        if modalidades:
            lines.append(f"     Modalidades: {modalidades}")
    return "\n".join(lines)


def _build_slack_block(document: str, entity_types: list[str], results: dict) -> list[dict]:
    """
    Monta os blocos Slack para um CNPJ em duas seções:
    1. Consulta de documentos  — Cartão CNPJ, RNTRC, CETESB LO
    2. Cadastro em órgãos emissores — SP Regula, SIGOR, SINIR, portais estaduais
    IMA-AL não exibido (não suporta consulta por CNPJ).
    """
    tipo_label = " + ".join(ENTITY_TYPE_LABELS.get(t, t) for t in entity_types)
    cnpj_fmt = fmt_cnpj(document)

    # --- Seção 1: Consulta de documentos ---
    doc_lines = [
        f"*CNPJ: {cnpj_fmt}*  |  Tipo: _{tipo_label}_\n",
        "*#Consulta de documentos*",
        _format_brasilapi_line(results.get("brasilapi_cnpj", {})),
        _format_rntrc_line(results.get("rntrc", {})),
        _format_ctf_ibama_line(results.get("ctf_ibama", {})),
        _format_cetesb_lo_line(results.get("cetesb_lo", {})),
    ]

    # --- Seção 2: Cadastro em órgãos emissores ---
    emissores_issuers = [
        "spregula", "sigor_sp", "sinir",
        "inea_rj", "semad_go", "semad_mg", "fepam_rs", "iema_es",
    ]

    emissor_lines = ["*#Cadastro em órgãos emissores*"]
    for issuer in emissores_issuers:
        label = ISSUER_LABELS[issuer]
        result = results.get(issuer, {})
        registered = result.get("registered")

        if issuer == "spregula":
            emissor_lines.append(_format_spregula_line(result))
        else:
            icon = ISSUER_ICONS.get(registered, "⚠️")
            if registered is True:
                units = result.get("units") or []
                if units:
                    first = _format_unit(units[0])
                    extra = f"  (+{len(units)-1} unidades)" if len(units) > 1 else ""
                    emissor_lines.append(f"{icon} *{label}* — {first}{extra}")
                else:
                    emissor_lines.append(f"{icon} *{label}* — cadastro encontrado")
            elif registered is False:
                emissor_lines.append(f"{icon} *{label}* — não encontrado")
            else:
                emissor_lines.append(f"{icon} *{label}* — {result.get('message', 'erro na consulta')}")

    section1_text = "\n".join(doc_lines)
    divider = "---------"
    section2_text = "\n".join(emissor_lines)

    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": section1_text}},
        {"type": "section", "text": {"type": "mrkdwn", "text": divider}},
        {"type": "section", "text": {"type": "mrkdwn", "text": section2_text}},
    ]


@slack_app.event("app_mention")
async def handle_mention(event, say, logger):
    text = event.get("text", "")
    thread_ts = event.get("thread_ts") or event.get("ts")
    channel = event["channel"]

    cnpjs = [strip_doc(m) for m in CNPJ_REGEX.findall(text)]
    cnpjs = [c for c in cnpjs if len(c) == 14]

    if not cnpjs:
        await say(
            text="Não encontrei nenhum CNPJ na mensagem. Tente: `@judy 12.345.678/0001-90`",
            channel=channel,
            thread_ts=thread_ts,
        )
        return

    entity_types = _detect_entity_types(text)

    # Confirmação imediata (Slack exige resposta em 3s)
    tipo_label = " + ".join(ENTITY_TYPE_LABELS.get(t, t) for t in entity_types)
    await say(
        text=f"🔍 Verificando {len(cnpjs)} CNPJ(s) como _{tipo_label}_ em todos os portais...",
        channel=channel,
        thread_ts=thread_ts,
    )

    for cnpj in cnpjs:
        try:
            results = await run_lookup_all_issuers(cnpj, entity_types)
            blocks = _build_slack_block(cnpj, entity_types, results)
            await say(blocks=blocks, text=f"Resultado para {fmt_cnpj(cnpj)}", channel=channel, thread_ts=thread_ts)
        except Exception as exc:
            logger.error(f"Erro ao consultar CNPJ {cnpj}: {exc}")
            await say(text=f"⚠️ Erro ao consultar CNPJ `{fmt_cnpj(cnpj)}`: {exc}", channel=channel, thread_ts=thread_ts)


# ---------------------------------------------------------------------------
# FastAPI Endpoints
# ---------------------------------------------------------------------------

@app.post("/slack/events")
async def slack_events(req: Request):
    # Ignora retentativas do Slack (enviadas quando a resposta demora >3s)
    # evita que o bot responda múltiplas vezes para o mesmo evento
    if req.headers.get("X-Slack-Retry-Num"):
        return {"ok": True}

    # Responde ao challenge de verificação imediatamente
    try:
        body = await req.json()
        if body.get("type") == "url_verification":
            return {"challenge": body["challenge"]}
    except Exception:
        pass

    return await slack_handler.handle(req)


@app.post("/lookup/operator")
async def lookup_operator(req: LookupRequest):
    """Verifica cadastro em um portal específico."""
    result = await run_lookup(req.issuer, req.document, req.entity_type)
    return {
        "entity_type": req.entity_type,
        "document": req.document,
        "issuer": req.issuer,
        "issuer_label": ISSUER_LABELS[req.issuer],
        **result,
    }


@app.post("/lookup/operator/multi")
async def lookup_operator_multi(req: MultiLookupRequest):
    """Verifica cadastro em múltiplos portais simultaneamente."""
    tasks = [run_lookup(issuer, req.document, req.entity_type) for issuer in req.issuers]
    results_raw = await asyncio.gather(*tasks)

    results = [
        {"issuer": issuer, "issuer_label": ISSUER_LABELS[issuer], **result}
        for issuer, result in zip(req.issuers, results_raw)
    ]

    return {
        "entity_type": req.entity_type,
        "document": req.document,
        "stats": {
            "total": len(results),
            "found": sum(1 for r in results if r["registered"] is True),
            "not_found": sum(1 for r in results if r["registered"] is False),
            "error_or_unsupported": sum(1 for r in results if r["registered"] is None),
        },
        "results": results,
    }


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.3.0"}
