"""
Traceability Agent — Operator Lookup + Judy Slack Bot
Verifica cadastro ativo de transportadores e receptores nos portais regulatórios MTR.
Bot @judy responde no Slack com resultados por órgão emissor.
"""

import os
import re
import asyncio
from typing import Literal

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
    version="1.1.0",
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
    "sigor_sp": "SIGOR-SP (CETESB)",
    "sinir": "SINIR (IBAMA Federal)",
    "inea_rj": "INEA-RJ",
    "semad_go": "SEMAD-GO",
    "semad_mg": "SEMAD-MG",
    "fepam_rs": "FEPAM-RS",
    "iema_es": "IEMA-ES",
    "ima_al": "IMA-AL",
}

SUPPORTED_ISSUERS = {"sigor_sp", "sinir", "inea_rj", "semad_go", "semad_mg", "fepam_rs", "iema_es"}
NOT_SUPPORTED_ISSUERS = {
    "ima_al": "IMA-AL requer CPF para lookup de unidades — consulta só por CNPJ não suportada.",
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
    "semad_mg": {"url": "https://mtr.meioambiente.mg.gov.br/ControllerServlet",      "cnpj_mask": True},
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

        if not body:
            return _found([])

        error_keywords = ["erro", "inválid", "incorret", "não encontrad", "nao encontrad"]
        if any(k in body.lower() for k in error_keywords):
            return _not_found("CNPJ não encontrado no portal.")

        return _found([])


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
    Se entity_types tiver mais de um, mescla os resultados por portal
    (registered=True prevalece sobre False).
    """
    all_issuers = list(ISSUER_LABELS.keys())

    async def lookup_for_type(issuer: str, entity_type: str) -> tuple[str, str, dict]:
        result = await run_lookup(issuer, document, entity_type)
        return issuer, entity_type, result

    tasks = [
        lookup_for_type(issuer, entity_type)
        for issuer in all_issuers
        for entity_type in entity_types
    ]
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


def _build_slack_block(document: str, entity_types: list[str], results: dict) -> list[dict]:
    """Monta os blocos Slack para um CNPJ."""
    tipo_label = " + ".join(ENTITY_TYPE_LABELS.get(t, t) for t in entity_types)
    cnpj_fmt = fmt_cnpj(document)

    lines = [f"*CNPJ: {cnpj_fmt}*  |  Tipo: _{tipo_label}_\n"]

    for issuer, label in ISSUER_LABELS.items():
        result = results.get(issuer, {})
        registered = result.get("registered")
        icon = ISSUER_ICONS.get(registered, "⚠️")

        if registered is True:
            units = result.get("units") or []
            if units:
                first = _format_unit(units[0])
                extra = f"  (+{len(units)-1} unidades)" if len(units) > 1 else ""
                lines.append(f"{icon} *{label}* — {first}{extra}")
            else:
                lines.append(f"{icon} *{label}* — cadastro encontrado")
        elif registered is False:
            lines.append(f"{icon} *{label}* — não encontrado")
        else:
            lines.append(f"{icon} *{label}* — {result.get('message', 'não suportado')}")

    return [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}]


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
    return {"status": "ok", "version": "1.1.0"}
