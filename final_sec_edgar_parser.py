# Enhanced SEC EDGAR parser and indexer (HTML tables, optional XBRL via Arelle, submissions JSON index)

from typing import List, Dict, Any, Optional
import re

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

# Optional Arelle integration for XBRL if installed
try:
    from arelle import Cntlr
    HAVE_ARELLE = True
except Exception:
    Cntlr = None
    HAVE_ARELLE = False

LABEL_MAP = {
    r"^revenue|sales|total revenue$": "revenue",
    r"^gross profit$": "gross_profit",
    r"^operating income|operating profit|ebit$": "operating_income",
    r"^net income|net earnings|profit attributable": "net_income",
    r"^basic earnings per share|eps basic$": "eps_basic",
    r"^diluted earnings per share|eps diluted$": "eps_diluted",
    r"^net cash provided by operating activities|operating cash flow$": "operating_cash_flow",
    r"^capital expenditures|capex$": "capex",
    r"^free cash flow$": "free_cash_flow",
    r"^total assets$": "total_assets",
    r"^total liabilities$": "total_liabilities",
}
COMPILED = [(re.compile(pat, re.I), field) for pat, field in LABEL_MAP.items()]


def _normalize_label(text: str) -> Optional[str]:
    t = (text or "").strip()
    for rx, field in COMPILED:
        if rx.search(t):
            return field
    return None


def parse_html_financial_table(html: str, ticker: Optional[str] = None, company: Optional[str] = None, currency: Optional[str] = None) -> List[Dict[str, Any]]:
    if BeautifulSoup is None:
        raise RuntimeError("bs4 is required to parse HTML tables")
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    records: List[Dict[str, Any]] = []
    for tbl in tables:
        rows = tbl.find_all("tr")
        if not rows:
            continue
        headers = [th.get_text(" ", strip=True) for th in rows[0].find_all(["th", "td"]) ]
        for r in rows[1:]:
            cols = r.find_all(["td", "th"])
            if len(cols) < 2:
                continue
            label = _normalize_label(cols[0].get_text(" ", strip=True))
            if not label:
                continue
            for idx in range(1, len(cols)):
                period = headers[idx] if idx < len(headers) else ""
                value_text = cols[idx].get_text(" ", strip=True)
                rec = {
                    "ticker": ticker,
                    "company": company,
                    "period_header": period,
                    "label": label,
                    "value_raw": value_text,
                    "currency": currency
                }
                records.append(rec)
    return records


def records_to_periodized_financials(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import datetime as _dt
    out: Dict[str, Dict[str, Any]] = {}
    def parse_period(p: str):
        p = (p or "").strip()
        fy = None
        fq = None
        date = None
        m = re.search(r"(20\d{2})", p)
        if m:
            fy = int(m.group(1))
        mq = re.search(r"Q\s*(\d)", p, re.I)
        if mq:
            fq = int(mq.group(1))
        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%b %d, %Y", "%B %d, %Y"]:
            try:
                date = _dt.datetime.strptime(p, fmt).date().isoformat()
                break
            except Exception:
                pass
        return fy, fq, date
    for rec in records:
        key = rec.get("period_header") or ""
        fy, fq, date = parse_period(key)
        out_key = (fy, fq, date, rec.get("currency"), rec.get("ticker"), rec.get("company"))
        if out_key not in out:
            out[out_key] = {
                "ticker": rec.get("ticker"),
                "company": rec.get("company"),
                "fiscal_year": fy,
                "fiscal_quarter": fq,
                "period_end": date,
                "currency": rec.get("currency")
            }
        field = rec.get("label")
        out[out_key][field] = rec.get("value_raw")
    return list(out.values())

# Optional: parse XBRL file via Arelle controller if available. Minimal fact extraction for selected concepts.
XBRL_CONCEPTS = {
    "revenue": ["us-gaap:Revenues", "us-gaap:SalesRevenueNet"],
    "gross_profit": ["us-gaap:GrossProfit"],
    "operating_income": ["us-gaap:OperatingIncomeLoss"],
    "net_income": ["us-gaap:NetIncomeLoss"],
    "operating_cash_flow": ["us-gaap:NetCashProvidedByUsedInOperatingActivities"],
    "capex": ["us-gaap:PaymentsToAcquirePropertyPlantAndEquipment"],
    "total_assets": ["us-gaap:Assets"],
    "total_liabilities": ["us-gaap:Liabilities"],
}


def parse_xbrl_via_arelle(xbrl_path: str, ticker: Optional[str] = None, company: Optional[str] = None, currency: Optional[str] = None) -> List[Dict[str, Any]]:
    if not HAVE_ARELLE:
        raise RuntimeError("Arelle is not installed. Install arelle to enable XBRL parsing.")
    ctrl = Cntlr.Cntlr(logFileName=None)
    model = ctrl.modelManager.load(xbrl_path)
    facts = []
    for f in model.factsInInstance:
        qname = f.qname if hasattr(f, "qname") else None
        if not qname:
            continue
        concept = str(qname)
        for field, concepts in XBRL_CONCEPTS.items():
            if concept in concepts:
                # Use context end date to infer period
                period_end = None
                try:
                    period_end = f.context.endDatetime.date().isoformat()
                except Exception:
                    try:
                        period_end = f.context.instantDatetime.date().isoformat()
                    except Exception:
                        period_end = None
                val = None
                try:
                    val = float(str(f.xValue))
                except Exception:
                    val = None
                facts.append({
                    "ticker": ticker,
                    "company": company,
                    "period_end": period_end,
                    field: val,
                    "currency": currency
                })
    # Coalesce by period_end
    out: Dict[str, Dict[str, Any]] = {}
    for r in facts:
        key = (r.get("period_end"), r.get("currency"), r.get("ticker"), r.get("company"))
        if key not in out:
            out[key] = {"ticker": r.get("ticker"), "company": r.get("company"), "period_end": r.get("period_end"), "currency": r.get("currency")}
        for k, v in r.items():
            if k not in ["ticker", "company", "period_end", "currency"]:
                out[key][k] = v
    return list(out.values())

# Indexer utilities (offline friendly): parse submissions JSON structure
# Caller is expected to supply the already-downloaded submissions JSON content.

def filings_from_submissions_json(submissions: Dict[str, Any], forms: Optional[List[str]] = None, max_items: Optional[int] = None) -> List[Dict[str, Any]]:
    forms = forms or ["10-K", "10-Q"]
    out = []
    filings = submissions.get("filings", {}).get("recent", {})
    tickers = submissions.get("tickers") or []
    cik = submissions.get("cik")
    for i in range(len(filings.get("form", []))):
        form = filings["form"][i]
        if form not in forms:
            continue
        acc = filings["accessionNumber"][i]
        report_date = filings["reportDate"][i] if i < len(filings.get("reportDate", [])) else None
        period = filings["periodOfReport"][i] if i < len(filings.get("periodOfReport", [])) else None
        primary_doc = filings["primaryDocument"][i]
        out.append({
            "cik": cik,
            "ticker": tickers[0] if tickers else None,
            "form": form,
            "accession": acc,
            "report_date": report_date,
            "period_of_report": period,
            "primary_document": primary_doc
        })
        if max_items and len(out) >= max_items:
            break
    return out