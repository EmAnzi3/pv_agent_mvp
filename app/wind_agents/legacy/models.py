from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class ProjectRecord(BaseModel):
    source: str
    source_url: Optional[str] = None
    external_id: Optional[str] = None

    title: str
    description: Optional[str] = None

    region: Optional[str] = None
    province: Optional[str] = None
    municipality: Optional[str] = None
    address: Optional[str] = None

    sector: Optional[str] = None
    category: Optional[str] = None
    intervention_type: Optional[str] = None
    phase: Optional[str] = None
    status: Optional[str] = None

    # Committente / soggetto titolare
    client: Optional[str] = None
    client_type: Optional[str] = None

    # Figure tecniche / amministrative
    designer: Optional[str] = None
    works_director: Optional[str] = None
    rup: Optional[str] = None

    # Azienda incaricata / aggiudicatario / appaltatore
    contractor: Optional[str] = None
    contractor_name: Optional[str] = None
    contractor_vat: Optional[str] = None
    contractor_tax_code: Optional[str] = None
    contractor_address: Optional[str] = None
    contractor_city: Optional[str] = None
    contractor_province: Optional[str] = None
    contractor_pec: Optional[str] = None
    contractor_email: Optional[str] = None
    contractor_phone: Optional[str] = None

    # Referente utile, se pubblicato
    contact_name: Optional[str] = None
    contact_role: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None

    # Valori tecnici/economici
    estimated_value_eur: Optional[float] = None
    award_amount_eur: Optional[float] = None
    power_mw: Optional[float] = None
    surface_mq: Optional[float] = None

    # Codici e finanziamento
    cup: Optional[str] = None
    cig: Optional[str] = None
    funding: Optional[str] = None

    # Date
    tender_date: Optional[str] = None
    award_date: Optional[str] = None
    works_start: Optional[str] = None
    works_end: Optional[str] = None

    # Fonti arricchimento
    award_source_url: Optional[str] = None
    enrichment_status: Optional[str] = None

    # Scoring / note
    commercial_score: int = 0
    ai_notes: Optional[str] = None

    first_seen: str = Field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    last_seen: str = Field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
