from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


CompanyScope = Literal["app_sales", "real_estate", "mortgage", "shared"]
SignalType = Literal["product", "gtm", "content", "data", "automation", "channel", "market", "ops"]
SourceType = Literal["repo", "rss", "reddit", "x", "blog", "website", "official_doc"]
ContentType = Literal["x_post", "thread", "linkedin", "email", "newsletter", "carousel", "script"]
TargetAudience = Literal["agents", "principals", "mortgage", "proptech", "founders"]
CampaignType = Literal["app_sales", "seller", "buyer", "mortgage"]
CampaignStage = Literal["first_touch", "follow_up_1", "follow_up_2", "reengage"]
CampaignChannel = Literal["email", "whatsapp", "sms"]
CommandType = Literal[
    "RESEARCH_TOPIC",
    "SYNC_SOURCES",
    "GENERATE_CONTENT",
    "REPURPOSE_ITEM",
    "BUILD_CAMPAIGN",
    "SUMMARIZE_WEEK",
    "FIND_COMPETITOR_MOVES",
    "FIND_OPEN_SOURCE_PATTERNS",
]


class ResearchFindingPayload(BaseModel):
    source_type: SourceType
    source_name: str
    url: str
    topic: str
    summary: str
    why_it_matters: str
    company_scope: CompanyScope
    signal_type: SignalType
    novelty_score: float
    confidence_score: float
    actionability_score: float
    proposed_actions: List[str] = Field(default_factory=list)


class ContentDraftPayload(BaseModel):
    content_type: ContentType
    target_audience: TargetAudience
    hook: str
    body: str
    cta: str
    source_refs: List[str] = Field(default_factory=list)
    repurposable: bool = True


class CampaignDraftPayload(BaseModel):
    campaign_type: CampaignType
    stage: CampaignStage
    channel: CampaignChannel
    subject: str
    message: str
    goal: str


class MemoryEntryPayload(BaseModel):
    memory_type: str
    title: str
    body: str
    tags: List[str] = Field(default_factory=list)
    source_refs: List[str] = Field(default_factory=list)
    confidence: float = 0.5
    expires_at: Optional[str] = None


class HermesSourceCreateRequest(BaseModel):
    name: str
    source_type: str
    base_url: str
    rss_url: Optional[str] = None
    enabled: bool = True
    fetch_frequency_minutes: int = Field(default=180, ge=5, le=10080)
    tags: List[str] = Field(default_factory=list)
    company_scope: str = "shared"
    credibility_score: float = Field(default=0.7, ge=0.0, le=1.0)


class HermesSourcePatchRequest(BaseModel):
    name: Optional[str] = None
    source_type: Optional[str] = None
    base_url: Optional[str] = None
    rss_url: Optional[str] = None
    enabled: Optional[bool] = None
    fetch_frequency_minutes: Optional[int] = Field(default=None, ge=5, le=10080)
    tags: Optional[List[str]] = None
    company_scope: Optional[str] = None
    credibility_score: Optional[float] = Field(default=None, ge=0.0, le=1.0)


class HermesSyncSourcesRequest(BaseModel):
    source_ids: List[str] = Field(default_factory=list)
    force: bool = False


class HermesApproveContentRequest(BaseModel):
    content_id: str
    approved_by: str = "operator"
    note: str = ""


class HermesApproveCampaignRequest(BaseModel):
    campaign_id: str
    approved_by: str = "operator"
    note: str = ""


class HermesCommandRequest(BaseModel):
    command_type: CommandType
    prompt: str = ""
    finding_ids: List[str] = Field(default_factory=list)
    source_ids: List[str] = Field(default_factory=list)
    campaign_type: Optional[str] = None
    content_type: Optional[str] = None
    channel: Optional[str] = None
    operator_note: str = ""
    options: Dict[str, Any] = Field(default_factory=dict)
