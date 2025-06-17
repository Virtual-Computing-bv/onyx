from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from typing import List
from typing import Optional

import requests

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.interfaces import GenerateDocumentsOutput
from onyx.connectors.interfaces import LoadConnector
from onyx.connectors.interfaces import PollConnector
from onyx.connectors.interfaces import SecondsSinceUnixEpoch
from onyx.connectors.models import ConnectorMissingCredentialError
from onyx.connectors.models import Document
from onyx.connectors.models import TextSection
from onyx.file_processing.html_utils import parse_html_page_basic
from onyx.utils.logger import setup_logger

logger = setup_logger()

_ZAMMAD_ID_PREFIX = "ZAMMAD_"

KEYS = {
    "id",
    "number",
    "title",
    "state",
    "priority",
    "group",
    "owner",
    "customer",
    "customer_id",
    "created_at",
    "updated_at",
}


def _create_metadata_from_ticket(ticket: dict) -> dict:
    # Extract key fields into metadata
    metadata: dict[str, str] = {}
    for key in KEYS:
        if key in ticket and ticket[key] is not None:
            metadata[key] = str(ticket[key])
    return metadata


def _create_doc_from_ticket(ticket: dict, base_url: str, api_key: str) -> Document:
    # Build ticket URL for UI
    ticket_id = ticket.get("id")
    ticket_url = f"{base_url}/#ticket/zoom/{ticket_id}"

    # Fetch all articles for this ticket
    headers = {"Authorization": f"Token token={api_key}"}

    articles_resp = requests.get(
        f"{base_url}/api/v1/ticket_articles/by_ticket/{ticket_id}", headers=headers
    )

    articles_resp.raise_for_status()
    articles = articles_resp.json()

    # Create sections from each article
    sections: List[TextSection] = []
    for article in articles:
        body_html = article.get("body", "")
        text = parse_html_page_basic(body_html)

        # Link to the API view of the article
        article_url = f"{base_url}/api/v1/ticket_articles/{article.get('id')}"
        sections.append(TextSection(link=article_url, text=text))

    metadata = _create_metadata_from_ticket(ticket)
    # Parse updated_at for doc_updated_at
    updated_at = datetime.fromisoformat(ticket["updated_at"].replace("Z", "+00:00"))

    return Document(
        id=_ZAMMAD_ID_PREFIX + ticket_url,
        sections=sections,
        source=DocumentSource.ZAMMAD,
        semantic_identifier=ticket.get("number", str(ticket_id)),
        metadata=metadata,
        doc_updated_at=updated_at,
    )


class ZammadConnector(LoadConnector, PollConnector):
    def __init__(self, batch_size: int = INDEX_BATCH_SIZE) -> None:
        self.batch_size = batch_size

    def load_credentials(self, credentials: dict[str, str]) -> None:
        api_key = credentials.get("zammad_api_key")
        url = credentials.get("zammad_url")

        if not api_key or not url:
            raise ConnectorMissingCredentialError("zammad")

        self.api_key = str(api_key)
        self.url = str(url).rstrip("/")

    def _fetch_tickets(
        self, start: Optional[datetime] = None, end: Optional[datetime] = None
    ) -> Iterator[List[dict]]:
        if not self.api_key or not self.url:
            raise ConnectorMissingCredentialError("zammad")

        headers = {"Authorization": f"Token token={self.api_key}"}
        per_page = 50
        page = 1

        if start:
            # Use search to filter by updated_at
            since_date = start.date().isoformat()
            endpoint = f"{self.url}/api/v1/tickets/search"
            params = {
                "query": f"updated_at:>{since_date}",
                "expand": "true",
                "per_page": per_page,
                "page": page,
            }
        else:
            # List all tickets
            endpoint = f"{self.url}/api/v1/tickets"
            params = {"per_page": per_page, "page": page}

        while True:
            resp = requests.get(endpoint, headers=headers, params=params)
            resp.raise_for_status()

            tickets = resp.json()
            if not tickets:
                break

            logger.info(
                f"Fetched {len(tickets)} tickets from Zammad API (Page {params['page']})"
            )
            yield tickets

            if len(tickets) < per_page:
                break
            page += 1
            params["page"] = page

    def _process_tickets(
        self, start: Optional[datetime] = None, end: Optional[datetime] = None
    ) -> GenerateDocumentsOutput:
        docs: List[Document] = []

        for batch in self._fetch_tickets(start, end):
            for ticket in batch:
                doc = _create_doc_from_ticket(ticket, self.url, self.api_key)
                docs.append(doc)
                if len(docs) >= self.batch_size:
                    yield docs
                    docs = []

        if docs:
            yield docs

    def load_from_state(self) -> GenerateDocumentsOutput:
        return self._process_tickets()

    def poll_source(
        self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch
    ) -> GenerateDocumentsOutput:
        start_datetime = datetime.fromtimestamp(start, tz=timezone.utc)
        end_datetime = datetime.fromtimestamp(end, tz=timezone.utc)
        yield from self._process_tickets(start_datetime, end_datetime)
