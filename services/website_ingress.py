"""Credential-authenticated input into the existing canonical Website journal."""
from uuid import uuid4

from sqlalchemy import select, text

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import WebsiteAdmission
from schemas.contracts import ErrorCode, Principal
from schemas.website import WEBSITE_NAMESPACE, WebsiteAdmissionResult
from schemas.website import WebsiteAdmission as AdmissionRequest
from schemas.website_session import WebsiteSessionAdmission
from services import website, website_session
from services.common import fingerprint


async def admit(database: Database, principal: Principal, request: WebsiteSessionAdmission,
                settings: Settings) -> WebsiteAdmissionResult:
    # The raw visitor secret never enters fingerprints, journal rows or receipts.
    canonical = AdmissionRequest(**request.model_dump(exclude={"session_token",
        "binding_generation"}), desired_route="MANAGED")
    digest = fingerprint({"contract_version": 1, "channel": "WEBSITE_WIDGET",
        "provider_namespace": WEBSITE_NAMESPACE,
        **canonical.model_dump(mode="json", exclude={"desired_route"})})
    async with database.session_factory() as session, session.begin():
        root = await website_session.locked_access(session, principal, request, settings)
        key = fingerprint({"owner": request.owner_id, "integration": str(request.integration_id),
                           "namespace": WEBSITE_NAMESPACE, "message": str(request.message_id)})
        await session.execute(text("SELECT pg_advisory_xact_lock(:key)"),
                              {"key": int(key[:16], 16) - (1 << 63)})
        existing = await session.scalar(select(WebsiteAdmission).where(
            *website.identity_filter(canonical)))
        if existing is not None:
            if (existing.fingerprint != digest or existing.conversation_id != root.id
                    or existing.agent_id != request.agent_id
                    or existing.session_id != request.session_id or existing.route != "MANAGED"):
                raise DomainError(ErrorCode.EVENT_CONFLICT)
            result = WebsiteAdmissionResult(route="MANAGED", admission_id=existing.id,
                                            replayed=True)
        else:
            message_id = await website.append_canonical_message(session, root, canonical, settings,
                digest=digest, separate_authority_epoch=True)
            row = WebsiteAdmission(id=uuid4(), owner_id=request.owner_id,
                integration_id=request.integration_id, agent_id=request.agent_id,
                channel="WEBSITE_WIDGET", provider_namespace=WEBSITE_NAMESPACE,
                message_id=request.message_id, session_id=request.session_id,
                fingerprint=digest, route="MANAGED", conversation_id=root.id,
                canonical_message_id=message_id)
            session.add(row)
            await session.flush()
            result = WebsiteAdmissionResult(route="MANAGED", admission_id=row.id, replayed=False)
    return result
