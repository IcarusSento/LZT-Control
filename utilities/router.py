from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .resale_finder import ResaleFinderService
from .steam_kt_manager import SteamKTManagerService
from .deal_finder import DealFinderService
from .mass_claims import MassClaimsService


class ResaleFinderStart(BaseModel):
    orders_url: str = ""
    page_from: int = Field(default=1, ge=1, le=100_000)
    page_to: int = Field(default=3, ge=1, le=100_000)
    token: str = Field(default="", max_length=8192)
    token_id: int | None = Field(default=None, ge=1)


class UtilityTokenPayload(BaseModel):
    token: str = Field(default="", max_length=8192)
    token_id: int | None = Field(default=None, ge=1)


class ResaleFinderTags(BaseModel):
    # ``selection`` is retained for compatibility with the first UI version.
    selection: str | None = None
    selections: list[str] = Field(default_factory=list, max_length=16)
    tag_ids: list[int] = Field(default_factory=list, max_length=100)
    token: str = Field(default="", max_length=8192)
    token_id: int | None = Field(default=None, ge=1)


class SteamKTStart(BaseModel):
    text: str = Field(default="", max_length=2_000_000)


class SteamKTAddTag(BaseModel):
    item_ids: list[int] = Field(default_factory=list, max_length=5_000)
    tag_id: int = Field(ge=1)
    token: str = Field(default="", max_length=8192)
    token_id: int | None = Field(default=None, ge=1)


class DealFinderStart(BaseModel):
    source_url: str = Field(default="", max_length=8192)
    page_from: int = Field(default=1, ge=1, le=100_000)
    page_to: int = Field(default=3, ge=1, le=100_000)
    mode: str = Field(default="reseller", max_length=32)
    token: str = Field(default="", max_length=8192)
    token_id: int | None = Field(default=None, ge=1)


class MassClaimsPrepare(BaseModel):
    text: str = Field(default="", max_length=2_000_000)
    description: str = Field(default="", max_length=4_000)
    use_previous_id: bool = True
    interval_seconds: int = Field(default=63, ge=60, le=3_600)
    token: str = Field(default="", max_length=8192)
    token_id: int | None = Field(default=None, ge=1)


class UtilitiesServiceGroup:
    def __init__(
        self,
        resale: ResaleFinderService,
        steam_kt: SteamKTManagerService,
        deals: DealFinderService,
        mass_claims: MassClaimsService,
    ) -> None:
        self.resale = resale
        self.steam_kt = steam_kt
        self.deals = deals
        self.mass_claims = mass_claims

    def stop(self) -> None:
        self.resale.stop()
        self.steam_kt.stop()
        self.deals.stop()
        self.mass_claims.stop()


def create_utilities_router(
    config_loader: Callable[[], dict[str, Any]],
    state_path: Path,
    token_loader: Callable[[int], dict[str, Any] | None] | None = None,
) -> tuple[APIRouter, UtilitiesServiceGroup]:
    router = APIRouter(prefix="/api/utilities", tags=["Utilities"])
    service = ResaleFinderService(config_loader, state_path)
    steam_kt = SteamKTManagerService(config_loader)
    deals = DealFinderService(config_loader)
    mass_claims = MassClaimsService(config_loader)

    def resolve_token(manual_token: str, token_id: int | None) -> str:
        token = str(manual_token or "").strip()
        if token:
            return token
        if token_id is None:
            return ""
        account = token_loader(token_id) if token_loader else None
        if not account:
            raise ValueError("Выбранный аккаунт из менеджера токенов не найден")
        token = str(account.get("api_token") or "").strip()
        if not token:
            raise ValueError("У выбранного аккаунта не задан API-токен")
        return token

    @router.get("/resale-finder/defaults")
    def defaults() -> dict[str, Any]:
        try:
            return service.defaults()
        except Exception as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.get("/resale-finder/status")
    def status() -> dict[str, Any]:
        return service.status()

    @router.get("/resale-finder/results")
    def results() -> dict[str, Any]:
        return service.results()

    @router.post("/resale-finder/start")
    def start(payload: ResaleFinderStart) -> dict[str, Any]:
        try:
            return service.start(
                payload.orders_url,
                payload.page_from,
                payload.page_to,
                resolve_token(payload.token, payload.token_id),
            )
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/resale-finder/cancel")
    def cancel() -> dict[str, Any]:
        return service.cancel()

    @router.post("/resale-finder/clear")
    def clear_session() -> dict[str, Any]:
        try:
            return service.clear_session()
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc

    @router.post("/resale-finder/statistics/start")
    def start_statistics(payload: UtilityTokenPayload) -> dict[str, Any]:
        try:
            return service.start_statistics(resolve_token(payload.token, payload.token_id))
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/resale-finder/statistics/cancel")
    def cancel_statistics() -> dict[str, Any]:
        return service.cancel_statistics()

    @router.post("/resale-finder/tags/start")
    def start_tags(payload: ResaleFinderTags) -> dict[str, Any]:
        try:
            selections = payload.selections or ([payload.selection] if payload.selection else [])
            return service.start_tags(
                selections,
                payload.tag_ids,
                resolve_token(payload.token, payload.token_id),
            )
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
        except (TypeError, ValueError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/resale-finder/tags/cancel")
    def cancel_tags() -> dict[str, Any]:
        return service.cancel_tags()

    @router.get("/steam-kt/status")
    def steam_kt_status() -> dict[str, Any]:
        return steam_kt.status()

    @router.get("/steam-kt/results")
    def steam_kt_results() -> dict[str, Any]:
        return steam_kt.results()

    @router.post("/steam-kt/start")
    def steam_kt_start(payload: SteamKTStart) -> dict[str, Any]:
        try:
            return steam_kt.start(payload.text)
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/steam-kt/cancel")
    def steam_kt_cancel() -> dict[str, Any]:
        return steam_kt.cancel()

    @router.post("/steam-kt/sellers/start")
    def steam_kt_sellers_start(payload: UtilityTokenPayload) -> dict[str, Any]:
        try:
            return steam_kt.start_sellers(resolve_token(payload.token, payload.token_id))
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/steam-kt/sellers/cancel")
    def steam_kt_sellers_cancel() -> dict[str, Any]:
        return steam_kt.cancel_sellers()

    @router.post("/steam-kt/tags/add")
    def steam_kt_add_tag(payload: SteamKTAddTag) -> dict[str, Any]:
        try:
            return steam_kt.add_tag(
                resolve_token(payload.token, payload.token_id),
                payload.item_ids,
                payload.tag_id,
            )
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
        except (TypeError, ValueError) as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/steam-kt/clear")
    def steam_kt_clear() -> dict[str, Any]:
        try:
            return steam_kt.clear()
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc

    @router.get("/deal-finder/status")
    def deal_finder_status() -> dict[str, Any]:
        return deals.status()

    @router.get("/deal-finder/results")
    def deal_finder_results() -> dict[str, Any]:
        return deals.results()

    @router.post("/deal-finder/start")
    def deal_finder_start(payload: DealFinderStart) -> dict[str, Any]:
        try:
            return deals.start(
                resolve_token(payload.token, payload.token_id),
                payload.source_url,
                payload.page_from,
                payload.page_to,
                payload.mode,
            )
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/deal-finder/cancel")
    def deal_finder_cancel() -> dict[str, Any]:
        return deals.cancel()

    @router.post("/deal-finder/clear")
    def deal_finder_clear() -> dict[str, Any]:
        try:
            return deals.clear()
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc

    @router.get("/mass-claims/status")
    def mass_claims_status() -> dict[str, Any]:
        return mass_claims.status()

    @router.get("/mass-claims/results")
    def mass_claims_results() -> dict[str, Any]:
        return mass_claims.results()

    @router.post("/mass-claims/prepare")
    def mass_claims_prepare(payload: MassClaimsPrepare) -> dict[str, Any]:
        try:
            return mass_claims.prepare(
                resolve_token(payload.token, payload.token_id),
                payload.text,
                payload.description,
                payload.use_previous_id,
                payload.interval_seconds,
            )
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/mass-claims/create")
    def mass_claims_create(payload: UtilityTokenPayload) -> dict[str, Any]:
        try:
            return mass_claims.create(resolve_token(payload.token, payload.token_id), retry_errors=False)
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/mass-claims/retry")
    def mass_claims_retry(payload: UtilityTokenPayload) -> dict[str, Any]:
        try:
            return mass_claims.create(resolve_token(payload.token, payload.token_id), retry_errors=True)
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @router.post("/mass-claims/cancel")
    def mass_claims_cancel() -> dict[str, Any]:
        return mass_claims.cancel()

    @router.post("/mass-claims/clear")
    def mass_claims_clear() -> dict[str, Any]:
        try:
            return mass_claims.clear()
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc

    return router, UtilitiesServiceGroup(service, steam_kt, deals, mass_claims)
