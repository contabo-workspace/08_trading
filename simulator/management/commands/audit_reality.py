from __future__ import annotations

import json
from decimal import Decimal
from urllib.request import Request, urlopen

from django.core.management.base import BaseCommand

from simulator.models import Market, MarketPriceTick, WorldSignal


def _extract_api_yes_price(row: dict) -> Decimal | None:
    direct_keys = ["yesPrice", "lastTradePrice", "price", "currentPrice"]
    for key in direct_keys:
        raw = row.get(key)
        if raw is None:
            continue
        try:
            return Decimal(str(raw))
        except Exception:
            continue

    outcomes = row.get("outcomes")
    outcome_prices = row.get("outcomePrices")

    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            outcomes = None
    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            outcome_prices = None

    if isinstance(outcome_prices, list) and outcome_prices:
        yes_index = 0
        if isinstance(outcomes, list):
            for idx, label in enumerate(outcomes):
                if str(label).strip().lower() == "yes":
                    yes_index = idx
                    break
        if yes_index < len(outcome_prices):
            try:
                return Decimal(str(outcome_prices[yes_index]))
            except Exception:
                return None

    return None


class Command(BaseCommand):
    help = "Audit that simulator uses real Polymarket data (only money is simulated)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--price-tolerance",
            type=str,
            default="0.08",
            help="Allowed absolute diff between DB price and live API yesPrice for sample market.",
        )

    def handle(self, *args, **options):
        tolerance = Decimal(options["price_tolerance"])

        total_markets = Market.objects.count()
        polymarket_markets = Market.objects.filter(source=Market.SOURCE_POLYMARKET).count()
        synthetic_markets = Market.objects.filter(source=Market.SOURCE_SYNTHETIC).count()
        world_signals = WorldSignal.objects.count()
        ticks = MarketPriceTick.objects.count()

        checks: list[tuple[str, bool, str]] = []

        checks.append(
            (
                "No synthetic markets",
                synthetic_markets == 0,
                f"synthetic_markets={synthetic_markets}",
            )
        )
        checks.append(
            (
                "No world/news signals",
                world_signals == 0,
                f"world_signals={world_signals}",
            )
        )
        checks.append(
            (
                "All markets are Polymarket",
                total_markets > 0 and total_markets == polymarket_markets,
                f"polymarket={polymarket_markets}, total={total_markets}",
            )
        )
        checks.append(
            (
                "Tick history exists",
                ticks > 0,
                f"market_price_ticks={ticks}",
            )
        )

        live_check_ok = False
        live_check_detail = "no overlap between API markets and local DB"

        try:
            req = Request(
                "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=500",
                headers={"User-Agent": "08-trading-audit/1.0"},
            )
            with urlopen(req, timeout=15) as response:
                payload = json.loads(response.read().decode("utf-8"))

            rows = payload.get("data") if isinstance(payload, dict) else payload
            if isinstance(rows, list):
                overlap_row = None
                overlap_market = None
                for row in rows:
                    external_id = str(row.get("id") or row.get("marketId") or "")
                    if not external_id:
                        continue
                    market = Market.objects.filter(source=Market.SOURCE_POLYMARKET, external_id=external_id).first()
                    if market:
                        overlap_row = row
                        overlap_market = market
                        break

                if overlap_row is not None and overlap_market is not None:
                    api_yes = _extract_api_yes_price(overlap_row)
                else:
                    api_yes = None

                if overlap_row is not None and overlap_market is not None and api_yes is not None:
                    db_yes = Decimal(overlap_market.last_price_yes)
                    diff = abs(db_yes - api_yes)
                    live_check_ok = diff <= tolerance
                    live_check_detail = (
                        f"market={overlap_market.symbol}, db_yes={db_yes}, api_yes={api_yes}, "
                        f"abs_diff={diff}, tolerance={tolerance}"
                    )
                elif overlap_row is not None and overlap_market is not None:
                    live_check_detail = "overlap market found but API yesPrice missing"
        except Exception as exc:
            live_check_detail = f"api_error={exc}"

        checks.append(("Live API price consistency (sample)", live_check_ok, live_check_detail))

        self.stdout.write("")
        self.stdout.write("REAL-DATA AUDIT")
        self.stdout.write("=" * 60)
        passed = 0

        for title, ok, detail in checks:
            status = "PASS" if ok else "FAIL"
            if ok:
                passed += 1
            self.stdout.write(f"[{status}] {title} | {detail}")

        self.stdout.write("-" * 60)
        self.stdout.write(f"Summary: {passed}/{len(checks)} checks passed")

        if passed == len(checks):
            self.stdout.write("Conclusion: Runtime is consistent with strict real-data mode.")
        else:
            self.stdout.write("Conclusion: Some checks failed. See FAIL lines above.")
