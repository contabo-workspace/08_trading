from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Iterable
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from django.db.models import Sum
from django.utils.dateparse import parse_datetime
from django.utils import timezone

from .models import (
    Market,
    MarketPriceTick,
    MarketPrediction,
    PerformanceSnapshot,
    Position,
    SimulationAccount,
    SimulationTrade,
    WorldSignal,
)


DECIMAL_ZERO = Decimal('0.00')
DECIMAL_ONE = Decimal('1.0000')
DECIMAL_HALF = Decimal('0.5000')
DEFAULT_EDGE_THRESHOLD = Decimal('0.01')

# Public feeds are available without API keys. Paid API/social providers can be enabled via env vars.
RSS_SOURCES = {
    'Reuters World': 'https://feeds.reuters.com/Reuters/worldNews',
    'BBC World': 'http://feeds.bbci.co.uk/news/world/rss.xml',
    'NYTimes World': 'https://rss.nytimes.com/services/xml/rss/nyt/World.xml',
}

SOCIAL_RSS_SOURCES = {
    'Reddit WorldNews': 'https://www.reddit.com/r/worldnews/.rss',
    'Reddit Geopolitics': 'https://www.reddit.com/r/geopolitics/.rss',
}

POLYMARKET_MARKETS_URL = os.getenv(
    'POLYMARKET_MARKETS_URL',
    'https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=200',
)

POSITIVE_WORDS = {
    'deal',
    'growth',
    'success',
    'win',
    'bullish',
    'upside',
    'progress',
    'agreement',
    'peace',
    'record',
}

NEGATIVE_WORDS = {
    'war',
    'crash',
    'recession',
    'loss',
    'lawsuit',
    'bearish',
    'default',
    'attack',
    'risk',
    'sanction',
}

STOP_WORDS = {
    'the', 'a', 'an', 'and', 'or', 'of', 'to', 'for', 'in', 'on', 'at', 'is', 'are', 'will', 'be', 'by', 'with'
}


@dataclass
class SourceItem:
    source: str
    source_name: str
    headline: str
    url: str
    published_at: timezone.datetime


def _safe_decimal(value: Decimal, places: str = '0.0001') -> Decimal:
    return Decimal(value).quantize(Decimal(places))


def _parse_decimal(value: object, default: Decimal = DECIMAL_ZERO) -> Decimal:
    if value is None:
        return default
    try:
        if isinstance(value, bool):
            return default
        return Decimal(str(value).strip())
    except Exception:
        return default


def _bounded_price(value: Decimal) -> Decimal:
    return max(Decimal('0.01'), min(Decimal('0.99'), value))


def _tokenize(text: str) -> set[str]:
    tokens = {tok for tok in re.findall(r'[a-zA-Z]{3,}', text.lower()) if tok not in STOP_WORDS}
    return tokens


def _headline_sentiment(headline: str) -> Decimal:
    words = {w.strip('.,:;!?()[]{}\"\'').lower() for w in headline.split()}
    positives = len(words & POSITIVE_WORDS)
    negatives = len(words & NEGATIVE_WORDS)
    score = positives - negatives
    # Clamp to a bounded range so extreme headlines do not dominate.
    return Decimal(max(min(score, 5), -5)) / Decimal('5')


def _infer_impact(headline: str) -> Decimal:
    char_count = len(headline)
    if char_count > 140:
        return Decimal('1.50')
    if char_count > 90:
        return Decimal('1.20')
    return Decimal('1.00')


def _make_market_symbol(headline: str) -> str:
    base = '-'.join(headline.lower().split()[:6])
    digest = hashlib.sha1(headline.encode('utf-8')).hexdigest()[:8]
    compact = ''.join(ch for ch in base if ch.isalnum() or ch == '-')
    return f"mk-{compact[:42]}-{digest}"


def _extract_yes_price(item: dict) -> Decimal:
    direct_keys = ['yesPrice', 'lastTradePrice', 'price', 'currentPrice']
    for key in direct_keys:
        value = _parse_decimal(item.get(key), default=Decimal('-1'))
        if value >= 0:
            return _bounded_price(value)

    outcomes = item.get('outcomes')
    outcome_prices = item.get('outcomePrices')

    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except Exception:
            outcome_prices = None
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            outcomes = None

    if isinstance(outcome_prices, list) and outcome_prices:
        yes_index = 0
        if isinstance(outcomes, list):
            for idx, outcome in enumerate(outcomes):
                if str(outcome).strip().lower() == 'yes':
                    yes_index = idx
                    break
        if yes_index < len(outcome_prices):
            parsed = _parse_decimal(outcome_prices[yes_index], default=DECIMAL_HALF)
            return _bounded_price(parsed)

    return DECIMAL_HALF


def _extract_market_close(item: dict):
    for key in ['endDate', 'end_date_iso', 'closeTime', 'closedTime', 'endTime']:
        raw = item.get(key)
        if not raw:
            continue
        parsed = parse_datetime(str(raw))
        if parsed is None:
            continue
        if timezone.is_naive(parsed):
            parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
        return parsed
    return None


def sync_polymarket_markets(limit: int = 200) -> int:
    headers = {'User-Agent': '08-trading-simulator/1.0'}
    created_or_updated = 0
    try:
        req = Request(POLYMARKET_MARKETS_URL, headers=headers)
        with urlopen(req, timeout=12) as response:
            payload = json.loads(response.read().decode('utf-8'))
    except Exception:
        return 0

    markets = payload.get('data') if isinstance(payload, dict) else payload
    if not isinstance(markets, list):
        return 0

    for item in markets[:limit]:
        market_id = str(item.get('id') or item.get('marketId') or '').strip()
        question = (item.get('question') or item.get('title') or '').strip()
        if not market_id or not question:
            continue

        yes_price = _extract_yes_price(item)
        spread_default = Decimal('0.02')
        yes_bid = _bounded_price(_parse_decimal(item.get('yesBid'), default=yes_price - spread_default / Decimal('2')))
        yes_ask = _bounded_price(_parse_decimal(item.get('yesAsk'), default=yes_price + spread_default / Decimal('2')))
        no_bid = _bounded_price(_parse_decimal(item.get('noBid'), default=(DECIMAL_ONE - yes_price) - spread_default / Decimal('2')))
        no_ask = _bounded_price(_parse_decimal(item.get('noAsk'), default=(DECIMAL_ONE - yes_price) + spread_default / Decimal('2')))

        symbol = f'poly-{market_id}'
        defaults = {
            'external_id': market_id,
            'source': Market.SOURCE_POLYMARKET,
            'name': question[:255],
            'description': (item.get('description') or '')[:3000],
            'last_price_yes': _safe_decimal(yes_price),
            'yes_bid': _safe_decimal(yes_bid),
            'yes_ask': _safe_decimal(yes_ask),
            'no_bid': _safe_decimal(no_bid),
            'no_ask': _safe_decimal(no_ask),
            'liquidity_usd': _safe_decimal(_parse_decimal(item.get('liquidity') or item.get('liquidityNum')), '0.01'),
            'volume_24h_usd': _safe_decimal(_parse_decimal(item.get('volume24hr') or item.get('volume24h')), '0.01'),
            'market_close_at': _extract_market_close(item),
            'is_active': bool(item.get('active', True)) and not bool(item.get('closed', False)),
        }
        market, created = Market.objects.update_or_create(symbol=symbol, defaults=defaults)
        if created:
            created_or_updated += 1
        else:
            created_or_updated += 1

    return created_or_updated


def _fetch_rss_items(limit_per_source: int = 8) -> list[SourceItem]:
    items: list[SourceItem] = []
    items.extend(_fetch_feed_items(RSS_SOURCES, WorldSignal.SOURCE_RSS, limit_per_source))
    return items


def _fetch_feed_items(feed_map: dict[str, str], source: str, limit_per_source: int) -> list[SourceItem]:
    items: list[SourceItem] = []
    headers = {'User-Agent': '08-trading-simulator/1.0'}
    for source_name, feed_url in feed_map.items():
        try:
            req = Request(feed_url, headers=headers)
            with urlopen(req, timeout=10) as response:
                xml_data = response.read()
            root = ET.fromstring(xml_data)
            channel_items = root.findall('.//item')
            if not channel_items:
                channel_items = root.findall('.//{http://www.w3.org/2005/Atom}entry')
            channel_items = channel_items[:limit_per_source]
            for node in channel_items:
                title = (node.findtext('title') or node.findtext('{http://www.w3.org/2005/Atom}title') or '').strip()
                link = (node.findtext('link') or '').strip()
                if not link:
                    atom_link = node.find('{http://www.w3.org/2005/Atom}link')
                    if atom_link is not None:
                        link = (atom_link.attrib.get('href') or '').strip()
                if not title or not link:
                    continue
                pub = timezone.now()
                items.append(
                    SourceItem(
                        source=source,
                        source_name=source_name,
                        headline=title,
                        url=link,
                        published_at=pub,
                    )
                )
        except Exception:
            continue
    return items


def _fetch_newsapi_items() -> list[SourceItem]:
    api_key = os.getenv('NEWSAPI_KEY', '').strip()
    if not api_key:
        return []
    url = 'https://newsapi.org/v2/top-headlines?language=en&pageSize=20'
    headers = {'X-Api-Key': api_key, 'User-Agent': '08-trading-simulator/1.0'}
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=10) as response:
            import json

            payload = json.loads(response.read().decode('utf-8'))
        out: list[SourceItem] = []
        for article in payload.get('articles', []):
            title = (article.get('title') or '').strip()
            link = (article.get('url') or '').strip()
            if not title or not link:
                continue
            out.append(
                SourceItem(
                    source=WorldSignal.SOURCE_NEWS_API,
                    source_name='NewsAPI',
                    headline=title,
                    url=link,
                    published_at=timezone.now(),
                )
            )
        return out
    except Exception:
        return []


def _fetch_social_items() -> list[SourceItem]:
    items = _fetch_feed_items(SOCIAL_RSS_SOURCES, WorldSignal.SOURCE_SOCIAL, limit_per_source=10)
    # Placeholders for authenticated providers (X/Reddit API) can be plugged in via env keys.
    if os.getenv('X_BEARER_TOKEN'):
        return items
    return items


def ingest_world_signals() -> int:
    source_items = []
    source_items.extend(_fetch_rss_items())
    source_items.extend(_fetch_newsapi_items())
    source_items.extend(_fetch_social_items())

    created = 0
    for item in source_items:
        sentiment = _headline_sentiment(item.headline)
        impact = _infer_impact(item.headline)
        obj, was_created = WorldSignal.objects.get_or_create(
            url=item.url,
            defaults={
                'source': item.source,
                'source_name': item.source_name,
                'headline': item.headline,
                'published_at': item.published_at,
                'sentiment_score': _safe_decimal(sentiment, '0.01'),
                'impact_score': _safe_decimal(impact, '0.01'),
            },
        )
        if was_created:
            created += 1
            Market.objects.get_or_create(
                symbol=_make_market_symbol(item.headline),
                defaults={
                    'source': Market.SOURCE_SYNTHETIC,
                    'name': item.headline[:250],
                    'description': f'Auto-created from signal: {item.source_name}',
                    'last_price_yes': Decimal('0.5000'),
                },
            )
        else:
            obj.sentiment_score = _safe_decimal(sentiment, '0.01')
            obj.impact_score = _safe_decimal(impact, '0.01')
            obj.save(update_fields=['sentiment_score', 'impact_score'])
    return created


def _weighted_signal_score(signals: Iterable[WorldSignal]) -> Decimal:
    weighted_sum = Decimal('0')
    weight_sum = Decimal('0')
    for signal in signals:
        weight = Decimal(signal.impact_score)
        weighted_sum += Decimal(signal.sentiment_score) * weight
        weight_sum += weight
    if weight_sum == 0:
        return Decimal('0')
    return weighted_sum / weight_sum


def _market_relevance_score(market_name: str, headline: str) -> Decimal:
    market_tokens = _tokenize(market_name)
    headline_tokens = _tokenize(headline)
    if not market_tokens or not headline_tokens:
        return Decimal('0')
    overlap = len(market_tokens & headline_tokens)
    if overlap == 0:
        return Decimal('0')
    return Decimal(overlap) / Decimal(max(1, min(len(market_tokens), 8)))


def _market_implied_fallback_score(market: Market) -> Decimal:
    price = Decimal(market.last_price_yes or DECIMAL_HALF)
    distance_from_mid = DECIMAL_HALF - price
    liquidity = max(Decimal('1.00'), Decimal(market.liquidity_usd or DECIMAL_ZERO))
    spread = max(DECIMAL_ZERO, _market_spread_for_side(market, Position.SIDE_YES))

    liquidity_factor = min(Decimal('1.25'), Decimal('0.65') + (liquidity / Decimal('50000')))
    spread_penalty = max(Decimal('0.35'), Decimal('1.00') - (spread / Decimal('0.08')))

    return distance_from_mid * Decimal('0.45') * liquidity_factor * spread_penalty


def generate_predictions(window_hours: int = 24) -> int:
    since = timezone.now() - timedelta(hours=window_hours)
    recent_signals = list(WorldSignal.objects.filter(published_at__gte=since)[:400])
    global_score = _weighted_signal_score(recent_signals) if recent_signals else Decimal('0')

    created = 0
    markets = Market.objects.filter(is_active=True, source=Market.SOURCE_POLYMARKET).order_by('-liquidity_usd')[:120]
    for market in markets:
        weighted = Decimal('0')
        weight_sum = Decimal('0')
        relevant_count = 0
        for signal in recent_signals:
            relevance = _market_relevance_score(market.name, signal.headline)
            if relevance <= 0:
                continue
            relevant_count += 1
            signal_weight = relevance * Decimal(signal.impact_score)
            weighted += Decimal(signal.sentiment_score) * signal_weight
            weight_sum += signal_weight

        local_signal_multiplier = Decimal(os.getenv('SIMULATOR_LOCAL_SIGNAL_MULTIPLIER', '0.14'))
        global_signal_multiplier = Decimal(os.getenv('SIMULATOR_GLOBAL_SIGNAL_MULTIPLIER', '0.08'))
        fallback_score = _market_implied_fallback_score(market)

        if weight_sum > 0:
            local_score = weighted / weight_sum
            confidence = min(Decimal('92.00'), Decimal('42.00') + Decimal(relevant_count * 2))
            reasoning = f'Relevance-weighted sentiment from {relevant_count} matching world signals.'
            price_shift = (local_score * local_signal_multiplier) + (fallback_score * Decimal('0.35'))
        else:
            local_score = (global_score * Decimal('0.35')) + fallback_score
            confidence = Decimal('35.00') + min(Decimal('18.00'), abs(fallback_score) * Decimal('120'))
            reasoning = 'No strong direct signal match; fallback to macro sentiment and market-implied mean reversion.'
            price_shift = (global_score * global_signal_multiplier) + (fallback_score * Decimal('0.12'))

        prob_yes = _bounded_price(Decimal(market.last_price_yes) + price_shift)

        prediction = MarketPrediction.objects.create(
            market=market,
            probability_yes=_safe_decimal(prob_yes),
            confidence=_safe_decimal(confidence, '0.01'),
            reasoning=reasoning,
        )
        prediction.signals.set(recent_signals[:40])
        created += 1
    return created


def _price_for_side(market: Market, side: str, action: str) -> Decimal:
    yes_bid = Decimal(market.yes_bid if market.yes_bid is not None else max(Decimal('0.01'), Decimal(market.last_price_yes) - Decimal('0.01')))
    yes_ask = Decimal(market.yes_ask if market.yes_ask is not None else min(Decimal('0.99'), Decimal(market.last_price_yes) + Decimal('0.01')))
    no_bid = Decimal(market.no_bid if market.no_bid is not None else max(Decimal('0.01'), (DECIMAL_ONE - Decimal(market.last_price_yes)) - Decimal('0.01')))
    no_ask = Decimal(market.no_ask if market.no_ask is not None else min(Decimal('0.99'), (DECIMAL_ONE - Decimal(market.last_price_yes)) + Decimal('0.01')))

    if side == Position.SIDE_YES:
        return yes_ask if action == SimulationTrade.ACTION_BUY else yes_bid
    return no_ask if action == SimulationTrade.ACTION_BUY else no_bid


def _apply_slippage(base_price: Decimal, market: Market, size_usd: Decimal, action: str) -> Decimal:
    liquidity = max(Decimal('1.00'), Decimal(market.liquidity_usd or DECIMAL_ZERO))
    impact = min(Decimal('0.03'), max(Decimal('0.001'), (size_usd / liquidity) * Decimal('0.05')))
    if action == SimulationTrade.ACTION_BUY:
        return _bounded_price(base_price + impact)
    return _bounded_price(base_price - impact)


def _market_spread_for_side(market: Market, side: str) -> Decimal:
    if side == Position.SIDE_YES:
        bid = Decimal(market.yes_bid if market.yes_bid is not None else Decimal(market.last_price_yes) - Decimal('0.01'))
        ask = Decimal(market.yes_ask if market.yes_ask is not None else Decimal(market.last_price_yes) + Decimal('0.01'))
    else:
        bid = Decimal(market.no_bid if market.no_bid is not None else (DECIMAL_ONE - Decimal(market.last_price_yes)) - Decimal('0.01'))
        ask = Decimal(market.no_ask if market.no_ask is not None else (DECIMAL_ONE - Decimal(market.last_price_yes)) + Decimal('0.01'))
    return max(DECIMAL_ZERO, ask - bid)


def _mark_price_for_position(position: Position) -> Decimal:
    market = position.market
    if position.side == Position.SIDE_YES:
        bid = Decimal(market.yes_bid if market.yes_bid is not None else Decimal(market.last_price_yes) - Decimal('0.01'))
        ask = Decimal(market.yes_ask if market.yes_ask is not None else Decimal(market.last_price_yes) + Decimal('0.01'))
    else:
        bid = Decimal(market.no_bid if market.no_bid is not None else (DECIMAL_ONE - Decimal(market.last_price_yes)) - Decimal('0.01'))
        ask = Decimal(market.no_ask if market.no_ask is not None else (DECIMAL_ONE - Decimal(market.last_price_yes)) + Decimal('0.01'))
    return _bounded_price((bid + ask) / Decimal('2'))


def _open_position(account: SimulationAccount, market: Market, side: str, size_usd: Decimal, expected_edge: Decimal) -> Position | None:
    base_price = _price_for_side(market, side, SimulationTrade.ACTION_BUY)
    execution_price = _apply_slippage(base_price, market, size_usd, SimulationTrade.ACTION_BUY)
    if execution_price <= Decimal('0.0001'):
        return None

    fee_open = _safe_decimal(size_usd * Decimal(account.fee_rate), '0.01')
    total_debit = size_usd + fee_open
    if account.balance_cash < total_debit:
        return None

    qty = _safe_decimal(size_usd / execution_price, '0.000001')
    account.balance_cash = _safe_decimal(Decimal(account.balance_cash) - total_debit, '0.01')
    account.balance_reserved = _safe_decimal(Decimal(account.balance_reserved) + size_usd, '0.01')
    account.save(update_fields=['balance_cash', 'balance_reserved'])

    position = Position.objects.create(
        account=account,
        market=market,
        side=side,
        entry_prob=_safe_decimal(execution_price),
        size_usd=size_usd,
        quantity_shares=qty,
        fee_open=fee_open,
    )
    SimulationTrade.objects.create(
        account=account,
        market=market,
        position=position,
        action=SimulationTrade.ACTION_BUY,
        side=side,
        probability=_safe_decimal(execution_price),
        size_usd=size_usd,
        fee_usd=fee_open,
        expected_edge=_safe_decimal(expected_edge),
        note='Open simulated position (spread+slippage applied)',
    )
    return position


def _close_position(position: Position, close_prob: Decimal, note: str) -> None:
    account = position.account
    market = position.market
    side = position.side

    base_price = _price_for_side(market, side, SimulationTrade.ACTION_SELL)
    close_price = _apply_slippage(base_price, market, Decimal(position.size_usd), SimulationTrade.ACTION_SELL)
    proceeds = _safe_decimal(Decimal(position.quantity_shares) * close_price, '0.01')
    fee_close = _safe_decimal(proceeds * Decimal(account.fee_rate), '0.01')
    net = proceeds - fee_close

    pnl = _safe_decimal(net - Decimal(position.size_usd) - Decimal(position.fee_open), '0.01')

    account.balance_cash = _safe_decimal(Decimal(account.balance_cash) + net, '0.01')
    account.balance_reserved = _safe_decimal(max(DECIMAL_ZERO, Decimal(account.balance_reserved) - Decimal(position.size_usd)), '0.01')
    account.save(update_fields=['balance_cash', 'balance_reserved'])

    position.status = Position.STATUS_CLOSED
    position.close_prob = _safe_decimal(close_prob)
    position.closed_at = timezone.now()
    position.fee_close = fee_close
    position.pnl_usd = pnl
    position.save(update_fields=['status', 'close_prob', 'closed_at', 'fee_close', 'pnl_usd'])

    SimulationTrade.objects.create(
        account=account,
        market=market,
        position=position,
        action=SimulationTrade.ACTION_SELL,
        side=side,
        probability=_safe_decimal(close_price),
        size_usd=proceeds,
        fee_usd=fee_close,
        expected_edge=Decimal('0.0000'),
        note=note,
    )


def execute_trading_cycle(account: SimulationAccount, threshold: Decimal = DEFAULT_EDGE_THRESHOLD) -> dict[str, int]:
    opens = 0
    closes = 0
    min_liquidity = Decimal(os.getenv('SIMULATOR_MIN_LIQUIDITY_USD', '5000'))
    max_spread = Decimal(os.getenv('SIMULATOR_MAX_SPREAD', '0.0450'))
    max_reserved_pct = Decimal(os.getenv('SIMULATOR_MAX_RESERVED_PCT', '0.45'))
    max_hold_minutes = int(os.getenv('SIMULATOR_MAX_HOLD_MINUTES', '120'))
    stop_loss_pct = Decimal(os.getenv('SIMULATOR_STOP_LOSS_PCT', '0.35'))

    active_predictions = list(
        MarketPrediction.objects.select_related('market')
        .order_by('market_id', '-created_at')
    )

    latest_for_market: dict[int, MarketPrediction] = {}
    for pred in active_predictions:
        if pred.market_id not in latest_for_market:
            latest_for_market[pred.market_id] = pred

    for market_id, pred in latest_for_market.items():
        market = pred.market
        if market.source != Market.SOURCE_POLYMARKET or Decimal(market.liquidity_usd) < min_liquidity:
            continue

        edge = Decimal(pred.probability_yes) - Decimal(market.last_price_yes)
        open_position = Position.objects.filter(
            account=account,
            market_id=market_id,
            status=Position.STATUS_OPEN,
        ).first()

        if open_position:
            age_minutes = (timezone.now() - open_position.opened_at).total_seconds() / 60
            mark_price = _mark_price_for_position(open_position)
            mark_value = Decimal(open_position.quantity_shares) * mark_price
            unrealized = mark_value - Decimal(open_position.size_usd) - Decimal(open_position.fee_open)
            loss_limit = Decimal(open_position.size_usd) * stop_loss_pct * Decimal('-1')

            should_close = False
            close_note = 'Signal weakened or reversed'
            if (open_position.side == Position.SIDE_YES and edge < Decimal('0.005')) or (
                open_position.side == Position.SIDE_NO and edge > Decimal('-0.005')
            ):
                should_close = True
                close_note = 'Signal weakened or reversed'
            elif age_minutes >= max_hold_minutes:
                should_close = True
                close_note = 'Time stop: position held too long'
            elif unrealized <= loss_limit:
                should_close = True
                close_note = 'Stop loss reached'

            if should_close:
                _close_position(
                    open_position,
                    Decimal(pred.probability_yes),
                    note=close_note,
                )
                closes += 1
            continue

        if abs(edge) < threshold:
            continue

        position_size = min(
            Decimal(account.position_limit),
            _safe_decimal(Decimal(account.balance_cash) * Decimal('0.10'), '0.01'),
        )
        if position_size < Decimal('20.00'):
            continue

        # Avoid freezing the portfolio by keeping a minimum free-cash buffer.
        starting = max(Decimal('1.00'), Decimal(account.starting_balance))
        reserved_ratio = Decimal(account.balance_reserved) / starting
        if reserved_ratio >= max_reserved_pct:
            continue

        side = Position.SIDE_YES if edge > 0 else Position.SIDE_NO
        if _market_spread_for_side(market, side) > max_spread:
            continue
        if _open_position(account, market, side, position_size, edge):
            opens += 1

    _snapshot_account(account)
    return {'opened': opens, 'closed': closes}


def _snapshot_account(account: SimulationAccount) -> None:
    open_positions = Position.objects.filter(account=account, status=Position.STATUS_OPEN).select_related('market')
    open_pnl = Decimal('0')

    for position in open_positions:
        market = position.market
        if position.side == Position.SIDE_YES:
            bid = Decimal(market.yes_bid if market.yes_bid is not None else Decimal(market.last_price_yes) - Decimal('0.01'))
            ask = Decimal(market.yes_ask if market.yes_ask is not None else Decimal(market.last_price_yes) + Decimal('0.01'))
        else:
            bid = Decimal(market.no_bid if market.no_bid is not None else (DECIMAL_ONE - Decimal(market.last_price_yes)) - Decimal('0.01'))
            ask = Decimal(market.no_ask if market.no_ask is not None else (DECIMAL_ONE - Decimal(market.last_price_yes)) + Decimal('0.01'))
        mark_price = _bounded_price((bid + ask) / Decimal('2'))
        mark_value = Decimal(position.quantity_shares) * mark_price
        open_pnl += mark_value - Decimal(position.size_usd) - Decimal(position.fee_open)

    closed_pnl = Position.objects.filter(account=account, status=Position.STATUS_CLOSED).aggregate(total=Sum('pnl_usd'))['total'] or Decimal('0')
    total_fees = (
        Position.objects.filter(account=account).aggregate(total=Sum('fee_open'))['total'] or Decimal('0')
    ) + (
        Position.objects.filter(account=account).aggregate(total=Sum('fee_close'))['total'] or Decimal('0')
    )

    equity = Decimal(account.balance_cash) + Decimal(account.balance_reserved) + open_pnl

    PerformanceSnapshot.objects.create(
        account=account,
        snapshot_date=timezone.localdate(),
        equity=_safe_decimal(equity, '0.01'),
        cash=_safe_decimal(Decimal(account.balance_cash), '0.01'),
        open_pnl=_safe_decimal(open_pnl, '0.01'),
        closed_pnl=_safe_decimal(closed_pnl, '0.01'),
        total_fees=_safe_decimal(total_fees, '0.01'),
        trade_count=SimulationTrade.objects.filter(account=account).count(),
    )


def snapshot_market_ticks(limit: int = 220) -> int:
    tracked_markets = list(
        Market.objects.filter(source=Market.SOURCE_POLYMARKET, is_active=True)
        .order_by('-liquidity_usd')[:limit]
    )
    if not tracked_markets:
        return 0

    now = timezone.now()
    ticks = []
    for market in tracked_markets:
        ticks.append(
            MarketPriceTick(
                market=market,
                captured_at=now,
                price_yes=_safe_decimal(Decimal(market.last_price_yes)),
                yes_bid=_safe_decimal(Decimal(market.yes_bid), '0.0001') if market.yes_bid is not None else None,
                yes_ask=_safe_decimal(Decimal(market.yes_ask), '0.0001') if market.yes_ask is not None else None,
                liquidity_usd=_safe_decimal(Decimal(market.liquidity_usd), '0.01'),
                volume_24h_usd=_safe_decimal(Decimal(market.volume_24h_usd), '0.01'),
            )
        )
    MarketPriceTick.objects.bulk_create(ticks, batch_size=200)

    max_age_days = int(os.getenv('SIMULATOR_TICK_RETENTION_DAYS', '21'))
    retention_from = now - timedelta(days=max(1, max_age_days))
    MarketPriceTick.objects.filter(captured_at__lt=retention_from).delete()
    return len(ticks)


def bootstrap_default_account() -> SimulationAccount:
    account, _ = SimulationAccount.objects.get_or_create(
        name='default-paper-account',
        defaults={
            'starting_balance': Decimal('10000.00'),
            'balance_cash': Decimal('10000.00'),
            'balance_reserved': Decimal('0.00'),
            'fee_rate': Decimal(os.getenv('SIMULATOR_FEE_RATE', '0.01000')),
            'position_limit': Decimal('250.00'),
            'is_active': True,
        },
    )
    return account


def reset_account_state(account: SimulationAccount) -> None:
    account.positions.all().delete()
    account.trades.all().delete()
    account.snapshots.all().delete()
    MarketPriceTick.objects.all().delete()
    account.balance_cash = account.starting_balance
    account.balance_reserved = DECIMAL_ZERO
    account.save(update_fields=['balance_cash', 'balance_reserved'])


def run_once(threshold: Decimal = DEFAULT_EDGE_THRESHOLD) -> dict[str, int]:
    account = bootstrap_default_account()
    markets_synced = sync_polymarket_markets(limit=250)
    ticks_saved = snapshot_market_ticks(limit=220)
    created_signals = ingest_world_signals()
    created_predictions = generate_predictions(window_hours=36)
    trade_result = execute_trading_cycle(account, threshold=threshold)
    return {
        'markets_synced': markets_synced,
        'ticks_saved': ticks_saved,
        'signals': created_signals,
        'predictions': created_predictions,
        'opened': trade_result['opened'],
        'closed': trade_result['closed'],
    }


def run_loop(interval_seconds: int = 300, threshold: Decimal = DEFAULT_EDGE_THRESHOLD) -> None:
    while True:
        run_once(threshold=threshold)
        time.sleep(max(20, interval_seconds))
