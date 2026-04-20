from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from decimal import Decimal
from urllib.request import Request, urlopen

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
)


DECIMAL_ZERO = Decimal('0.00')
DECIMAL_ONE = Decimal('1.0000')
DECIMAL_HALF = Decimal('0.5000')
DEFAULT_EDGE_THRESHOLD = Decimal('0.01')

POLYMARKET_MARKETS_URL = os.getenv(
    'POLYMARKET_MARKETS_URL',
    'https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=200',
)
CLOB_API_URL = 'https://clob.polymarket.com'


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Polymarket Gamma API — market sync
# ---------------------------------------------------------------------------

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

        # Extract CLOB yes-token ID for real orderbook lookups
        clob_token_ids = item.get('clobTokenIds') or []
        if isinstance(clob_token_ids, str):
            try:
                clob_token_ids = json.loads(clob_token_ids)
            except Exception:
                clob_token_ids = []
        yes_token_id = str(clob_token_ids[0]) if isinstance(clob_token_ids, list) and clob_token_ids else ''

        # Gamma API rarely provides real bid/ask; sync_clob_orderbooks() will overwrite with real data
        spread_default = Decimal('0.02')
        yes_bid = _bounded_price(_parse_decimal(item.get('yesBid'), default=yes_price - spread_default / Decimal('2')))
        yes_ask = _bounded_price(_parse_decimal(item.get('yesAsk'), default=yes_price + spread_default / Decimal('2')))
        no_bid = _bounded_price(_parse_decimal(item.get('noBid'), default=(DECIMAL_ONE - yes_price) - spread_default / Decimal('2')))
        no_ask = _bounded_price(_parse_decimal(item.get('noAsk'), default=(DECIMAL_ONE - yes_price) + spread_default / Decimal('2')))

        symbol = f'poly-{market_id}'
        defaults = {
            'external_id': market_id,
            'clob_token_id': yes_token_id,
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
        Market.objects.update_or_create(symbol=symbol, defaults=defaults)
        created_or_updated += 1

    return created_or_updated


# ---------------------------------------------------------------------------
# Polymarket CLOB API — real orderbook bid/ask
# ---------------------------------------------------------------------------

def _fetch_clob_best(token_id: str, timeout: int = 5) -> tuple[Decimal | None, Decimal | None]:
    """Fetch best bid and best ask from CLOB orderbook for a single yes-token.

    Returns (best_bid, best_ask) with validated prices, or (None, None) on any error.
    """
    if not token_id:
        return None, None
    url = f'{CLOB_API_URL}/book?token_id={token_id}'
    headers = {'User-Agent': '08-trading-simulator/1.0'}
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=timeout) as response:
            data = json.loads(response.read().decode('utf-8'))
        bids = data.get('bids') or []
        asks = data.get('asks') or []
        if not bids or not asks:
            return None, None
        best_bid = _parse_decimal(bids[0].get('price'), default=DECIMAL_ZERO)
        best_ask = _parse_decimal(asks[0].get('price'), default=DECIMAL_ZERO)
        # Sanity check: valid prices and non-zero spread
        if best_bid <= DECIMAL_ZERO or best_ask <= DECIMAL_ZERO or best_bid >= best_ask:
            return None, None
        return _bounded_price(best_bid), _bounded_price(best_ask)
    except Exception:
        return None, None


def sync_clob_orderbooks(limit: int = 60) -> int:
    """Update real bid/ask on top liquid markets using CLOB orderbook API.

    Runs parallel HTTP requests (max 8 workers) and saves results back to Market.
    Returns the number of markets successfully updated.
    """
    markets = list(
        Market.objects.filter(
            source=Market.SOURCE_POLYMARKET,
            is_active=True,
        )
        .exclude(clob_token_id='')
        .exclude(clob_token_id__isnull=True)
        .order_by('-liquidity_usd')[:limit]
    )
    if not markets:
        return 0

    updated = 0
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_clob_best, m.clob_token_id): m for m in markets}
        for future in as_completed(futures):
            market = futures[future]
            bid, ask = future.result()
            if bid is not None and ask is not None:
                market.yes_bid = _safe_decimal(bid)
                market.yes_ask = _safe_decimal(ask)
                # No token is the complement: buying No = selling Yes
                market.no_bid = _safe_decimal(_bounded_price(DECIMAL_ONE - ask))
                market.no_ask = _safe_decimal(_bounded_price(DECIMAL_ONE - bid))
                market.save(update_fields=['yes_bid', 'yes_ask', 'no_bid', 'no_ask'])
                updated += 1

    return updated


# ---------------------------------------------------------------------------
# Prediction generation (market-data only, no external signals)
# ---------------------------------------------------------------------------

def generate_market_micro_predictions(window_hours: int = 12, max_markets: int = 180) -> int:
    """Build trade signals from real Polymarket market data only (price ticks + spread + liquidity).

    No external news sentiment is used. All inputs come from Polymarket.
    """
    since = timezone.now() - timedelta(hours=window_hours)
    markets = (
        Market.objects.filter(is_active=True, source=Market.SOURCE_POLYMARKET)
        .order_by('-liquidity_usd')[:max_markets]
    )

    created = 0
    for market in markets:
        ticks = list(
            MarketPriceTick.objects.filter(market=market, captured_at__gte=since)
            .order_by('-captured_at')[:40]
        )

        current_price = Decimal(market.last_price_yes)
        if not ticks:
            prob_yes = _bounded_price(current_price)
            confidence = Decimal('30.00')
            reasoning = 'Market-only mode: not enough tick history yet, using current Polymarket price.'
        else:
            ticks.reverse()
            first_price = Decimal(ticks[0].price_yes)
            last_price = Decimal(ticks[-1].price_yes)
            high_price = max(Decimal(t.price_yes) for t in ticks)
            low_price = min(Decimal(t.price_yes) for t in ticks)

            momentum = last_price - first_price
            mean_reversion = DECIMAL_HALF - last_price
            volatility = max(Decimal('0.0001'), high_price - low_price)
            liquidity = max(Decimal('1.00'), Decimal(market.liquidity_usd or DECIMAL_ZERO))
            spread = _market_spread_for_side(market, Position.SIDE_YES)

            liquidity_factor = min(Decimal('1.25'), Decimal('0.60') + (liquidity / Decimal('50000')))
            spread_penalty = max(Decimal('0.35'), Decimal('1.00') - (spread / Decimal('0.08')))
            vol_penalty = max(Decimal('0.40'), Decimal('1.00') - (volatility * Decimal('4.0')))

            score = (momentum * Decimal('1.40')) + (mean_reversion * Decimal('0.30'))
            score *= liquidity_factor * spread_penalty * vol_penalty

            prob_yes = _bounded_price(last_price + (score * Decimal('0.80')))

            base_conf = Decimal('34.00')
            conf_from_ticks = min(Decimal('30.00'), Decimal(len(ticks)))
            conf_from_spread = max(Decimal('0.00'), Decimal('12.00') - (spread * Decimal('180')))
            confidence = min(Decimal('92.00'), base_conf + conf_from_ticks + conf_from_spread)
            reasoning = (
                'Market-only Polymarket signal from real tick momentum, spread and liquidity '
                f'(ticks={len(ticks)}, spread={spread:.4f}, liq={liquidity:.0f}).'
            )

        MarketPrediction.objects.create(
            market=market,
            probability_yes=_safe_decimal(prob_yes),
            confidence=_safe_decimal(confidence, '0.01'),
            reasoning=reasoning,
        )
        created += 1

    return created


# ---------------------------------------------------------------------------
# Trade execution helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Trading cycle
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Account snapshot
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Tick capture
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Account management
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Main run loop
# ---------------------------------------------------------------------------

def run_once(threshold: Decimal = DEFAULT_EDGE_THRESHOLD) -> dict[str, int]:
    account = bootstrap_default_account()
    markets_synced = sync_polymarket_markets(limit=250)
    clob_updated = sync_clob_orderbooks(limit=60)
    ticks_saved = snapshot_market_ticks(limit=220)
    created_predictions = generate_market_micro_predictions(window_hours=12)

    trade_result = execute_trading_cycle(account, threshold=threshold)
    return {
        'markets_synced': markets_synced,
        'clob_updated': clob_updated,
        'ticks_saved': ticks_saved,
        'signal_mode': 'market_only_clob',
        'predictions': created_predictions,
        'opened': trade_result['opened'],
        'closed': trade_result['closed'],
    }


def run_loop(interval_seconds: int = 300, threshold: Decimal = DEFAULT_EDGE_THRESHOLD) -> None:
    while True:
        run_once(threshold=threshold)
        time.sleep(max(20, interval_seconds))
