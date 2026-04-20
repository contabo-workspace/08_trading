from decimal import Decimal
import os
from datetime import timedelta

from django.db.models import Sum
from django.db.utils import OperationalError
from django.http import JsonResponse
from django.utils import timezone
from django.views import View
from django.views.generic import TemplateView

from simulator.models import Market, MarketPrediction, MarketPriceTick, PerformanceSnapshot, Position, SimulationAccount, SimulationTrade


def _to_epoch_ms(value) -> int:
    return int(timezone.localtime(value).timestamp() * 1000)


def _bucket_timestamp(value, minutes: int = 5):
    local = timezone.localtime(value)
    minute = (local.minute // minutes) * minutes
    return local.replace(minute=minute, second=0, microsecond=0)


def _build_candles(predictions: list[MarketPrediction], bucket_minutes: int = 5) -> list[dict]:
    grouped: dict = {}
    for pred in predictions:
        bucket = _bucket_timestamp(pred.created_at, minutes=bucket_minutes)
        price = float(pred.probability_yes)
        if bucket not in grouped:
            grouped[bucket] = {
                'o': price,
                'h': price,
                'l': price,
                'c': price,
            }
        else:
            grouped[bucket]['h'] = max(grouped[bucket]['h'], price)
            grouped[bucket]['l'] = min(grouped[bucket]['l'], price)
            grouped[bucket]['c'] = price

    candles = []
    for bucket in sorted(grouped.keys()):
        item = grouped[bucket]
        candles.append(
            {
                'x': _to_epoch_ms(bucket),
                'y': [item['o'], item['h'], item['l'], item['c']],
            }
        )
    return candles


def _build_candles_from_ticks(ticks: list[MarketPriceTick], bucket_minutes: int = 5) -> tuple[list[dict], list[dict]]:
    grouped: dict = {}
    for tick in ticks:
        bucket = _bucket_timestamp(tick.captured_at, minutes=bucket_minutes)
        price = float(tick.price_yes)
        if bucket not in grouped:
            grouped[bucket] = {
                'o': price,
                'h': price,
                'l': price,
                'c': price,
                'count': 1,
                'liquidity_sum': float(tick.liquidity_usd or 0),
            }
        else:
            grouped[bucket]['h'] = max(grouped[bucket]['h'], price)
            grouped[bucket]['l'] = min(grouped[bucket]['l'], price)
            grouped[bucket]['c'] = price
            grouped[bucket]['count'] += 1
            grouped[bucket]['liquidity_sum'] += float(tick.liquidity_usd or 0)

    candles = []
    volumes = []
    for bucket in sorted(grouped.keys()):
        item = grouped[bucket]
        candles.append(
            {
                'x': _to_epoch_ms(bucket),
                'y': [item['o'], item['h'], item['l'], item['c']],
            }
        )

        # Proxy volume/activity for prediction-market data where true tick volume may be unavailable.
        activity = max(1.0, ((item['h'] - item['l']) * 100000.0) + (item['count'] * 6.0) + (item['liquidity_sum'] / 60000.0))
        volumes.append({'x': _to_epoch_ms(bucket), 'y': round(activity, 2)})

    return candles, volumes


def _position_break_even_probability(position: Position, account: SimulationAccount) -> Decimal:
    qty = Decimal(position.quantity_shares)
    if qty <= Decimal('0'):
        return Decimal(position.entry_prob)

    denominator = Decimal('1.0') - Decimal(account.fee_rate)
    if denominator <= Decimal('0'):
        return Decimal(position.entry_prob)

    proceeds = (Decimal(position.size_usd) + Decimal(position.fee_open)) / denominator
    close_prob = proceeds / qty
    return max(Decimal('0.01'), min(Decimal('0.99'), close_prob))


def _position_target_probability(position: Position, account: SimulationAccount, target_pnl: Decimal) -> Decimal:
    qty = Decimal(position.quantity_shares)
    if qty <= Decimal('0'):
        return Decimal(position.entry_prob)

    denominator = Decimal('1.0') - Decimal(account.fee_rate)
    if denominator <= Decimal('0'):
        return Decimal(position.entry_prob)

    proceeds = (Decimal(position.size_usd) + Decimal(position.fee_open) + target_pnl) / denominator
    close_prob = proceeds / qty
    return max(Decimal('0.01'), min(Decimal('0.99'), close_prob))


def _yes_equivalent(probability: Decimal, side: str) -> Decimal:
    if side == Position.SIDE_NO:
        return max(Decimal('0.01'), min(Decimal('0.99'), Decimal('1.0') - Decimal(probability)))
    return max(Decimal('0.01'), min(Decimal('0.99'), Decimal(probability)))


def _timeframe_settings(timeframe: str) -> dict:
    tf_map = {
        '15m': {'hours': 8, 'bucket': 1, 'max_points': 180},
        '1h': {'hours': 36, 'bucket': 5, 'max_points': 180},
        '4h': {'hours': 7 * 24, 'bucket': 15, 'max_points': 180},
        '1d': {'hours': 30 * 24, 'bucket': 60, 'max_points': 220},
    }
    return tf_map.get(timeframe, tf_map['1h'])


def _build_dashboard_payload(selected_market_symbol: str | None = None, selected_timeframe: str = '1h') -> dict:
    payload = {'simulator_ready': False}
    account = SimulationAccount.objects.filter(is_active=True).order_by('created_at').first()
    if not account:
        return payload

    latest_snapshot = PerformanceSnapshot.objects.filter(account=account).order_by('-snapshot_date', '-created_at').first()
    open_positions_qs = Position.objects.filter(account=account, status=Position.STATUS_OPEN).select_related('market')
    recent_trades_qs = SimulationTrade.objects.filter(account=account).select_related('market').order_by('-executed_at')[:8]

    invested_total = open_positions_qs.aggregate(total=Sum('size_usd'))['total'] or Decimal('0.00')
    open_fees_total = open_positions_qs.aggregate(total=Sum('fee_open'))['total'] or Decimal('0.00')
    open_count = open_positions_qs.count()
    closed_count = Position.objects.filter(account=account, status=Position.STATUS_CLOSED).count()

    snapshots = list(
        PerformanceSnapshot.objects.filter(account=account)
        .order_by('snapshot_date', 'created_at')[:120]
    )
    chart_labels = []
    chart_equity = []
    chart_invested = []
    chart_profit = []

    for snap in snapshots:
        invested_amount = Decimal(snap.equity) - Decimal(snap.cash) - Decimal(snap.open_pnl)
        if invested_amount < Decimal('0.00'):
            invested_amount = Decimal('0.00')
        chart_labels.append(timezone.localtime(snap.created_at).strftime('%d.%m. %H:%M'))
        chart_equity.append(float(snap.equity))
        chart_invested.append(float(invested_amount))
        chart_profit.append(float(Decimal(snap.equity) - Decimal(account.starting_balance)))

    open_positions = [
        {
            'market': p.market.name,
            'side': 'ANO' if p.side.lower() == 'yes' else 'NE',
            'entry_prob': str(p.entry_prob),
            'size_usd': f"{p.size_usd:.2f}",
        }
        for p in open_positions_qs[:6]
    ]

    recent_trades = [
        {
            'time': timezone.localtime(t.executed_at).strftime('%d.%m. %H:%M'),
            'action': (
                ('Nákup ' if t.action.lower() == 'buy' else 'Prodej ')
                + ('ANO' if t.side.lower() == 'yes' else 'NE')
            ),
            'market': t.market.name,
            'price': str(t.probability),
            'size_usd': f"{t.size_usd:.2f}",
            'fee_usd': f"{t.fee_usd:.2f}",
        }
        for t in recent_trades_qs
    ]

    payload.update(
        {
            'simulator_ready': True,
            'account_name': account.name,
            'cash_free': f"{account.balance_cash:.2f}",
            'invested_total': f"{invested_total:.2f}",
            'running_value': (
                f"{(Decimal(invested_total) + Decimal(latest_snapshot.open_pnl)):.2f}"
                if latest_snapshot
                else f"{invested_total:.2f}"
            ),
            'equity': f"{latest_snapshot.equity:.2f}" if latest_snapshot else f"{account.balance_cash + account.balance_reserved:.2f}",
            'profit_total': f"{(Decimal(latest_snapshot.equity) - Decimal(account.starting_balance)):.2f}" if latest_snapshot else '0.00',
            'open_pnl': f"{latest_snapshot.open_pnl:.2f}" if latest_snapshot else '0.00',
            'open_count': open_count,
            'closed_count': closed_count,
            'open_fees_total': f"{open_fees_total:.2f}",
            'chart_labels': chart_labels,
            'chart_equity': chart_equity,
            'chart_invested': chart_invested,
            'chart_profit': chart_profit,
            'open_positions': open_positions,
            'recent_trades': recent_trades,
            'updated_at': timezone.localtime(timezone.now()).strftime('%H:%M:%S'),
            'advanced_market_options': [],
            'advanced_selected_symbol': '',
            'advanced_market_name': '',
            'advanced_candles': [],
            'advanced_markers': [],
            'advanced_entry_line': None,
            'advanced_breakeven_line': None,
            'advanced_stop_loss_line': None,
            'advanced_take_profit_line': None,
            'advanced_volume': [],
            'advanced_recent_trades': [],
            'advanced_selected_timeframe': selected_timeframe,
            'advanced_timeframes': ['15m', '1h', '4h', '1d'],
        }
    )

    markets_for_selector = []
    seen_symbols = set()
    for position in open_positions_qs[:20]:
        if position.market.symbol in seen_symbols:
            continue
        seen_symbols.add(position.market.symbol)
        markets_for_selector.append({'symbol': position.market.symbol, 'name': position.market.name})

    recent_markets = (
        SimulationTrade.objects.filter(account=account)
        .select_related('market')
        .order_by('-executed_at')[:40]
    )
    for trade in recent_markets:
        if trade.market.symbol in seen_symbols:
            continue
        seen_symbols.add(trade.market.symbol)
        markets_for_selector.append({'symbol': trade.market.symbol, 'name': trade.market.name})

    focus_symbol = (selected_market_symbol or '').strip()
    if not focus_symbol and markets_for_selector:
        focus_symbol = markets_for_selector[0]['symbol']

    payload['advanced_market_options'] = markets_for_selector[:16]

    if focus_symbol:
        focus_market = Market.objects.filter(symbol=focus_symbol).first()
    else:
        focus_market = None

    if focus_market:
        payload['advanced_selected_symbol'] = focus_market.symbol
        payload['advanced_market_name'] = focus_market.name

        tf_value = selected_timeframe if selected_timeframe in {'15m', '1h', '4h', '1d'} else '1h'
        tf_cfg = _timeframe_settings(tf_value)
        payload['advanced_selected_timeframe'] = tf_value

        since_dt = timezone.now() - timedelta(hours=tf_cfg['hours'])

        tick_points = list(
            MarketPriceTick.objects.filter(market=focus_market, captured_at__gte=since_dt)
            .order_by('-captured_at')[:5000]
        )
        tick_points.reverse()
        candles = []
        volume_bars = []
        if tick_points:
            candles, volume_bars = _build_candles_from_ticks(tick_points, bucket_minutes=tf_cfg['bucket'])
        else:
            prediction_points = list(
                MarketPrediction.objects.filter(market=focus_market, created_at__gte=since_dt)
                .order_by('-created_at')[:1200]
            )
            prediction_points.reverse()
            candles = _build_candles(prediction_points, bucket_minutes=tf_cfg['bucket'])
            volume_bars = [{'x': c['x'], 'y': 1.0} for c in candles]

        payload['advanced_candles'] = candles[-tf_cfg['max_points']:]
        payload['advanced_volume'] = volume_bars[-tf_cfg['max_points']:]

        trade_points = list(
            SimulationTrade.objects.filter(account=account, market=focus_market)
            .order_by('-executed_at')[:120]
        )
        trade_points.reverse()

        markers = []
        for trade in trade_points:
            yes_price = _yes_equivalent(Decimal(trade.probability), trade.side)
            markers.append(
                {
                    'x': _to_epoch_ms(trade.executed_at),
                    'y': float(yes_price),
                    'label': (
                        ('Nákup ' if trade.action.lower() == 'buy' else 'Prodej ')
                        + ('ANO' if trade.side.lower() == 'yes' else 'NE')
                    ),
                    'kind': trade.action.lower(),
                }
            )
        payload['advanced_markers'] = markers[-80:]
        payload['advanced_recent_trades'] = [
            {
                'time': timezone.localtime(t.executed_at).strftime('%d.%m. %H:%M:%S'),
                'action': 'Nákup' if t.action.lower() == 'buy' else 'Prodej',
                'side': 'ANO' if t.side.lower() == 'yes' else 'NE',
                'price_yes': f"{_yes_equivalent(Decimal(t.probability), t.side):.4f}",
                'size_usd': f"{t.size_usd:.2f}",
                'fee_usd': f"{t.fee_usd:.2f}",
            }
            for t in trade_points[-12:][::-1]
        ]

        open_position = (
            Position.objects.filter(account=account, market=focus_market, status=Position.STATUS_OPEN)
            .order_by('-opened_at')
            .first()
        )
        if open_position:
            entry_yes = _yes_equivalent(Decimal(open_position.entry_prob), open_position.side)
            breakeven_side_prob = _position_break_even_probability(open_position, account)
            breakeven_yes = _yes_equivalent(breakeven_side_prob, open_position.side)

            stop_loss_pct = Decimal(os.getenv('SIMULATOR_STOP_LOSS_PCT', '0.35'))
            take_profit_pct = Decimal(os.getenv('SIMULATOR_TAKE_PROFIT_PCT', '0.20'))
            stop_loss_target = Decimal(open_position.size_usd) * stop_loss_pct * Decimal('-1')
            take_profit_target = Decimal(open_position.size_usd) * take_profit_pct
            stop_side_prob = _position_target_probability(open_position, account, stop_loss_target)
            take_side_prob = _position_target_probability(open_position, account, take_profit_target)

            payload['advanced_entry_line'] = float(entry_yes)
            payload['advanced_breakeven_line'] = float(breakeven_yes)
            payload['advanced_stop_loss_line'] = float(_yes_equivalent(stop_side_prob, open_position.side))
            payload['advanced_take_profit_line'] = float(_yes_equivalent(take_side_prob, open_position.side))

    expected_interval = int(os.getenv('SIMULATOR_EXPECTED_INTERVAL_SECONDS', '300'))
    stale_after = timedelta(seconds=max(60, expected_interval * 2))
    if latest_snapshot:
        last_engine_dt = timezone.localtime(latest_snapshot.created_at)
        payload['last_engine_run_at'] = last_engine_dt.strftime('%H:%M:%S')
        payload['engine_running'] = timezone.now() - latest_snapshot.created_at <= stale_after

        previous_snapshot = (
            PerformanceSnapshot.objects.filter(account=account, created_at__lt=latest_snapshot.created_at)
            .order_by('-created_at')
            .first()
        )
        if previous_snapshot:
            delta = Decimal(latest_snapshot.equity) - Decimal(previous_snapshot.equity)
            payload['equity_delta_last_run'] = f"{delta:.2f}"
            payload['equity_changed'] = abs(delta) >= Decimal('0.01')
        else:
            payload['equity_delta_last_run'] = '0.00'
            payload['equity_changed'] = False
    else:
        payload['last_engine_run_at'] = '-'
        payload['engine_running'] = False
        payload['equity_delta_last_run'] = '0.00'
        payload['equity_changed'] = False

    return payload


class HomePageView(TemplateView):
    template_name = 'home/index.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        try:
            context.update(_build_dashboard_payload())
        except OperationalError:
            # During first startup before migrations are applied.
            pass

        return context


class DashboardDataView(View):
    def get(self, request):
        try:
            market_symbol = request.GET.get('market_symbol', '').strip()
            timeframe = request.GET.get('timeframe', '1h').strip().lower()
            return JsonResponse(_build_dashboard_payload(selected_market_symbol=market_symbol, selected_timeframe=timeframe))
        except OperationalError:
            return JsonResponse({'simulator_ready': False, 'error': 'db_not_ready'})
