from decimal import Decimal
import os
from datetime import timedelta

from django.db.models import Sum
from django.db.utils import OperationalError
from django.http import JsonResponse
from django.utils import timezone
from django.views import View
from django.views.generic import TemplateView

from simulator.models import PerformanceSnapshot, Position, SimulationAccount, SimulationTrade


def _build_dashboard_payload() -> dict:
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
        }
    )

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
            return JsonResponse(_build_dashboard_payload())
        except OperationalError:
            return JsonResponse({'simulator_ready': False, 'error': 'db_not_ready'})
