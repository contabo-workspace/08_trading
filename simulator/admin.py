from django.contrib import admin

from .models import (
    Market,
    MarketPrediction,
    PerformanceSnapshot,
    Position,
    SimulationAccount,
    SimulationTrade,
    WorldSignal,
)


@admin.register(Market)
class MarketAdmin(admin.ModelAdmin):
    list_display = (
        'symbol',
        'source',
        'name',
        'last_price_yes',
        'yes_bid',
        'yes_ask',
        'liquidity_usd',
        'volume_24h_usd',
        'is_active',
        'updated_at',
    )
    list_filter = ('source', 'is_active')
    search_fields = ('symbol', 'external_id', 'name')


@admin.register(WorldSignal)
class WorldSignalAdmin(admin.ModelAdmin):
    list_display = ('source', 'source_name', 'headline', 'sentiment_score', 'impact_score', 'published_at')
    list_filter = ('source', 'source_name')
    search_fields = ('headline', 'source_name', 'url')


@admin.register(SimulationAccount)
class SimulationAccountAdmin(admin.ModelAdmin):
    list_display = ('name', 'starting_balance', 'balance_cash', 'balance_reserved', 'fee_rate', 'is_active')
    list_filter = ('is_active',)
    search_fields = ('name',)


@admin.register(MarketPrediction)
class MarketPredictionAdmin(admin.ModelAdmin):
    list_display = ('market', 'probability_yes', 'confidence', 'created_at')
    list_filter = ('market',)


@admin.register(Position)
class PositionAdmin(admin.ModelAdmin):
    list_display = ('account', 'market', 'side', 'status', 'entry_prob', 'close_prob', 'size_usd', 'pnl_usd', 'opened_at')
    list_filter = ('status', 'side', 'account')
    search_fields = ('market__symbol', 'account__name')


@admin.register(SimulationTrade)
class SimulationTradeAdmin(admin.ModelAdmin):
    list_display = ('account', 'action', 'market', 'side', 'probability', 'size_usd', 'fee_usd', 'expected_edge', 'executed_at')
    list_filter = ('action', 'side', 'account')


@admin.register(PerformanceSnapshot)
class PerformanceSnapshotAdmin(admin.ModelAdmin):
    list_display = ('account', 'snapshot_date', 'equity', 'cash', 'open_pnl', 'closed_pnl', 'total_fees', 'trade_count')
    list_filter = ('account', 'snapshot_date')
