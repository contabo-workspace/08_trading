from decimal import Decimal

from django.db import models
from django.utils import timezone


class Market(models.Model):
    SOURCE_POLYMARKET = 'polymarket'
    SOURCE_SYNTHETIC = 'synthetic'
    SOURCE_CHOICES = [
        (SOURCE_POLYMARKET, 'Polymarket'),
        (SOURCE_SYNTHETIC, 'Synthetic'),
    ]

    symbol = models.CharField(max_length=80, unique=True)
    external_id = models.CharField(max_length=80, unique=True, null=True, blank=True)
    source = models.CharField(max_length=20, choices=SOURCE_CHOICES, default=SOURCE_SYNTHETIC)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    last_price_yes = models.DecimalField(max_digits=6, decimal_places=4, default=Decimal('0.5000'))
    yes_bid = models.DecimalField(max_digits=6, decimal_places=4, null=True, blank=True)
    yes_ask = models.DecimalField(max_digits=6, decimal_places=4, null=True, blank=True)
    no_bid = models.DecimalField(max_digits=6, decimal_places=4, null=True, blank=True)
    no_ask = models.DecimalField(max_digits=6, decimal_places=4, null=True, blank=True)
    liquidity_usd = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal('0.00'))
    volume_24h_usd = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal('0.00'))
    market_close_at = models.DateTimeField(null=True, blank=True)
    fee_bps = models.PositiveIntegerField(default=100)
    is_active = models.BooleanField(default=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['symbol']

    def __str__(self) -> str:
        return f"{self.symbol} ({self.last_price_yes})"


class WorldSignal(models.Model):
    SOURCE_RSS = 'rss'
    SOURCE_SOCIAL = 'social'
    SOURCE_NEWS_API = 'news_api'
    SOURCE_CHOICES = [
        (SOURCE_RSS, 'RSS'),
        (SOURCE_SOCIAL, 'Social'),
        (SOURCE_NEWS_API, 'News API'),
    ]

    source = models.CharField(max_length=20, choices=SOURCE_CHOICES)
    source_name = models.CharField(max_length=120)
    headline = models.CharField(max_length=500)
    url = models.URLField(max_length=1000, unique=True)
    published_at = models.DateTimeField(default=timezone.now)
    sentiment_score = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal('0.00'))
    impact_score = models.DecimalField(max_digits=5, decimal_places=2, default=Decimal('1.00'))
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-published_at']

    def __str__(self) -> str:
        return f"{self.source_name}: {self.headline[:80]}"


class SimulationAccount(models.Model):
    name = models.CharField(max_length=100, unique=True)
    starting_balance = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal('10000.00'))
    balance_cash = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal('10000.00'))
    balance_reserved = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal('0.00'))
    fee_rate = models.DecimalField(max_digits=6, decimal_places=5, default=Decimal('0.01000'))
    position_limit = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('250.00'))
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['name']

    def __str__(self) -> str:
        return self.name


class MarketPrediction(models.Model):
    market = models.ForeignKey(Market, on_delete=models.CASCADE, related_name='predictions')
    created_at = models.DateTimeField(auto_now_add=True)
    probability_yes = models.DecimalField(max_digits=6, decimal_places=4)
    confidence = models.DecimalField(max_digits=5, decimal_places=2)
    reasoning = models.TextField(blank=True)
    signals = models.ManyToManyField(WorldSignal, blank=True, related_name='predictions')

    class Meta:
        ordering = ['-created_at']

    def __str__(self) -> str:
        return f"{self.market.symbol}: {self.probability_yes} ({self.created_at:%Y-%m-%d %H:%M})"


class Position(models.Model):
    SIDE_YES = 'yes'
    SIDE_NO = 'no'
    SIDE_CHOICES = [(SIDE_YES, 'YES'), (SIDE_NO, 'NO')]

    STATUS_OPEN = 'open'
    STATUS_CLOSED = 'closed'
    STATUS_CHOICES = [(STATUS_OPEN, 'Open'), (STATUS_CLOSED, 'Closed')]

    account = models.ForeignKey(SimulationAccount, on_delete=models.CASCADE, related_name='positions')
    market = models.ForeignKey(Market, on_delete=models.CASCADE, related_name='positions')
    side = models.CharField(max_length=10, choices=SIDE_CHOICES)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default=STATUS_OPEN)
    entry_prob = models.DecimalField(max_digits=6, decimal_places=4)
    close_prob = models.DecimalField(max_digits=6, decimal_places=4, null=True, blank=True)
    size_usd = models.DecimalField(max_digits=12, decimal_places=2)
    quantity_shares = models.DecimalField(max_digits=14, decimal_places=6)
    fee_open = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    fee_close = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    pnl_usd = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    opened_at = models.DateTimeField(auto_now_add=True)
    closed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-opened_at']

    def __str__(self) -> str:
        return f"{self.account.name} {self.market.symbol} {self.side} {self.status}"


class SimulationTrade(models.Model):
    ACTION_BUY = 'buy'
    ACTION_SELL = 'sell'
    ACTION_CHOICES = [(ACTION_BUY, 'BUY'), (ACTION_SELL, 'SELL')]

    account = models.ForeignKey(SimulationAccount, on_delete=models.CASCADE, related_name='trades')
    market = models.ForeignKey(Market, on_delete=models.CASCADE, related_name='trades')
    position = models.ForeignKey(Position, on_delete=models.SET_NULL, null=True, blank=True, related_name='trades')
    action = models.CharField(max_length=10, choices=ACTION_CHOICES)
    side = models.CharField(max_length=10, choices=Position.SIDE_CHOICES)
    probability = models.DecimalField(max_digits=6, decimal_places=4)
    size_usd = models.DecimalField(max_digits=12, decimal_places=2)
    fee_usd = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    expected_edge = models.DecimalField(max_digits=6, decimal_places=4, default=Decimal('0.0000'))
    note = models.CharField(max_length=300, blank=True)
    executed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-executed_at']

    def __str__(self) -> str:
        return f"{self.account.name} {self.action} {self.market.symbol}"


class PerformanceSnapshot(models.Model):
    account = models.ForeignKey(SimulationAccount, on_delete=models.CASCADE, related_name='snapshots')
    snapshot_date = models.DateField(default=timezone.localdate)
    equity = models.DecimalField(max_digits=14, decimal_places=2)
    cash = models.DecimalField(max_digits=14, decimal_places=2)
    open_pnl = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    closed_pnl = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    total_fees = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    trade_count = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-snapshot_date', '-created_at']

    def __str__(self) -> str:
        return f"{self.account.name} {self.snapshot_date} equity={self.equity}"
