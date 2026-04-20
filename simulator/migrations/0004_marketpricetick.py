from decimal import Decimal

from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('simulator', '0003_remove_performancesnapshot_uniq_account_snapshot_date'),
    ]

    operations = [
        migrations.CreateModel(
            name='MarketPriceTick',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('captured_at', models.DateTimeField(db_index=True, default=django.utils.timezone.now)),
                ('price_yes', models.DecimalField(decimal_places=4, max_digits=6)),
                ('yes_bid', models.DecimalField(blank=True, decimal_places=4, max_digits=6, null=True)),
                ('yes_ask', models.DecimalField(blank=True, decimal_places=4, max_digits=6, null=True)),
                ('liquidity_usd', models.DecimalField(decimal_places=2, default=Decimal('0.00'), max_digits=14)),
                ('volume_24h_usd', models.DecimalField(decimal_places=2, default=Decimal('0.00'), max_digits=14)),
                ('market', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='price_ticks', to='simulator.market')),
            ],
            options={
                'ordering': ['-captured_at'],
                'indexes': [models.Index(fields=['market', '-captured_at'], name='sim_mkt_tick_mkt_cap_idx')],
            },
        ),
    ]
