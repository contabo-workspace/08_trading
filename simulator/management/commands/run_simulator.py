from django.core.management.base import BaseCommand
from decimal import Decimal

from simulator.services import DEFAULT_EDGE_THRESHOLD, bootstrap_default_account, reset_account_state, run_loop, run_once


class Command(BaseCommand):
    help = 'Runs the paper-trading simulator with world signal ingestion and fee-aware execution.'

    def add_arguments(self, parser):
        parser.add_argument('--loop', action='store_true', help='Run continuously in loop mode.')
        parser.add_argument('--interval', type=int, default=300, help='Loop interval in seconds (default: 300).')
        parser.add_argument('--reset', action='store_true', help='Reset paper account history and balances before run.')
        parser.add_argument('--threshold', type=str, default=str(DEFAULT_EDGE_THRESHOLD), help='Minimum edge threshold for opening trades.')

    def handle(self, *args, **options):
        account = bootstrap_default_account()
        if options['reset']:
            reset_account_state(account)
            self.stdout.write(self.style.WARNING('Paper account reset completed.'))

        threshold = Decimal(options['threshold'])

        if options['loop']:
            self.stdout.write(self.style.WARNING(f"Loop mode started. Interval: {options['interval']}s | threshold={threshold}"))
            run_loop(interval_seconds=options['interval'], threshold=threshold)
            return

        result = run_once(threshold=threshold)
        self.stdout.write(
            self.style.SUCCESS(
                'Simulator cycle done | '
                f"markets={result['markets_synced']} signals={result['signals']} predictions={result['predictions']} "
                f"opened={result['opened']} closed={result['closed']}"
            )
        )
