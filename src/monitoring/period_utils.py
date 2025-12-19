

from monitoring.period import PeriodType
def periods_from_args(
    period_type: PeriodType,
    start: str | None,
    stop: str | None,
    period: list[str],
) -> None:
    if period_type == PeriodType.ALL:
        raise ValueError

    breakpoint()
