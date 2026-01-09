import argparse
import itertools
import logging
from argparse import ArgumentParser, Namespace
from typing import Callable, Iterable, TypeVar

from cloudnet_api_client import APIClient

import monitoring.period as period_module
from monitoring.config import CONFIG
from monitoring.monitor import monitor
from monitoring.monitor_options import MonitorOptions
from monitoring.period import (
    All,
    Day,
    Month,
    PeriodProtocol,
    PeriodType,
    Week,
    Year,
    period_cls_from_str,
    period_str_from_cls,
)
from monitoring.product import MonitoringProduct
from monitoring.utils import RawFilesPayload
from processing.config import Config
from processing.metadata_api import MetadataApi
from processing.storage_api import StorageApi
from processing.utils import make_session

T = TypeVar("T", bound=PeriodProtocol)
PeriodList = list[All] | list[Day] | list[Month] | list[Week] | list[Year]


def main() -> None:
    api_client, md_api, storage_api = build_clients()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = _get_args()
    period_cls = period_cls_from_str(args.cmd)
    periods = build_periods(period_cls, args)
    periods_with_products = build_products(period_cls, periods, args.product)
    periods_with_products_sites_and_instruments = build_instruments(
        api_client, periods_with_products, args.site
    )
    validated_periods_with_products_sites_and_instruments = validate_products(
        md_api, periods_with_products_sites_and_instruments
    )

    for (
        period,
        product,
        site,
        instrument_uuid,
    ) in validated_periods_with_products_sites_and_instruments:
        try:
            monitor(
                MonitorOptions(
                    period,
                    product,
                    site,
                    instrument_uuid,
                    api_client,
                    storage_api,
                    md_api,
                )
            )
        except ValueError as err:
            logging.warning(err)


def build_clients() -> tuple[APIClient, MetadataApi, StorageApi]:
    config = Config()
    session = make_session()
    return (
        APIClient(base_url=f"{config.dataportal_url}/api"),
        MetadataApi(config, session),
        StorageApi(config, session),
    )


def build_periods(period_cls: type[PeriodType], args: Namespace) -> PeriodList:
    match period_cls:
        case period_module.Day:
            return _resolve_periods(args.start, args.stop, args.day, Day.now, Day.range)
        case period_module.Week:
            return _resolve_periods(
                args.start, args.stop, args.week, Week.now, Week.range
            )
        case period_module.Month:
            return _resolve_periods(
                args.start, args.stop, args.month, Month.now, Month.range
            )
        case period_module.Year:
            return _resolve_periods(
                args.start, args.stop, args.year, Year.now, Year.range
            )
        case period_module.All:
            return [All()]
        case _:
            raise ValueError


def build_products(
    period_cls: type[PeriodType],
    period_list: PeriodList,
    select_products: list[str] | None,
) -> list[tuple[PeriodType, str]]:
    period_str = period_str_from_cls(period_cls)

    products: list[str] = []
    for product, allowed_periods in CONFIG.items():
        if period_str in allowed_periods:
            products.append(product)
    if select_products:
        products = [p for p in products if p in select_products]
    return list(itertools.product(period_list, products))


def build_instruments(
    client: APIClient,
    product_list: list[tuple[PeriodType, str]],
    sites: list[str] | None,
) -> list[tuple[PeriodType, str, str, str]]:
    list_with_sites_and_instruments = []
    for period, product in product_list:
        for site, instrument_uuid in get_available_instruments(
            client, period, product, sites
        ):
            list_with_sites_and_instruments.append(
                (period, product, site, instrument_uuid)
            )
    return list_with_sites_and_instruments


def validate_products(
    api: MetadataApi,
    product_list: list[tuple[PeriodType, str, str, str]],
) -> list[tuple[PeriodType, MonitoringProduct, str, str]]:
    available_products = get_available_products(api)
    validated = []
    for period, product_str, site, instrument_uuid in product_list:
        if not product_str in available_products:
            raise ValueError(f"Invalid product '{product_str}'")
        validated.append(
            (period, available_products[product_str], site, instrument_uuid)
        )
    return validated


def get_available_products(api: MetadataApi) -> dict[str, MonitoringProduct]:
    data = api.get("api/monitoring-products/variables")
    return {entry["id"]: MonitoringProduct.from_dict(entry) for entry in data}


def get_available_instruments(
    client: APIClient, period: PeriodType, product: str, sites: list[str] | None
) -> list[tuple[str, str]]:
    payload: RawFilesPayload

    match product:
        case "halo-doppler-lidar_housekeeping":
            payload = {
                "instrument_id": "halo-doppler-lidar",
                "filename_prefix": "system_parameters_",
                "filename_suffix": ".txt",
            }
        case "halo-doppler-lidar_background":
            payload = {
                "instrument_id": "halo-doppler-lidar",
                "filename_prefix": "Background_",
                "filename_suffix": ".txt",
            }
        case "halo-doppler-lidar_signal":
            payload = {
                "instrument_id": "halo-doppler-lidar",
                "filename_suffix": ".hpl",
            }
        case _:
            raise ValueError
    match period:
        case Day() | Week() | Month() | Year():
            if product == "halo-doppler-lidar_housekeeping":
                start, stop = period.to_interval_padded(days=31)
            else:
                start, stop = period.to_interval()
            payload.update({"date_from": str(start), "date_to": str(stop)})
        case All():
            pass
    if sites and len(sites) == 1:
        payload.update({"site_id": sites[0]})

    records = client.raw_files(**payload)
    sites_and_uuids = sorted(
        list(set((r.site.id, str(r.instrument.uuid)) for r in records))
    )
    if sites:
        sites_and_uuids = [
            (site, uuid) for site, uuid in sites_and_uuids if site in sites
        ]
    return sites_and_uuids


def _resolve_periods(
    start: T | None,
    stop: T | None,
    explicit: list[T] | None,
    default_factory: Callable[[], T],
    range_func: Callable[[T, T], Iterable[T]],
) -> list[T]:
    if not start and not stop and not explicit:
        return [default_factory()]
    if not start and not stop and explicit is not None:
        return sorted(list(set(explicit)))  # type: ignore[type-var]
    if start is None:
        raise ValueError
    if stop is None:
        stop = default_factory()
    range_ = list(range_func(start, stop))
    combined = set(range_)
    if explicit:
        combined.update(explicit)
    return sorted(list(combined))  # type: ignore[type-var]


def _get_args() -> Namespace:
    parser = ArgumentParser()
    subp = parser.add_subparsers(dest="cmd", required=True)
    _build_day_args(subp.add_parser("day"))
    _build_week_args(subp.add_parser("week"))
    _build_month_args(subp.add_parser("month"))
    _build_year_args(subp.add_parser("year"))
    _build_all_args(subp.add_parser("all"))
    return parser.parse_args()


def _build_day_args(parser: ArgumentParser) -> None:
    parser.add_argument("--start", type=_parse_day)
    parser.add_argument("--stop", type=_parse_day)
    parser.add_argument("--day", type=_parse_day, nargs="*")
    _common(parser)


def _build_week_args(parser: ArgumentParser) -> None:
    parser.add_argument("--start", type=_parse_week)
    parser.add_argument("--stop", type=_parse_week)
    parser.add_argument("--week", type=_parse_week, nargs="*")
    _common(parser)


def _build_month_args(parser: ArgumentParser) -> None:
    parser.add_argument("--start", type=_parse_month)
    parser.add_argument("--stop", type=_parse_month)
    parser.add_argument("--month", type=_parse_month, nargs="*")
    _common(parser)


def _build_year_args(parser: ArgumentParser) -> None:
    parser.add_argument("--start", type=_parse_year)
    parser.add_argument("--stop", type=_parse_year)
    parser.add_argument("--year", type=_parse_year, nargs="*")
    _common(parser)


def _build_all_args(parser: ArgumentParser) -> None:
    _common(parser)


def _common(parser: ArgumentParser) -> None:
    parser.add_argument("--product", nargs="*")
    parser.add_argument("--site", nargs="*")


def _parse_year(year_str: str) -> Year:
    try:
        return Year.from_str(year_str)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid year format: '{year_str}'. Expected format: YYYY"
        )


def _parse_month(month_str: str) -> Month:
    try:
        return Month.from_str(month_str)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid month format: '{month_str}'. Expected format: YYYY-MM"
        )


def _parse_week(week_str: str) -> Week:
    try:
        return Week.from_str(week_str)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid week format: '{week_str}'. Expected format: YYYY-VV"
        )


def _parse_day(date_str: str) -> Day:
    try:
        return Day.from_str(date_str)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date: '{date_str}'. Expected format: YYYY-MM-DD"
        )


if __name__ == "__main__":
    main()
