import argparse
from typing import Callable

from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import Instrument, Site

from monitoring.monitor import monitor
from monitoring.period import Period, period_from_str
from monitoring.product import MonitoringProduct
from processing.config import Config


def main() -> None:
    config = Config()
    client = APIClient(f"{config.dataportal_url}/api/")
    parser = argparse.ArgumentParser()
    parser.add_argument("instrument", type=_instrument_from_client(client))
    parser.add_argument("site", type=_site_from_client(client))
    parser.add_argument("period", type=_period_from_str)
    parser.add_argument("product", type=_product_from_client(client))
    args = parser.parse_args()
    monitor(
        site=args.site,
        instrument=args.instrument,
        period=args.period,
        product=args.product,
    )


def _period_from_str(s: str) -> Period:
    try:
        return period_from_str(s)
    except ValueError as err:
        raise argparse.ArgumentTypeError(err) from err


def _site_from_client(client: APIClient) -> Callable[[str], Site]:
    sites = client.sites()
    site_dict = {site.id: site for site in sites}

    def validate_id(id_: str) -> Site:
        if id_ not in site_dict:
            print("Invalid site ID. Available sites:")
            _print_sites(sites)
            raise argparse.ArgumentTypeError("Invalid site ID.")
        return site_dict[id_]

    return validate_id


def _product_from_client(client: APIClient) -> Callable[[str], MonitoringProduct]:
    data = client.session.get(f"{client.base_url}monitoring-products/variables").json()
    products = [MonitoringProduct.from_dict(d) for d in data]
    product_dict = {p.id: p for p in products}

    def validate_product(id_: str) -> MonitoringProduct:
        if id_ not in product_dict:
            print("Invalid product ID. Available products:")
            _print_products(products)
            raise argparse.ArgumentTypeError(f"Invalid product ID: '{id_}'")
        return product_dict[id_]

    return validate_product


def _instrument_from_client(client: APIClient) -> Callable[[str], Instrument]:
    instruments = client.instruments()
    instrument_dict = {str(inst.uuid): inst for inst in instruments}

    def validate_uuid(uuid: str) -> Instrument:
        if uuid not in instrument_dict:
            print("Invalid instrument UUID. Available instruments:")
            _print_instruments(instruments)
            raise argparse.ArgumentTypeError(f"Invalid instrument UUID: '{uuid}'")
        return instrument_dict[uuid]

    return validate_uuid


def _print_products(products: list[MonitoringProduct]) -> None:
    for p in products:
        print(p.id)


def _print_sites(sites: list[Site]) -> None:
    for s in sorted(sites, key=lambda x: x.id):
        print(f"{s.id:<20} {s.human_readable_name}, {s.country}")


def _print_instruments(instruments: list[Instrument]) -> None:
    for inst in sorted(instruments, key=lambda x: (x.type, x.name)):
        print(f"{inst.type:<40} {inst.name:<40} {inst.uuid} {inst.pid}")


if __name__ == "__main__":
    main()
