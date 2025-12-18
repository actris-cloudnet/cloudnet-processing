from dataclasses import dataclass, field
from typing import Any


@dataclass
class MonitoringVariable:
    id: str
    human_readable_name: str
    order: int = field(default=0)

    def __repr__(self) -> str:
        return f"Variable({self.id})"

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "MonitoringVariable":
        return MonitoringVariable(
            id=data["id"],
            human_readable_name=data["humanReadableName"],
            order=data.get("order", 0),
        )


@dataclass
class MonitoringProduct:
    id: str
    human_readable_name: str
    variables: list[MonitoringVariable]

    def __repr__(self) -> str:
        return f"Product({self.id})"

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "MonitoringProduct":
        variables = [
            MonitoringVariable.from_dict(var) for var in data.get("variables", [])
        ]
        return MonitoringProduct(
            id=data["id"],
            human_readable_name=data["humanReadableName"],
            variables=variables,
        )
