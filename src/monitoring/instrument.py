class Instrument:
    def __init__(self, pid: str, uuid: str, id_: str, type_: str):
        self.pid = pid
        self.uuid = uuid
        self.id = id_
        self.type = type_

    @classmethod
    def from_dict(cls, data: dict):
        instrument_data = data.get("instrument", {})
        return cls(
            pid=data["pid"],
            uuid=data["uuid"],
            id_=instrument_data["id"],
            type_=instrument_data["type"],
        )

    def __repr__(self):
        return (
            f"Instrument(pid={self.pid!r}, uuid={self.uuid!r}, "
            f"id={self.id!r}, type={self.type!r})"
        )

