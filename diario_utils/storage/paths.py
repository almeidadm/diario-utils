from datetime import date
from typing import Literal


def date_partition(
    d: date,
    level: Literal["year", "month", "day"] = "day",
) -> str:
    if level == "year":
        return f"year={d.year}"
    if level == "month":
        return f"year={d.year}/month={d.month:02d}"
    return f"year={d.year}/month={d.month:02d}/day={d.day:02d}"
