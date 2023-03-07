from typing import TypeAlias

Json: TypeAlias = bool | float | int | None | str | dict[str, 'Json'] | list['Json']
JsonList: TypeAlias = list[Json]
JsonObject: TypeAlias = dict[str, Json]
