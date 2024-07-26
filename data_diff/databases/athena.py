from functools import partial
import re
from typing import Any, ClassVar, Type

import attrs

from data_diff.schema import RawColumnInfo
from data_diff.utils import match_regexps

from data_diff.abcs.database_types import (
    Timestamp,
    TimestampTZ,
    Integer,
    Float,
    Text,
    FractionalType,
    DbPath,
    DbTime,
    Decimal,
    ColType,
    ColType_UUID,
    TemporalType,
    Boolean,
    JSON,
    Array,
    Struct,
)
from data_diff.databases.base import (
    BaseDialect,
    Database,
    import_helper,
    ThreadLocalInterpreter,
)
from data_diff.databases.base import (
    MD5_HEXDIGITS,
    CHECKSUM_HEXDIGITS,
    CHECKSUM_OFFSET,
    TIMESTAMP_PRECISION_POS,
)


def query_cursor(c, sql_code):
    c.execute(sql_code)
    if sql_code.lower().startswith("select"):
        return c.fetchall()
    # Required for the query to actually run 🤯
    if re.match(r"(insert|create|truncate|drop|explain)", sql_code, re.IGNORECASE):
        return c.fetchone()


@import_helper("athena")
def import_athena():
    import pyathena

    return pyathena


class Dialect(BaseDialect):
    name = "Athena"
    ROUNDS_ON_PREC_LOSS = True
    TYPE_CLASSES = {
        # Timestamps
        "timestamp with time zone": TimestampTZ,
        "timestamp without time zone": Timestamp,
        "timestamp": Timestamp,
        # Numbers
        "integer": Integer,
        "bigint": Integer,
        "real": Float,
        "double": Float,
        "decimal": Decimal,
        # Text
        "varchar": Text,
        "string": Text,
        # Boolean
        "boolean": Boolean,
        # Other
        "json": JSON,
        "array": Array,
        "struct": Struct,
    }

    def explain_as_text(self, query: str) -> str:
        return f"EXPLAIN (FORMAT TEXT) {query}"

    def type_repr(self, t) -> str:
        if isinstance(t, TimestampTZ):
            return "timestamp with time zone"

        try:
            return {float: "REAL"}[t]
        except KeyError:
            return super().type_repr(t)

    def timestamp_value(self, t: DbTime) -> str:
        return f"timestamp '{t.isoformat(' ')}'"

    def quote(self, s: str):
        return f'"{s}"'

    def to_string(self, s: str):
        return f"cast({s} as varchar)"

    def to_comparable(self, value: str, coltype: ColType) -> str:
        """Ensure that the expression is comparable in ``IS DISTINCT FROM``."""
        if isinstance(coltype, (JSON, Array, Struct)):
            return self.normalize_value_by_type(value, coltype)
        else:
            return super().to_comparable(value, coltype)

    def parse_type(self, table_path: DbPath, info: RawColumnInfo) -> ColType:
        timestamp_regexps = {
            r"timestamp\((\d)\)": Timestamp,
            r"timestamp\((\d)\) with time zone": TimestampTZ,
        }
        for m, t_cls in match_regexps(timestamp_regexps, info.data_type):
            precision = int(m.group(1))
            return t_cls(precision=precision, rounds=self.ROUNDS_ON_PREC_LOSS)

        number_regexps = {r"decimal\((\d+),(\d+)\)": Decimal}
        for m, n_cls in match_regexps(number_regexps, info.data_type):
            _prec, scale = map(int, m.groups())
            return n_cls(scale)

        array_regexps = {r"array\((.+)\)": Array}
        for m, n_cls in match_regexps(array_regexps, info.data_type):
            item_info = attrs.evolve(info, data_type=m.group(1))
            item_type = self.parse_type(table_path, item_info)
            col_type = Array(item_type=item_type)
            return col_type

        struct_regexps = {r"struct<(.+)>": Struct}
        for m, n_cls in match_regexps(struct_regexps, info.data_type):
            fields = [f.split(":") for f in m.group(1).split(",")]
            col_type = Struct(fields=fields)
            return col_type

        string_regexps = {r"varchar\((\d+)\)": Text, r"char\((\d+)\)": Text}
        for m, n_cls in match_regexps(string_regexps, info.data_type):
            return n_cls()

        return super().parse_type(table_path, info)

    def set_timezone_to_utc(self) -> str:
        return "SELECT 'UTC'"

    def current_timestamp(self) -> str:
        return "current_timestamp"

    def md5_as_int(self, s: str) -> str:
        return f"cast(from_base(substr(to_hex(md5(to_utf8({s}))), {1+MD5_HEXDIGITS-CHECKSUM_HEXDIGITS}), 16) as decimal(38, 0)) - {CHECKSUM_OFFSET}"

    def md5_as_hex(self, s: str) -> str:
        return f"to_hex(md5(to_utf8({s})))"

    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        # Trim doesn't work on CHAR type
        return f"TRIM(CAST({value} AS VARCHAR))"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        # TODO rounds
        if coltype.rounds:
            s = f"date_format(cast({value} as timestamp(6)), '%Y-%m-%d %H:%i:%S.%f')"
        else:
            s = f"date_format(cast({value} as timestamp(6)), '%Y-%m-%d %H:%i:%S.%f')"

        return f"RPAD(RPAD({s}, {TIMESTAMP_PRECISION_POS+coltype.precision}, '.'), {TIMESTAMP_PRECISION_POS+6}, '0')"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        return self.to_string(f"cast({value} as decimal(38,{coltype.precision}))")

    def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
        return self.to_string(f"cast ({value} as int)")

    def normalize_json(self, value: str, _coltype: JSON) -> str:
        return f"json_format(cast({value} as json))"

    def normalize_array(self, value: str, _coltype: Array) -> str:
        return self.normalize_json(value, _coltype)

    def normalize_struct(self, value: str, _coltype: Struct) -> str:
        return self.normalize_json(value, _coltype)


@attrs.define(frozen=False, init=False, kw_only=True)
class Athena(Database):
    DIALECT_CLASS: ClassVar[Type[BaseDialect]] = Dialect
    CONNECT_URI_HELP = "pyathena://<project>/<dataset>"
    CONNECT_URI_PARAMS = ["aws_profile_name", "s3_staging_dir", "region_name", "work_group"]

    _conn: Any

    def __init__(self, **kw) -> None:
        super().__init__()
        self.default_schema = "public"
        athenadb = import_athena()

        if kw.get("schema"):
            self.default_schema = kw.get("schema")

        aws_profile_name = kw.get("aws_profile_name")
        s3_staging_dir = kw.get("s3_staging_dir")
        region_name = kw.get("region_name")
        work_group = kw.get("work_group")

        self._conn = athenadb.connect(
            profile_name=aws_profile_name,
            s3_staging_dir=s3_staging_dir,
            region_name=region_name,
            work_group=work_group,
        )

    def _query(self, sql_code: str) -> list:
        "Uses the standard SQL cursor interface"
        c = self._conn.cursor()

        if isinstance(sql_code, ThreadLocalInterpreter):
            return sql_code.apply_queries(partial(query_cursor, c))

        return query_cursor(c, sql_code)

    def _normalize_table_path(self, path: DbPath) -> DbPath:
        if len(path) == 1:
            return self.default_schema, path[0]
        elif len(path) == 2:
            return path
        elif len(path) > 2:
            return path[1], path[2]

        raise ValueError(f"{self.name}: Bad table path for {self}: '{'.'.join(path)}'. Expected form: schema.table")

    def close(self):
        super().close()
        self._conn.close()

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return (
            "SELECT column_name, data_type, 3 as datetime_precision, 3 as numeric_precision, NULL as numeric_scale "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )

    @property
    def is_autocommit(self) -> bool:
        return False
