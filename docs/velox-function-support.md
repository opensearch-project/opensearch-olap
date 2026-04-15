# Velox Function Support Matrix

This document lists which PPL built-in functions are supported by the Velox vectorized execution engine. Functions marked as supported are executed natively in Velox C++. Unsupported functions cause the query to fall back to the default OpenSearch execution engine.

## How it works

When a PPL query arrives, `canVectorize()` checks:
1. **RelNode types** — all operators (scan, filter, project, join, sort, aggregate, window) must be supported
2. **Field types** — all columns in the scan must have Velox-compatible types (no `ANY`, no `MAP<VARCHAR, ANY>`)
3. **Functions** — all RexCall expressions must use supported functions (see tables below)

If any check fails, the entire query falls back to the default engine (no partial vectorization).

### `force_vectorize` setting

The dynamic setting `plugins.velox.force_vectorize` (default `false`) bypasses all `canVectorize()` checks. When enabled, all queries go through Velox — unsupported types or functions cause explicit errors instead of silent fallback. Used in integration tests to verify Velox coverage.

## Known Limitations

### Nested object fields (MAP type)

OpenSearch `object` fields (e.g., `cloud`, `aws.cloudwatch`, `agent`) are mapped by the SQL plugin's Calcite schema as `MAP<VARCHAR, ANY>`. Velox cannot handle the `ANY` value type, so **any query scanning a table with nested object fields falls back to the default engine**.

This is the primary reason the Big5 benchmark queries fall back — all 58 queries scan the `big5` table which has nested objects like `cloud.region`, `aws.cloudwatch.log_stream`, etc.

**Workaround:** Use flat index mappings with only top-level keyword/numeric/date fields (no nested objects).

**Fix plan:**
- Short-term: Flatten the Calcite schema for object fields in `ClusterCopyShuttle` — convert `MAP<VARCHAR, ANY>` to individual typed columns using the index mapping
- Long-term: Support MAP type in VeloxTypeConverter + LuceneArrowReader by reading nested doc values with dot-path field names

### Fields without doc values

The Velox engine reads data from Lucene doc values only. Field types without doc values (`text`, `match_only_text`) cannot be read by `LuceneArrowReader`. Queries scanning these columns will get null/empty values or crash.

**Affected types:** `text` (no doc values by default), `match_only_text` (never has doc values)

**Workaround:** Use `keyword` type instead of `text` for fields that need vectorized execution. Or add `| fields` to PPL queries to project only supported columns.

## Mathematical Functions

| Function | Velox Support | Velox Name | Notes |
|----------|:---:|---|---|
| ABS | Yes | `abs` | |
| ADD (+) | Yes | `plus` | |
| SUBTRACT (-) | Yes | `minus` | |
| MULTIPLY (*) | Yes | `multiply` | |
| DIVIDE (/) | Yes | `divide` | |
| MOD (%) | Yes | `modulus` | |
| CEIL / CEILING | Yes | `ceil` | |
| FLOOR | Yes | `floor` | |
| ROUND | Yes | `round` | |
| SIGN / SIGNUM | Yes | `sign` | |
| SQRT | Yes | `sqrt` | |
| CBRT | Yes | `cbrt` | |
| POW / POWER | Yes | `power` | |
| EXP | Yes | `exp` | |
| LN | Yes | `ln` | |
| LOG | Yes | `ln` | Maps to natural log |
| LOG2 | Yes | `log2` | |
| LOG10 | Yes | `log10` | |
| PI | Yes | `pi` | |
| E | Yes | `e` | |
| RAND | Yes | `rand` | |
| TRUNCATE | Yes | `truncate` | |
| EXPM1 | No | | |
| CONV | No | | Base conversion |
| CRC32 | Yes | `crc32` | |
| RINT | No | | |
| SINH | No | | Velox has no `sinh` |

## Trigonometric Functions

| Function | Velox Support | Velox Name | Notes |
|----------|:---:|---|---|
| COS | Yes | `cos` | |
| SIN | Yes | `sin` | |
| TAN | Yes | `tan` | |
| ACOS | Yes | `acos` | |
| ASIN | Yes | `asin` | |
| ATAN | Yes | `atan` | |
| ATAN2 | Yes | `atan2` | |
| COSH | Yes | `cosh` | |
| COT | Yes | `cot` | |
| RADIANS | Yes | `radians` | |
| DEGREES | Yes | `degrees` | |

## String Functions

| Function | Velox Support | Velox Name | Notes |
|----------|:---:|---|---|
| UPPER | Yes | `upper` | |
| LOWER | Yes | `lower` | |
| LENGTH | Yes | `length` | Via Calcite CHAR_LENGTH |
| CONCAT | Yes | `concat` | |
| CONCAT_WS | Yes | `concat_ws` | |
| SUBSTRING / SUBSTR | Yes | `substring` | |
| REVERSE | Yes | `reverse` | |
| REPLACE | Yes | `replace` | |
| TRIM | Yes | `trim` | |
| LTRIM | Yes | `ltrim` | Via TRIM(LEADING) |
| RTRIM | Yes | `rtrim` | Via TRIM(TRAILING) |
| LIKE | Yes | `like` | |
| LPAD | Yes | `lpad` | |
| RPAD | Yes | `rpad` | |
| LOCATE / POSITION | Yes | `strpos` | |
| ASCII | Yes | `codepoint` | |
| RIGHT | No | | Needs argument rewrite to `substr` |
| LEFT | No | | Needs argument rewrite to `substr` |
| ILIKE | Partial | `like` | Case-insensitive; needs lower() wrapping |
| STRCMP | No | | |
| REGEXP_REPLACE | No | | Velox has `regexp_replace` but signature differs |
| REGEXP_MATCH | No | | |

## Conditional Functions

| Function | Velox Support | Velox Name | Notes |
|----------|:---:|---|---|
| IF | Yes | `switch` | Calcite converts to CASE → Velox switch |
| CASE | Yes | `switch` | |
| COALESCE | Yes | `coalesce` | |
| IFNULL | Yes | `coalesce` | Calcite converts to COALESCE |
| NULLIF | Yes | `switch` | Calcite converts to CASE |
| ISNULL / IS_NULL | Yes | `is_null` | |
| ISNOTNULL / IS_NOT_NULL | Yes | `not(is_null)` | |
| ISPRESENT | Yes | `not(is_null)` | Calcite converts to IS NOT NULL |
| ISEMPTY | No | | Requires string length check |
| ISBLANK | No | | Requires trim + length check |
| EARLIEST (condition) | No | | OpenSearch-specific |
| LATEST (condition) | No | | OpenSearch-specific |
| CONTAINS | No | | OpenSearch-specific |

## Type Conversion

| Function | Velox Support | Velox Name | Notes |
|----------|:---:|---|---|
| CAST | Yes | native CastTypedExpr | |
| TOSTRING | No | | |
| TONUMBER | No | | |

## Aggregation Functions

| Function | Velox Support | Velox Name | Notes |
|----------|:---:|---|---|
| COUNT | Yes | `count` | |
| SUM | Yes | `sum` | |
| AVG | Yes | `avg` | |
| MIN | Yes | `min` | |
| MAX | Yes | `max` | |
| STDDEV_SAMP | No | | Available in Velox; not yet wired |
| STDDEV_POP | No | | Available in Velox; not yet wired |
| VAR_SAMP | No | | Available in Velox; not yet wired |
| VAR_POP | No | | Available in Velox; not yet wired |
| PERCENTILE_APPROX | No | | Available in Velox as `approx_percentile` |
| MEDIAN | No | | |
| DISTINCT_COUNT | No | | |
| EARLIEST (agg) | No | | |
| LATEST (agg) | No | | |
| FIRST | No | | |
| LAST | No | | |
| TAKE | No | | |
| LIST | No | | |
| VALUES | No | | |

## Cryptographic Functions

| Function | Velox Support | Velox Name | Notes |
|----------|:---:|---|---|
| MD5 | Yes | `md5` | |
| SHA1 | Yes | `sha1` | |
| SHA2 | No | | Velox has `sha256`, `sha512` but PPL SHA2 takes a bit-length arg |

## Date/Time Functions

| Function | Velox Support | Velox Name | Notes |
|----------|:---:|---|---|
| YEAR | Yes | `year` | |
| MONTH | Yes | `month` | |
| DAY / DAYOFMONTH | Yes | `day` | |
| HOUR | Yes | `hour` | |
| MINUTE | Yes | `minute` | |
| SECOND | Yes | `second` | |
| QUARTER | Yes | `quarter` | |
| WEEK / WEEKOFYEAR | Yes | `week` | |
| DAYOFWEEK | Yes | `dow` | |
| DAYOFYEAR | Yes | `doy` | |
| NOW / CURRENT_TIMESTAMP | Yes | `now` | |
| CURRENT_DATE / CURDATE | Yes | `current_date` | |
| CURRENT_TIME / CURTIME | Yes | `current_time` | |
| DATE_ADD | Yes | `date_add` | |
| DATE_SUB | No | | Needs interval negation rewrite |
| DATEDIFF | Yes | `date_diff` | |
| DATE_FORMAT | Yes | `date_format` | |
| DATE_TRUNC | Yes | `date_trunc` | |
| FROM_UNIXTIME | Yes | `from_unixtime` | |
| UNIX_TIMESTAMP | Yes | `to_unixtime` | |
| LAST_DAY | Yes | `last_day_of_month` | |
| ADDDATE | No | | |
| ADDTIME | No | | |
| SUBDATE | No | | |
| SUBTIME | No | | |
| TIMEDIFF | No | | |
| TIMESTAMPADD | No | | |
| TIMESTAMPDIFF | No | | |
| CONVERT_TZ | No | | |
| DAYNAME | No | | |
| MONTHNAME | No | | |
| FROM_DAYS | No | | |
| GET_FORMAT | No | | |
| MAKEDATE | No | | |
| MAKETIME | No | | |
| PERIOD_ADD | No | | |
| PERIOD_DIFF | No | | |
| SEC_TO_TIME | No | | |
| STR_TO_DATE | No | | |
| TIME_FORMAT | No | | |
| TIME_TO_SEC | No | | |
| TO_DAYS | No | | |
| TO_SECONDS | No | | |
| UTC_DATE | No | | |
| UTC_TIME | No | | |
| UTC_TIMESTAMP | No | | |
| YEARWEEK | No | | |
| WEEKDAY | No | | |
| SYSDATE | No | | |
| STRFTIME | No | | |
| EXTRACT | No | | Needs special EXTRACT handling |

## Collection Functions

| Function | Velox Support | Notes |
|----------|:---:|---|
| ARRAY | No | Complex type |
| ARRAY_LENGTH | No | Complex type |
| FORALL | No | Lambda function |
| EXISTS | No | Lambda function |
| FILTER | No | Lambda function |
| TRANSFORM | No | Lambda function |
| REDUCE | No | Lambda function |
| SPLIT | No | |
| MVAPPEND | No | |
| MVDEDUP | No | |
| MVFIND | No | |
| MVINDEX | No | |
| MVJOIN | No | |
| MVMAP | No | Lambda function |
| MVZIP | No | |

## JSON Functions

| Function | Velox Support | Notes |
|----------|:---:|---|
| JSON | No | |
| JSON_VALID | No | |
| JSON_OBJECT | No | |
| JSON_ARRAY | No | |
| JSON_ARRAY_LENGTH | No | |
| JSON_EXTRACT | No | |
| JSON_DELETE | No | |
| JSON_SET | No | |
| JSON_APPEND | No | |
| JSON_EXTEND | No | |
| JSON_KEYS | No | |

## Relevance Functions (OpenSearch-specific)

| Function | Velox Support | Notes |
|----------|:---:|---|
| MATCH | No | Lucene search function |
| MATCH_PHRASE | No | Lucene search function |
| MATCH_PHRASE_PREFIX | No | Lucene search function |
| MULTI_MATCH | No | Lucene search function |
| SIMPLE_QUERY_STRING | No | Lucene search function |
| MATCH_BOOL_PREFIX | No | Lucene search function |
| QUERY_STRING | No | Lucene search function |

## IP Functions

| Function | Velox Support | Notes |
|----------|:---:|---|
| CIDRMATCH | No | OpenSearch-specific |
| GEOIP | No | OpenSearch-specific |

## System Functions

| Function | Velox Support | Notes |
|----------|:---:|---|
| TYPEOF | No | |

## Summary

| Category | Supported | Total | Coverage |
|----------|-----------|-------|----------|
| Math | 27 | 30 | 90% |
| Trigonometric | 11 | 11 | 100% |
| String | 14 | 22 | 64% |
| Conditional | 9 | 14 | 64% |
| Aggregation | 5 | 18 | 28% |
| Cryptographic | 2 | 3 | 67% |
| Date/Time | 20 | 58 | 34% |
| Type Conversion | 1 | 3 | 33% |
| Collection | 0 | 15 | 0% |
| JSON | 0 | 11 | 0% |
| Relevance | 0 | 7 | 0% |
| IP | 0 | 2 | 0% |
| System | 0 | 1 | 0% |
| **Total** | **89** | **195** | **46%** |

## Type Coercion

Velox requires exact type matching for all function calls. The OLAP plugin automatically inserts CAST operations when:

- **Binary operators** (arithmetic, comparison): If operand types differ (e.g., `salary - 100000` produces `DOUBLE - INTEGER`), the narrower type is cast to the wider type.
- **Math/trig functions**: If an integer argument is passed to a function registered only for DOUBLE (e.g., `abs(dept_id)`), the argument is cast to DOUBLE.

This is transparent to the user — no explicit CAST is needed in PPL queries.
