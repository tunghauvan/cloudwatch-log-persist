from lark import Lark, Transformer, Tree, Token, v_args, Visitor

CWL_GRAMMAR = r"""
    ?start: query
    query: pipeline
    pipeline: command (_PIPE command)*
    _PIPE: WS? "|" WS?
    command: fields_cmd | filter_cmd | stats_cmd | sort_cmd | limit_cmd
    fields_cmd: "fields" field_list
    field_list: field ("," field)*
    field: FIELD_NAME | FUNC "(" ")" | FUNC "(" FIELD_NAME ")" ("as" FIELD_NAME)?
    filter_cmd: "filter" expr
    stats_cmd: "stats" stats_expr
    stats_expr: agg_func ("by" bin_expr)?
    agg_func: COUNT "(" ")" | "count" "(" "*" ")" | FUNC "(" FIELD_NAME ")"
    bin_expr: "bin" "(" NUMBER UNIT ")" | "bin" "(" NUMBER ")"
    sort_cmd: "sort" FIELD_NAME DIRECTION?
    limit_cmd: "limit" NUMBER
    ?expr: or_expr
    ?or_expr: and_expr | or_expr "or" and_expr
    ?and_expr: unary_expr | and_expr "and" unary_expr
    ?unary_expr: "not" unary_expr | comp_expr
    
    comp_expr: field_like_str
              | field_like_regex
              | field_regex
              | field_not_like_str
              | field_not_like_regex
              | field_in
              | field_cmp
              | "(" expr ")"
    
    field_like_str: FIELD_NAME "like" STRING
    field_like_regex: FIELD_NAME "like" REGEX_PATTERN
    field_regex: FIELD_NAME "=~" REGEX_PATTERN
    field_not_like_str: FIELD_NAME "not" "like" STRING
    field_not_like_regex: FIELD_NAME "not" "like" REGEX_PATTERN
    field_in: FIELD_NAME "in" "[" in_list "]"
    field_cmp: FIELD_NAME OP value
    
    in_list: value ("," value)*
    value: NUMBER | STRING | "true" | "false" | "null"
    OP: "=" | "!=" | "<" | ">" | "<=" | ">="
    DIRECTION: "asc" | "desc"
    FIELD_NAME: "@" /[a-zA-Z_][a-zA-Z0-9_]*/ | /[a-zA-Z_][a-zA-Z0-9_]*/
    FUNC: /count|avg|min|max|sum|abs|ceil|floor|log|sqrt|strlen|toupper|tolower|substr|strcontains|ispresent|isempty|isblank|coalesce|bin|now|fromMillis|toMillis|jsonParse|isValidIp|isValidIpV4|isValidIpV6|isIpInSubnet|isIpv4InSubnet|isIpv6InSubnet|replace|concat|ltrim|rtrim|trim|strcontains|isValidIp|isValidIpV4|isValidIpV6|isIpInSubnet|isIpv4InSubnet|isIpv6InSubnet|replace|concat|ltrim|rtrim|trim/
    REGEX_PATTERN: "/" /[^\/]*/ "/"
    NUMBER: /-?\d+(\.\d+)?/
    STRING: /\"[^\"]*\"|\'[^\']*\'/
    UNIT: "s" | "m" | "h" | "d" | "ms"
    COUNT: "count"
    %import common.WS
    %ignore WS
"""


def _token_value(t):
    if isinstance(t, Token):
        return t.value
    return str(t)


def _strip_quotes(s):
    if s and len(s) >= 2:
        if (s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'"):
            return s[1:-1]
    return s


def _transform_cond(node):
    if not isinstance(node, Tree):
        return node

    data = node.data

    if data == "field_like_str":
        field = _token_value(node.children[0])
        val = _token_value(node.children[1])
        pat = _strip_quotes(val)
        return ("like", field, pat)

    elif data == "field_like_regex":
        field = _token_value(node.children[0])
        pat = _token_value(node.children[1]).strip("/")
        return ("regex", field, pat)

    elif data == "field_regex":
        field = _token_value(node.children[0])
        pat = _token_value(node.children[1]).strip("/")
        return ("regex", field, pat)

    elif data == "field_not_like_str":
        field = _token_value(node.children[0])
        val = _token_value(node.children[1])
        pat = _strip_quotes(val)
        return ("not_like", field, pat)

    elif data == "field_not_like_regex":
        field = _token_value(node.children[0])
        pat = _token_value(node.children[1]).strip("/")
        return ("not_regex", field, pat)

    elif data == "field_cmp":
        field = _token_value(node.children[0])
        op = _token_value(node.children[1])
        val_node = node.children[2]
        val = (
            _token_value(val_node.children[0])
            if isinstance(val_node, Tree)
            else _token_value(val_node)
        )
        val = _strip_quotes(val)
        return ("cmp", field, op, val)

    elif data == "field_in":
        field = _token_value(node.children[0])
        in_list = node.children[1]
        vals = [
            _strip_quotes(_token_value(v.children[0]))
            if isinstance(v, Tree)
            else _strip_quotes(_token_value(v))
            for v in in_list.children
        ]
        return ("in", field, vals)

    elif data == "or_expr":
        return (
            "or",
            _transform_cond(node.children[0]),
            _transform_cond(node.children[1]),
        )

    elif data == "and_expr":
        return (
            "and",
            _transform_cond(node.children[0]),
            _transform_cond(node.children[1]),
        )

    elif data == "not_expr":
        return ("not", _transform_cond(node.children[0]))

    elif data in ("comp_expr", "unary_expr", "expr"):
        return _transform_cond(node.children[0])

    return node


def _extract_fields(field_list_node):
    if not isinstance(field_list_node, Tree):
        return []
    fields = []
    for child in field_list_node.children:
        if isinstance(child, Tree) and child.data == "field":
            fields.append(_token_value(child.children[0]))
        elif isinstance(child, Token):
            fields.append(_token_value(child))
    return fields


class CommandExtractor(Visitor):
    def __init__(self):
        self.fields = []
        self.filters = []
        self.sort_field = None
        self.sort_dir = "asc"
        self.limit = None
        self.stats_agg = None
        self.stats_group = None

    def fields_cmd(self, node):
        field_list = node.children[0]
        self.fields = _extract_fields(field_list)

    def filter_cmd(self, node):
        expr_node = node.children[0]
        cond = _transform_cond(expr_node)
        self.filters.append(cond)

    def sort_cmd(self, node):
        self.sort_field = _token_value(node.children[0])
        if len(node.children) > 1:
            dir_token = node.children[1]
            dir_val = _token_value(dir_token)
            if dir_val == "desc":
                self.sort_dir = "desc"
            else:
                self.sort_dir = "asc"

    def limit_cmd(self, node):
        self.limit = int(_token_value(node.children[0]))

    def stats_expr(self, node):
        agg_node = node.children[0]
        if isinstance(agg_node, Tree) and agg_node.data == "agg_func":
            if len(agg_node.children) == 1:
                self.stats_agg = ("count", "*")
            else:
                self.stats_agg = (
                    _token_value(agg_node.children[0]),
                    _token_value(agg_node.children[1]),
                )
        if len(node.children) > 1:
            bin_node = node.children[1]
            if isinstance(bin_node, Tree) and bin_node.data == "bin_expr":
                num = int(_token_value(bin_node.children[0]))
                unit = (
                    _token_value(bin_node.children[1])
                    if len(bin_node.children) > 1
                    else "h"
                )
                self.stats_group = ("bin", num, unit)


def _cond_to_sql(cond):
    if cond is None:
        return "1=1"

    if not isinstance(cond, (list, tuple)):
        return "1=1"

    op = cond[0]
    if op == "like":
        return f"{cond[1]} LIKE '%{cond[2]}%'"
    elif op == "regex":
        return f"{cond[1]} ~ '{cond[2]}'"
    elif op == "not_like":
        return f"{cond[1]} NOT LIKE '%{cond[2]}%'"
    elif op == "not_regex":
        return f"{cond[1]} !~ '{cond[2]}'"
    elif op == "cmp":
        val = cond[3]
        if val not in ("true", "false", "null"):
            if not val.replace(".", "").replace("-", "").isdigit():
                val = f"'{val}'"
        return f"{cond[1]} {cond[2]} {val}"
    elif op == "in":
        vals = ", ".join(f"'{str(v)}'" for v in cond[2])
        return f"{cond[1]} IN ({vals})"
    elif op == "and":
        return f"({_cond_to_sql(cond[1])} AND {_cond_to_sql(cond[2])})"
    elif op == "or":
        return f"({_cond_to_sql(cond[1])} OR {_cond_to_sql(cond[2])})"
    elif op == "not":
        return f"(NOT {_cond_to_sql(cond[1])})"
    return "1=1"


def parse_query(query_string: str) -> dict:
    parser = Lark(CWL_GRAMMAR, start="start")
    tree = parser.parse(query_string)

    extractor = CommandExtractor()
    extractor.visit(tree)

    where_parts = []
    for f in extractor.filters:
        where_parts.append(_cond_to_sql(f))

    where_sql = " AND ".join(where_parts) if where_parts else "1=1"

    select_fields = extractor.fields if extractor.fields else ["*"]

    sql_parts = ["SELECT", ", ".join(select_fields), "FROM logs"]
    if where_sql != "1=1":
        sql_parts.extend(["WHERE", where_sql])

    if extractor.sort_field:
        sql_parts.append(
            f"ORDER BY {extractor.sort_field} {extractor.sort_dir.upper()}"
        )

    if extractor.limit:
        sql_parts.append(f"LIMIT {extractor.limit}")

    sql = " ".join(sql_parts)

    return {
        "fields": extractor.fields,
        "filters": extractor.filters,
        "sort": (extractor.sort_field, extractor.sort_dir),
        "limit": extractor.limit,
        "stats": (extractor.stats_agg, extractor.stats_group),
        "sql": sql,
    }


if __name__ == "__main__":
    test_queries = [
        'fields @timestamp, @message | filter @message like "error" | sort @timestamp desc | limit 20',
        "fields @timestamp, @message | filter @message like /error/",
        'fields @timestamp, @message | filter @message = "test"',
        "fields @timestamp, @message | filter status >= 400 and status < 500",
        "stats count() by bin(1h)",
        'fields @timestamp, @message | filter @message not like "debug"',
        'fields @timestamp, @message | filter @message in ["error", "warn"]',
        "fields @timestamp, @message | filter @message like /(?i)error/",
        "fields @timestamp, @message | filter @message =~ /error/",
    ]

    for q in test_queries:
        try:
            result = parse_query(q)
            print(f"Query: {q}")
            print(f"  SQL: {result['sql']}")
            if result.get("stats") and result["stats"][0]:
                print(f"  Stats: {result['stats']}")
            print()
        except Exception as e:
            print(f"Query: {q}")
            print(f"  Error: {e}")
            import traceback

            traceback.print_exc()
            print()
