# based on https://github.com/plausible/clickhouse_ecto/blob/master/lib/clickhouse_ecto/query.ex

defmodule Ecto.Adapters.Choto.Connection do
  @moduledoc false
  @behaviour Ecto.Adapters.SQL.Connection

  alias Ecto.Query
  alias Ecto.Query.{BooleanExpr, JoinExpr, QueryExpr}

  @impl true
  def child_spec(options) do
    Choto.Connection.child_spec(options)
  end

  @impl true
  def query(connection, statement, _params, options) do
    with {:ok, _query, result} <-
           DBConnection.prepare_execute(
             connection,
             Choto.Connection.Query.new(statement),
             options
           ) do
      {:ok, result}
    end
  end

  @impl true
  def all(query) do
    sources = create_names(query)
    {select_distinct, order_by_distinct} = distinct(query.distinct, sources, query)

    from = from(query, sources)
    select = select(query, select_distinct, sources)
    join = join(query, sources)
    where = where(query, sources)
    group_by = group_by(query, sources)
    having = having(query, sources)
    order_by = order_by(query, order_by_distinct, sources)
    limit = limit(query, sources)
    offset = offset(query, sources)

    [select, from, join, where, group_by, having, order_by, limit, offset]
  end

  @impl true
  def insert(_prefix, _table, _header, _rows, _on_conflict, _returning, _placeholders) do
    # included_fields =
    #   header
    #   |> Enum.filter(fn value -> Enum.any?(rows, fn row -> value in row end) end)

    # included_rows =
    #   Enum.map(rows, fn row ->
    #     row
    #     |> Enum.zip(header)
    #     |> Enum.filter(fn {_row, col} -> col in included_fields end)
    #     |> Enum.map(fn {row, _col} -> row end)
    #   end)

    # fields = intersperse_map(included_fields, ?,, &quote_name/1)

    # query = [
    #   "INSERT INTO ",
    #   quote_table(prefix, table),
    #   " (",
    #   fields,
    #   ")",
    #   " VALUES ",
    #   insert_all(included_rows, 1)
    # ]

    # IO.iodata_to_binary(query)
    ""
  end

  @impl true
  def update(_prefix, _table, _fields, _filters, _returning) do
    raise "UPDATE is not supported"
  end

  @impl true
  def delete(_prefix, _table, _filters, _returning) do
    raise "DELETE is not supported"
  end

  @impl true
  def update_all(%{from: _from} = _query, _prefix \\ nil) do
    raise "UPDATE is not supported"
  end

  @impl true
  def delete_all(%{from: _from} = _query) do
    raise "DELETE is not supported"
  end

  binary_ops = [
    ==: " = ",
    !=: " != ",
    <=: " <= ",
    >=: " >= ",
    <: " < ",
    >: " > ",
    and: " AND ",
    or: " OR ",
    ilike: " ILIKE ",
    like: " LIKE ",
    in: " IN ",
    is_nil: " WHERE "
  ]

  @binary_ops Keyword.keys(binary_ops)

  Enum.map(binary_ops, fn {op, str} ->
    defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
  end)

  defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

  defp select(%Query{select: %{fields: fields}} = query, select_distinct, sources) do
    ["SELECT", select_distinct, ?\s | select_fields(fields, sources, query)]
  end

  defp select_fields([], _sources, _query), do: "'TRUE'"

  defp select_fields(fields, sources, query) do
    intersperse_map(fields, ", ", fn
      {key, value} ->
        [expr(value, sources, query), " AS " | quote_name(key)]

      value ->
        expr(value, sources, query)
    end)
  end

  defp distinct(nil, _, _), do: {[], []}
  defp distinct(%QueryExpr{expr: []}, _, _), do: {[], []}
  defp distinct(%QueryExpr{expr: true}, _, _), do: {" DISTINCT", []}
  defp distinct(%QueryExpr{expr: false}, _, _), do: {[], []}

  defp distinct(%QueryExpr{expr: _exprs}, _sources, query) do
    error!(
      query,
      "DISTINCT ON is not supported! Use `distinct: true`, for ex. `from rec in MyModel, distinct: true, select: rec.my_field`"
    )
  end

  defp from(%{from: %{source: source, hints: hints}} = query, sources) do
    {from, name} = get_source(query, sources, 0, source)
    [" FROM ", from, " AS ", name, hints(hints)]
  end

  defp join(%Query{joins: []}, _sources), do: []

  defp join(%Query{joins: joins} = query, sources) do
    [
      ?\s
      | intersperse_map(joins, ?\s, fn
          %JoinExpr{qual: qual, ix: ix, source: source, on: %QueryExpr{expr: on_expr}} ->
            {join, name} = get_source(query, sources, ix, source)
            [join_qual(qual), join, " AS ", name, on_join_expr(on_expr)]
        end)
    ]
  end

  defp on_join_expr({_, _, [head | tail]}) do
    retorno = [on_join_expr(head) | on_join_expr(tail)]
    retorno |> Enum.uniq() |> Enum.join(",")
  end

  defp on_join_expr([head | tail]) do
    [on_join_expr(head) | tail]
  end

  defp on_join_expr({{:., [], [{:&, [], _}, column]}, [], []}) when is_atom(column) do
    " USING " <> Atom.to_string(column)
  end

  defp on_join_expr({:==, _, [{{_, _, [_, column]}, _, _}, _]}) when is_atom(column) do
    " USING " <> Atom.to_string(column)
  end

  defp on_join_expr(true), do: ""

  defp join_qual(:inner), do: " INNER JOIN "
  defp join_qual(:inner_lateral), do: " ARRAY JOIN "
  defp join_qual(:cross), do: " CROSS JOIN "
  defp join_qual(:full), do: " FULL JOIN "
  defp join_qual(:left_lateral), do: " LEFT ARRAY JOIN "
  defp join_qual(:left), do: " LEFT OUTER JOIN "

  defp where(%Query{wheres: wheres} = query, sources) do
    boolean(" WHERE ", wheres, sources, query)
  end

  defp having(%Query{havings: havings} = query, sources) do
    boolean(" HAVING ", havings, sources, query)
  end

  defp group_by(%Query{group_bys: []}, _sources), do: []

  defp group_by(%Query{group_bys: group_bys} = query, sources) do
    [
      " GROUP BY "
      | intersperse_map(group_bys, ", ", fn
          %QueryExpr{expr: expr} ->
            intersperse_map(expr, ", ", &expr(&1, sources, query))
        end)
    ]
  end

  defp order_by(%Query{order_bys: []}, _distinct, _sources), do: []

  defp order_by(%Query{order_bys: order_bys} = query, distinct, sources) do
    order_bys = Enum.flat_map(order_bys, & &1.expr)

    [
      " ORDER BY "
      | intersperse_map(distinct ++ order_bys, ", ", &order_by_expr(&1, sources, query))
    ]
  end

  def order_by_expr({dir, expr}, sources, query) do
    str = expr(expr, sources, query)

    case dir do
      :asc -> str
      :desc -> [str | " DESC"]
    end
  end

  defp limit(%Query{limit: nil}, _sources), do: []

  defp limit(%Query{limit: %QueryExpr{expr: expr}} = query, sources) do
    [" LIMIT ", expr(expr, sources, query)]
  end

  defp offset(%{offset: nil}, _sources), do: []

  defp offset(%{offset: %QueryExpr{expr: expr}} = query, sources) do
    [" OFFSET " | expr(expr, sources, query)]
  end

  defp hints([_ | _] = hints) do
    hint_list =
      Enum.map(hints, &hint/1)
      |> Enum.intersperse(", ")

    [" ", hint_list]
  end

  defp hints([]), do: []

  defp hint(hint_str) when is_binary(hint_str), do: hint_str

  defp hint({key, val}) when is_atom(key) and is_integer(val),
    do: [Atom.to_string(key), " ", Integer.to_string(val)]

  defp boolean(_name, [], _sources, _query), do: []

  defp boolean(name, [%{expr: expr, op: op} | query_exprs], sources, query) do
    [
      name
      | Enum.reduce(query_exprs, {op, paren_expr(expr, sources, query)}, fn
          %BooleanExpr{expr: expr, op: op}, {op, acc} ->
            {op, [acc, operator_to_boolean(op), paren_expr(expr, sources, query)]}

          %BooleanExpr{expr: expr, op: op}, {_, acc} ->
            {op, [?(, acc, ?), operator_to_boolean(op), paren_expr(expr, sources, query)]}
        end)
        |> elem(1)
    ]
  end

  defp operator_to_boolean(:and), do: " AND "
  defp operator_to_boolean(:or), do: " OR "

  defp paren_expr(false, _sources, _query), do: "(0=1)"
  defp paren_expr(true, _sources, _query), do: "(1=1)"

  defp paren_expr(expr, sources, query) do
    [?(, expr(expr, sources, query), ?)]
  end

  defp expr({_type, [literal]}, sources, query) do
    expr(literal, sources, query)
  end

  defp expr({:^, [], [_ix]}, _sources, _query) do
    [??]
  end

  defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _query) when is_atom(field) do
    quote_qualified_name(field, sources, idx)
  end

  defp expr({:&, _, [idx, fields, _counter]}, sources, query) do
    {_, name, schema} = elem(sources, idx)

    if is_nil(schema) and is_nil(fields) do
      error!(
        query,
        "ClickHouse requires a schema module when using selector " <>
          "#{inspect(name)} but none was given. " <>
          "Please specify a schema or specify exactly which fields from " <>
          "#{inspect(name)} you desire"
      )
    end

    intersperse_map(fields, ", ", &[name, ?. | quote_name(&1)])
  end

  defp expr({:in, _, [_left, []]}, _sources, _query) do
    "0"
  end

  defp expr({:in, _, [left, right]}, sources, query) when is_list(right) do
    args = intersperse_map(right, ?,, &expr(&1, sources, query))
    [expr(left, sources, query), " IN (", args, ?)]
  end

  defp expr({:in, _, [_, {:^, _, [_, 0]}]}, _sources, _query) do
    "0"
  end

  defp expr({:in, _, [left, {:^, _, [_, length]}]}, sources, query) do
    args = Enum.intersperse(List.duplicate(??, length), ?,)
    [expr(left, sources, query), " IN (", args, ?)]
  end

  defp expr({:in, _, [left, right]}, sources, query) do
    [expr(left, sources, query), " = ANY(", expr(right, sources, query), ?)]
  end

  defp expr({:is_nil, _, [arg]}, sources, query) do
    [expr(arg, sources, query) | " IS NULL"]
  end

  defp expr({:not, _, [expr]}, sources, query) do
    case expr do
      {fun, _, _} when fun in @binary_ops ->
        ["NOT (", expr(expr, sources, query), ?)]

      _ ->
        ["~(", expr(expr, sources, query), ?)]
    end
  end

  defp expr(%Ecto.SubQuery{query: query, params: _params}, _sources, _query) do
    all(query)
  end

  defp expr({:fragment, _, [kw]}, sources, query) when is_list(kw) or tuple_size(kw) == 3 do
    Enum.reduce(kw, query, fn {key, {op, val}}, query ->
      expr({op, nil, [key, val]}, sources, query)
    end)
  end

  defp expr({:fragment, _, parts}, sources, query) do
    Enum.map(parts, fn
      {:raw, part} -> part
      {:expr, expr} -> expr(expr, sources, query)
    end)
  end

  defp expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
    {modifier, args} =
      case args do
        [rest, :distinct] -> {"DISTINCT ", [rest]}
        _ -> {[], args}
      end

    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args
        [op_to_binary(left, sources, query), op | op_to_binary(right, sources, query)]

      {:fun, fun} ->
        [fun, ?(, modifier, intersperse_map(args, ", ", &expr(&1, sources, query)), ?)]
    end
  end

  defp expr({:count, _, []}, _sources, _query), do: "count(*)"

  defp expr(list, sources, query) when is_list(list) do
    ["ARRAY[", intersperse_map(list, ?,, &expr(&1, sources, query)), ?]]
  end

  defp expr(%Decimal{} = decimal, _sources, _query) do
    Decimal.to_string(decimal, :normal)
  end

  defp expr(%Ecto.Query.Tagged{value: binary, type: :binary}, _sources, _query)
       when is_binary(binary) do
    ["0x", Base.encode16(binary, case: :lower)]
  end

  defp expr(%Ecto.Query.Tagged{value: other, type: type}, sources, query) do
    ["CAST(", expr(other, sources, query), " AS ", tagged_to_db(type), ")"]
  end

  defp expr(nil, _sources, _query), do: "NULL"
  defp expr(true, _sources, _query), do: "1"
  defp expr(false, _sources, _query), do: "0"

  defp expr(literal, _sources, _query) when is_binary(literal) do
    [?\', escape_string(literal), ?\']
  end

  defp expr(literal, _sources, _query) when is_integer(literal) do
    Integer.to_string(literal)
  end

  defp expr(literal, _sources, _query) when is_float(literal) do
    Float.to_string(literal)
  end

  defp expr(literal, _sources, _query) when is_atom(literal) do
    Atom.to_string(literal)
  end

  # defp interval(count, _interval, sources, query) do
  #   [expr(count, sources, query)]
  # end

  defp op_to_binary({op, _, [_, _]} = expr, sources, query) when op in @binary_ops do
    paren_expr(expr, sources, query)
  end

  defp op_to_binary(expr, sources, query) do
    expr(expr, sources, query)
  end

  defp create_names(%{sources: sources}) do
    create_names(sources, 0, tuple_size(sources)) |> List.to_tuple()
  end

  defp create_names(_sources, pos, pos) do
    []
  end

  defp create_names(sources, pos, limit) when pos < limit do
    [create_name(sources, pos) | create_names(sources, pos + 1, limit)]
  end

  defp create_name(sources, pos) do
    case elem(sources, pos) do
      {:fragment, _, _} ->
        {nil, [?f | Integer.to_string(pos)], nil}

      {table, schema, prefix} ->
        name = [create_alias(table) | Integer.to_string(pos)]
        {quote_table(prefix, table), name, schema}

      %Ecto.SubQuery{} ->
        {nil, [?s | Integer.to_string(pos)], nil}
    end
  end

  defp create_alias(<<first, _rest::binary>>) when first in ?a..?z when first in ?A..?Z do
    <<first>>
  end

  defp create_alias(_) do
    "t"
  end

  # helpers

  defp get_source(query, sources, ix, source) do
    {expr, name, _schema} = elem(sources, ix)
    {expr || paren_expr(source, sources, query), name}
  end

  defp quote_qualified_name(name, sources, ix) do
    {_, source, _} = elem(sources, ix)
    [source, ?. | quote_name(name)]
  end

  defp quote_name(name, quoter \\ ?")
  defp quote_name(nil, _), do: []

  defp quote_name(names, quoter) when is_list(names) do
    names
    |> Enum.filter(&(not is_nil(&1)))
    |> intersperse_map(?., &quote_name(&1, nil))
    |> wrap_in(quoter)
  end

  defp quote_name(name, quoter) when is_atom(name) do
    quote_name(Atom.to_string(name), quoter)
  end

  defp quote_name(name, quoter) do
    if String.contains?(name, "\"") do
      error!(nil, "bad name #{inspect(name)}")
    end

    wrap_in(name, quoter)
  end

  defp wrap_in(value, nil), do: value

  defp wrap_in(value, {left_wrapper, right_wrapper}) do
    [left_wrapper, value, right_wrapper]
  end

  defp wrap_in(value, wrapper) do
    [wrapper, value, wrapper]
  end

  defp quote_table(prefix, name)
  defp quote_table(nil, name), do: quote_name(name)
  defp quote_table(prefix, name), do: intersperse_map([prefix, name], ?., &quote_name/1)

  defp intersperse_map(list, separator, mapper, acc \\ [])

  defp intersperse_map([elem], _separator, mapper, acc), do: [acc | mapper.(elem)]

  defp intersperse_map([elem | rest], separator, mapper, acc) do
    intersperse_map(rest, separator, mapper, [acc, mapper.(elem), separator])
  end

  defp intersperse_map([], _separator, _mapper, acc), do: acc

  defp escape_string(value) when is_binary(value) do
    :binary.replace(value, "'", "''", [:global])
  end

  defp tagged_to_db(:integer), do: "Int64"
  defp tagged_to_db(other), do: ecto_to_db(other)

  defp ecto_to_db({:array, t}), do: "Array(#{ecto_to_db(t)})"

  defp ecto_to_db({:nested, types}) do
    fields =
      Tuple.to_list(types)
      |> Enum.map(fn {fieldname, fieldtype} ->
        Atom.to_string(fieldname) <> " " <> ecto_to_db(fieldtype)
      end)
      |> Enum.join(", ")

    "Nested(#{fields})"
  end

  defp ecto_to_db(:id), do: "UInt32"
  defp ecto_to_db(:binary_id), do: "FixedString(36)"
  defp ecto_to_db(:uuid), do: "FixedString(36)"
  defp ecto_to_db(:string), do: "String"
  defp ecto_to_db(:binary), do: "FixedString(4000)"
  defp ecto_to_db(:integer), do: "Int32"
  defp ecto_to_db(:bigint), do: "Int64"
  defp ecto_to_db(:float), do: "Float32"
  defp ecto_to_db(:decimal), do: "Float64"
  defp ecto_to_db(:boolean), do: "UInt8"
  defp ecto_to_db(:date), do: "Date"
  defp ecto_to_db(:utc_datetime), do: "DateTime"
  defp ecto_to_db(:naive_datetime), do: "DateTime"
  defp ecto_to_db(:timestamp), do: "DateTime"
  defp ecto_to_db(other), do: Atom.to_string(other)

  defp error!(query, message) do
    raise Ecto.QueryError, query: query, message: message
  end
end
