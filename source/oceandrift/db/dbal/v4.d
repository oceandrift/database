/++
    SQL Query Builder

    Construct SQL queries from code.
    Compile them to any specific SQL dialect later.

    Works at compile-time thanks to $(B CTFE).
    Best used with $(B UFCS).

    ---
    table("person").qb
        .where("age", '>')
        .select("*")
    ;
    // is equivalent to writing this query by hand
    // --> SELECT * FROM "person" WHERE "age" > ?
    ---

    [table] creates a struct instance representing a table; this is the base for query building.

    [qb] instantiates a complex query builder (for SELECT, UPDATE or DELETE queries – those that can have WHERE clauses etc.)

    [where] appends a new condition to a query’s WHERE clause.
    For disjunctions (“OR”) the first template parameter is set to `or (i.e. `where!or(…)`)
    whereas `where!and` is the default.

    Parentheses `()` can be inserted by calling [whereParentheses]:

    ---
    table("person").qb
        .whereParentheses(q => q
            .where("age", '>')
            .where!or("height", '>')
        )
        .where("gender", '=')
        .select("*")
    ;
    // --> SELECT * FROM "person" WHERE ( "age" > ? OR "height" > ? ) AND "gender" = ?
    ---

    Applying aggregate functions is super simple as well:
    Helper functions [avg], [count], [max], [min], [sum] and [group_concat] got you covered!

    `DISTINCT` can be applied by setting the first (and only) template parameter of said functions to `distinct` (or `true`),
    i.e. `SELECT COUNT(DISTINCT "name")` becomes `.select(count!distinct("name"))`.

    ---
    table("person").qb              // FROM "person"
        .where("age", '>')          // WHERE "age" > ?
        .select(                    // SELECT
            count!distinct("name")  //      COUNT(DISTINCT "name")
        )
    ;
    ---

    Selecting multiple columns? No big deal either:

    ---
    table("person").qb.select(
        "name",
        "age"
    );
    // --> SELECT "name", "age" FROM "person"
    ---

    Writing `.select(*)` is tedious? How about omitting the parameter?
    `"*"` is the default:`

    ---
    table("person").qb.select();
    // --> SELECT * FROM "person"
    ---

    $(SIDEBAR
        I am not sure whether it’s reasonable to have a default value here.
        It might be better to just `static assert` instead.
        So: Dear users, please let me know what you think!
    )

    Pre-set values during query building.
    Those values are not inserted into the query string (Regular dynamic parameters are used as well.)
    but will be bound to the prepared statement later.

    ---
    table("person").qb
        .where("age", '>', 18)
        .select("*")
    // --> SELECT * FROM person WHERE "age" > ?
    ---

    Talking about “later”, it’s time to build an actual query from those abstractations…
    This is done by calling [build] on the [Query].
    The target dialect is specified through a template parameter.

    ---
    BuiltQuery q = table("person").qb
        .where("age", '>')
        .select("*")
        .build!SQLite3()
    ;
    writeln(q.sql);
    ---

    $(NOTE
        The primary goal of this query builder implementation is to power an ORM.
        Nevertheless, it’s designed to be used by actual human programmers.

        It doesn’t support every possible SQL feature.
        The goal is to support the subset needed for ORM purposes.

        I’d rather have limitations that affect versatility than to give up usability.
        Feedback is always welcome!
    )

    Codename: v4 (“version 4”)

    Special thanks to Steven “schveiguy” Schveighoffer.
 +/
module oceandrift.db.dbal.v4;

import std.conv : to;
import std.traits : ReturnType;
import std.typecons : Nullable;

import oceandrift.db.dbal.driver;

@safe:

/++
    Prepares a built query using the specified database connection
 +/
Statement prepareBuiltQuery(DatabaseDriver)(DatabaseDriver db, BuiltQuery builtQuery)
{
    Statement stmt = db.prepare(builtQuery.sql);

    foreach (index, value; builtQuery.preSets.where)
        stmt.bindDBValue(index, value);

    if (!builtQuery.preSets.limit.isNull)
        stmt.bind(cast(int) builtQuery.placeholders.where + 0, builtQuery.preSets.limit.get);

    if (!builtQuery.preSets.limitOffset.isNull)
        stmt.bind(cast(int) builtQuery.placeholders.where + 1, builtQuery.preSets.limitOffset.get);

    return stmt;
}

// there’s no “impure” keyword :(

pure:

///
enum ComparisonOperator : wchar
{
    invalid = '\0',

    equals = '=',
    notEquals = '≠',
    lessThan = '<',
    greaterThan = '>',
    lessThanOrEquals = '≤',
    greaterThanOrEquals = '≥',

    in_ = '∈',
    notIn = '∉',
    like = '≈',
    notLike = '≉',
    isNull = '0',
    isNotNull = '1', //
    // between = '∓',
}

// Logical operators
enum
{
    /// OR (logical operator, SQL)
    or = true,
    /// AND (logical operator, SQL)
    and = false,
}

enum
{
    /// NOT (SQL)
    not = false,
}

/++
    Abstract SQL query token
 +/
struct Token
{
    /++
        Meaning of the token
     +/
    Type type;

    /++
        Additional data (optional)
     +/
    Data data;

    /++
        Token types
     +/
    enum Type : char
    {
        invalid = '\xFF', /// garbage, apparently something is broken should you encounter this type in an actual token

        column = 'c',
        placeholder = '?',
        comparisonOperator = 'o', /// a [ComparisonOperator], for the actual operator see [Token.data.op]

        and = '&',
        or = '|',

        not = '!',

        leftParenthesis = '(',
        rightParenthesis = ')',
    }

    /++
        Token data
     +/
    union Data
    {
        string str;
        ComparisonOperator op;
    }
}

/++
    Database Table Representation
 +/
struct Table
{
    string name;
}

/++
    Convenience function to create a table instance
 +/
inout(Table) table(inout string name) nothrow @nogc
{
    return Table(name);
}

/++
    Abstract WHERE clause
 +/
struct Where
{
    Token[] tokens;
    DBValue[int] preSet; // Pre-set values provided during query building
private:
    int _placeholders = 0; // number of placeholders in tokens

public @safe pure nothrow @nogc:
    int placeholders() const
    {
        return _placeholders;
    }
}

struct OrderingTerm
{
    string column;
    bool desc = false;
}

struct Limit
{
    bool enabled = false;
    Nullable!ulong preSet;

    bool offsetEnabled = false;
    Nullable!ulong offsetPreSet;
}

/++
    SQL SELECT/UPDATE/DELETE query abstraction

    This is the secondary base type for query building.

    Not used with INSERT queries.

    ---
    Query myQuery = table("my_table").qb;

    Select myQuerySelectAll= myQuery.where(/* … */).select("*");
    BuiltQuery myQuerySelectAllBuilt = myQuerySelectAll.build!Database;
    ---
 +/
struct Query
{
    Table table;

private:
    Where _where;
    OrderingTerm[] _orderBy;
    Limit _limit;
}

/++
    Special query representation for use in query compilers
 +/
struct CompilerQuery
{
@safe pure nothrow @nogc:
    this(const Query q)
    {
        this.table = q.table;
        this.where = q._where;
        this.orderBy = q._orderBy;
        this.limit = q._limit;
    }

    const
    {
        /++
            Table to query
         +/
        Table table;

        /++
            WHERE clause
         +/
        Where where;

        /++
            ORDER BY clause
        +/
        OrderingTerm[] orderBy;

        /++
            LIMIT clause
         +/
        Limit limit;
    }
}

/++
    Returns: a complex query builder for the specified table
 +/
Query complexQueryBuilder(const Table table) nothrow @nogc
{
    return Query(table);
}

/// ditto
Query complexQueryBuilder(const string table) nothrow @nogc
{
    return Query(Table(table));
}

///
alias qb = complexQueryBuilder;

enum bool isComparisonOperator(T) = (
        is(T == ComparisonOperator)
            || is(T == wchar)
            || is(T == char)
    );

/++
    Appends a condition to the query's WHERE clause

    ---
    // …FROM mountain WHERE height > ?…
    Query qMountainsGreaterThan = table("mountain").qb.where("height", '>');

    // …FROM mountain WHERE height > ?…
    // Pre-sets the value `8000` for the dynamic paramter.
    Query qMountainsGreaterThan = table("mountain").qb.where("height", '>', 8000);

    // …FROM people WHERE ago > ? AND age < ?…
    // Pre-sets the values 60 and 100 for the dynamic paramters.
    Query qOver60butNot100yet = table("people").qb
        .where("age", '>', 60)
        .where("age", ComparisonOperator.lessThan, 100)
    ;
    ---
 +/
/// ditto
Query where(bool logicalJunction = and, TComparisonOperator)(
    Query q,
    string column,
    TComparisonOperator op
) if (isComparisonOperator!TComparisonOperator)
{
    enum Token tokenLogicalJunction =
        (logicalJunction == or) ? Token(Token.Type.or) : Token(Token.Type.and);

    if ((q._where.tokens.length > 0) && (q._where.tokens[$ - 1].type != Token.Type.leftParenthesis))
        q._where.tokens ~= tokenLogicalJunction;

    q._where.tokens ~= Token(Token.Type.column, Token.Data(column));

    auto dataOp = Token.Data();
    dataOp.op = cast(ComparisonOperator) op;
    q._where.tokens ~= Token(Token.Type.comparisonOperator, dataOp);

    if ((op != ComparisonOperator.isNull) && (op != ComparisonOperator.isNotNull))
    {
        q._where.tokens ~= Token(Token.Type.placeholder);
        ++q._where._placeholders;
    }

    return q;
}

/// ditto
Query where(bool logicalJunction = and, TComparisonOperator, T)(
    Query q,
    string column,
    TComparisonOperator op,
    T value
) @trusted // TODO: template constraint
{
    q._where.preSet[q._where.placeholders] = value;

    return q.where!logicalJunction(column, op);
}

/++
    Appends checks in parentheses to the query's WHERE clause
    
    ---
    // …FROM mountain WHERE height > ? AND ( country = ? OR country = ? )…
    Query qMountainsGreaterThanInUSorCA = table("mountain").qb
        .where("height", '>')
        .whereParentheses(q => q
            .where   ("location", '=', "US")
            .where!or("location", '=', "CA")
        )
    ;
    ---
 +/
Query whereParentheses(bool logicalJunction = and)(Query q, Query delegate(scope Query q) @safe pure conditions)
{
    enum Token tokenLogicalJunction =
        (logicalJunction == or) ? Token(Token.Type.or) : Token(Token.Type.and);

    if ((q._where.tokens.length > 0) && (q._where.tokens[$ - 1].type != Token.Type.leftParenthesis))
        q._where.tokens ~= tokenLogicalJunction;

    q._where.tokens ~= Token(Token.Type.leftParenthesis);
    q = conditions(q);
    q._where.tokens ~= Token(Token.Type.rightParenthesis);

    return q;
}

///
template whereNot(bool logicalJunction = or, TComparisonOperator)
        if (isComparisonOperator!TComparisonOperator)
{
    Query whereNot(Query q, string column, TComparisonOperator op)
    {
        q._where ~= Token(Token.Type.not);
        q._where ~= Token(Token.Type.leftParenthesis);
        q.where!(column, op);
        q._where ~= Token(Token.Type.rightParenthesis);
    }

    Query whereNot(Query q, string column, TComparisonOperator op, DBValue value)
    {
        q._where ~= Token(Token.Type.not);
        q._where ~= Token(Token.Type.leftParenthesis);
        q.where!(column, op, value);
        q._where ~= Token(Token.Type.rightParenthesis);
    }
}

Query orderBy(Query q, string column, bool desc = false)
{
    q._orderBy ~= OrderingTerm(column, desc);
    return q;
}

Query limit(bool withOffset = false)(Query q)
{
    q._limit.enabled = true;
    q._limit.preSet.nullify();

    q._limit.offsetEnabled = withOffset;
    q._limit.offsetPreSet.nullify();

    return q;
}

Query limit(Query q, ulong limit)
{
    q._limit.enabled = true;
    q._limit.preSet = limit;

    return q;
}

Query limit(Query q, ulong limit, ulong offset)
{
    q._limit.enabled = true;
    q._limit.preSet = limit;
    q._limit.offsetEnabled = true;
    q._limit.offsetPreSet = offset;

    return q;
}

// -- Select

enum AggregateFunction
{
    none = 0,
    avg,
    count,
    max,
    min,
    sum,
    group_concat,
}

struct SelectExpression
{
    string columnName;
    AggregateFunction aggregateFunction;
    bool distinct;
}

enum
{
    distinct = true,
}

SelectExpression avg(bool distinct = false)(string column)
{
    return SelectExpression(column, AggregateFunction.avg, distinct);
}

SelectExpression count(bool distinct = false)(string column = "*")
{
    return SelectExpression(column, AggregateFunction.count, distinct);
}

SelectExpression max(bool distinct = false)(string column)
{
    return SelectExpression(column, AggregateFunction.max, distinct);
}

SelectExpression min(bool distinct = false)(string column)
{
    return SelectExpression(column, AggregateFunction.min, distinct);
}

SelectExpression sum(bool distinct = false)(string column)
{
    return SelectExpression(column, AggregateFunction.sum, distinct);
}

SelectExpression group_concat(bool distinct = false)(string column)
{
    return SelectExpression(column, AggregateFunction.groupConcat, distinct);
}

struct Select
{
    Query query;
    const(SelectExpression)[] columns;
}

Select select(Column...)(Query from, Column columns)
{
    static if (columns.length == 0)
        return Select(from, [SelectExpression("*")]);
    else
    {
        auto data = new SelectExpression[](columns.length);

        static foreach (idx, col; columns)
        {
            static if (is(typeof(col) == string))
                data[idx] = SelectExpression(col);
            else static if (is(typeof(col) == SelectExpression))
                data[idx] = col;
            else
            {
                enum colType = typeof(col).stringof;
                static assert(
                    is(typeof(col) == string) || is(typeof(col) == SelectExpression),
                    "Column identifiers must be strings, but type of parameter " ~ idx.to!string ~ " is `" ~ colType ~ '`'
                );
            }
        }

        return Select(from, data);
    }
}

// -- Update

struct Update
{
    Query query;
    const(string)[] columns;
}

Update update(Query query, const(string)[] columns)
{
    return Update(query, columns);
}

Update update(Columns...)(Query query, Columns columns)
{
    auto data = new string[](columns.length);

    static foreach (idx, col; columns)
    {
        static if (is(typeof(col) == string))
            data[idx] = col;
        else
        {
            enum colType = typeof(col).stringof;
            static assert(
                is(typeof(col) == string) || is(typeof(col) == SelectExpression),
                "Column identifiers must be strings, but type of parameter " ~ idx.to!string ~ " is `" ~ colType ~ '`'
            );
        }
    }

    return query.update(data);
}

// -- Insert

struct Insert
{
    Table table;
    const(string)[] columns;
    uint rowCount = 1;
}

Insert insert(Table table, const(string)[] columns)
{
    return Insert(table, columns);
}

Insert insert(Columns...)(Table table, Columns columns)
{
    auto data = new string[](columns.length);

    static foreach (idx, col; columns)
    {
        static if (is(typeof(col) == string))
            data[idx] = col;
        else
        {
            enum colType = typeof(col).stringof;
            static assert(
                is(typeof(col) == string) || is(typeof(col) == SelectExpression),
                "Column identifiers must be strings, but type of parameter " ~ idx.to!string ~ " is `" ~ colType ~ '`'
            );
        }
    }

    return table.insert(data);
}

Insert times(Insert insert, const uint rows)
{
    insert.rowCount = rows;
    return insert;
}

// -- Delete

struct Delete
{
    Query query;
}

Delete delete_(Query query)
{
    return Delete(query);
}

// -- Query building

private struct _PlaceholdersMeta
{
    size_t where;
}

public alias PlaceholdersMeta = const(_PlaceholdersMeta);

private struct _PreSets
{
    DBValue[int] where;
    Nullable!ulong limit;
    Nullable!ulong limitOffset;
}

public alias PreSets = const(_PreSets);

private struct _BuiltQuery
{
    string sql;
    PlaceholdersMeta placeholders;
    PreSets preSets;
}

///
public alias BuiltQuery = const(_BuiltQuery);

/++
    Determines whether `T` is a valid SQL “Query Compiler” implementation
    
    ---
    struct MyDatabaseDriver
    {
    @safe pure:
        static BuiltQuery build(const Select selectQuery);
        static BuiltQuery build(const Update updateQuery);
        static BuiltQuery build(const Insert insertQuery);
        static BuiltQuery build(const Delete deleteQuery);
    }

    static assert(isQueryCompiler!MyDatabaseDriver);
    ---
 +/
enum bool isQueryCompiler(T) =
    // dfmt off
(
       is(ReturnType!(() => T.build(Select())) == BuiltQuery)
    && is(ReturnType!(() => T.build(Update())) == BuiltQuery)
    && is(ReturnType!(() => T.build(Insert())) == BuiltQuery)
    && is(ReturnType!(() => T.build(Delete())) == BuiltQuery)
);
// dfmt on

pragma(inline, true)
BuiltQuery build(QueryCompiler)(Select q) if (isQueryCompiler!QueryCompiler)
{
    return QueryCompiler.build(q);
}

pragma(inline, true)
BuiltQuery build(QueryCompiler)(Update q) if (isQueryCompiler!QueryCompiler)
{
    return QueryCompiler.build(q);
}

pragma(inline, true)
BuiltQuery build(QueryCompiler)(Insert q) if (isQueryCompiler!QueryCompiler)
{
    return QueryCompiler.build(q);
}

pragma(inline, true)
BuiltQuery build(QueryCompiler)(Delete q) if (isQueryCompiler!QueryCompiler)
{
    return QueryCompiler.build(q);
}
