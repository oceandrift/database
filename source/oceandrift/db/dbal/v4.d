/++
    SQL Query Builder

    Construct SQL queries from code.

    ---
    table("misc").qb
        .where("id", ComparisonOperator.greaterThan)
            .where("age", '>')
        .select(
            "*",
            count!distinct("name")
        )
    ;
    // equivalent to writing
    ---

    Codename: v4 “version 4”

    Special thanks to Steven “schveiguy” Schveighoffer.
 +/
module oceandrift.db.dbal.v4;

import std.traits : ReturnType;
import std.typecons : Nullable;

import oceandrift.db.dbal.driver;

@safe:

/++
    Prepares a built query using the specified database connection
 +/
Statement prepareBuiltQuery(DatabaseDriver db, BuiltQuery builtQuery)
{
    Statement stmt = db.prepare(builtQuery.sql);

    foreach (index, value; builtQuery.preSet.where)
        stmt.bindDBValue(index + 1, value);

    if (!builtQuery.preSet.limit.isNull)
        stmt.bind(cast(int) builtQuery.wherePlaceholders + 1, builtQuery.preSet.limit.get);

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
    like = '≙',
    isNull = '0',
    isNotNull = '1', //between = '∓',
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
        comparisonOperator = 'o', /// a  [ComparisonOperator], for the actual operator see [Token.data.op]

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
    Pre-set values provided during query building

    They get automatically set when preparing a built query.
 +/
struct PreSet
{
    DBValue[int] where;
    Nullable!int limit;
}

/++
    Abstract WHERE clause
 +/
struct Where
{
    Token[] tokens;
    int placeholders = 0; // number of placeholders in tokens
}

/++
    SQL query abstraction

    This is the base type for query building.

    ---
    Query myQuery = table("my_table").qb;

    Select myQuerySelectAll= myQuery.where(/* … */).select("*");
    BuiltQuery myQuerySelectAllBuilt = myQuerySelectAll.build!DatabaseDialect;
    ---
 +/
struct Query
{
    Table table;

private:
    Where _where;
    PreSet _preSet; // pre-set values for placeholders, already provided during query building
    bool _limit = false;
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
        this.preSet = q._preSet;
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
            Pre-set values for placeholders, already provided during query building
         +/
        PreSet preSet;

        /++
            Has a LIMIT clause
         +/
        bool limit;
    }
}

/++
    Returns: a query builder for the specified table
 +/
Query buildQuery(const Table table) nothrow @nogc
{
    return Query(table);
}

/// ditto
Query buildQuery(const string table) nothrow @nogc
{
    return Query(Table(table));
}

///
alias qb = buildQuery;

enum isComparisonOperator(T) = (
        is(T == ComparisonOperator)
            || is(T == wchar)
            || is(T == char)
    );

/++
    Appends a check to the query's WHERE clause

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
        ++q._where.placeholders;
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
    q._preSet.where[q._where.placeholders] = value;

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

    if (q._where.tokens.length > 0)
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

Query limit(Query q)
{
    q._limit = true;
    q._preSet.limit.nullify;

    return q;
}

Query limit(Query q, int limit)
{
    q._limit = true;
    q._preSet.limit = limit;

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
                "Column indentifiers must be strings, but type of parameter " ~ idx.to!string ~ " is `" ~ colType ~ '`'
            );
        }
    }

    return Select(from, data);
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
                "Column indentifiers must be strings, but type of parameter " ~ idx.to!string ~ " is `" ~ colType ~ '`'
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
                "Column indentifiers must be strings, but type of parameter " ~ idx.to!string ~ " is `" ~ colType ~ '`'
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

struct Delete
{
}

private struct _BuiltQuery
{
    string sql;
    size_t wherePlaceholders;
    PreSet preSet;
}

///
public alias BuiltQuery = const(_BuiltQuery);

/++
    Determines whether `T` is a valid SQL “Query Compiler” implementation
    
    ---
    struct MyDialect
    {
    @safe pure:
        static BuiltQuery build(const Select selectQuery);
        static BuiltQuery build(const Update updateQuery);
        static BuiltQuery build(const Insert insertQuery);
        static BuiltQuery build(const Delete deleteQuery);
    }

    static assert(isQueryCompilerDialect!MyDialect);
    ---
 +/
enum bool isQueryCompilerDialect(T) =
    // dfmt off
(
    is(ReturnType!(() => T.build(Select())) == BuiltQuery)
    && is(ReturnType!(() => T.build(Update())) == BuiltQuery)
    && is(ReturnType!(() => T.build(Insert())) == BuiltQuery)
    //&& is(ReturnType!(T.build(Delete())) == BuiltQuery)
);
// dfmt on

BuiltQuery build(QueryCompilerDialect)(Select q)
        if (isQueryCompilerDialect!QueryCompilerDialect)
{
    return QueryCompilerDialect.build(q);
}

BuiltQuery build(QueryCompilerDialect)(Update q)
        if (isQueryCompilerDialect!QueryCompilerDialect)
{
    return QueryCompilerDialect.build(q);
}

BuiltQuery build(QueryCompilerDialect)(Insert q)
        if (isQueryCompilerDialect!QueryCompilerDialect)
{
    return QueryCompilerDialect.build(q);
}

BuiltQuery build(QueryCompilerDialect)(Delete q)
        if (isQueryCompilerDialect!QueryCompilerDialect)
{
    return QueryCompilerDialect.build(q);
}
