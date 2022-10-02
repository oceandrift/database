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
    The target dialect is specified through a template parameter ([oceandrift.db.sqlite3.SQLite3] in this example).

    ---
    BuiltQuery q = table("person").qb
        .where("age", '>')
        .select("*")
        .build!SQLite3()
    ;
    writeln(q.sql); // prints: SELECT * FROM "person" WHERE "age" > ?
    ---

    Multiple conditions can be chained by calling [where] multiple times:

    ---
    Query q = table("person").qb
        .where("age", '>')
        .where("height", '>')
    ;
    // --> FROM "person" WHERE "age" > ? AND "height" > ?
    ---

    Note that “conjunction” (AND) is the default.
    To create a “disjunction” (OR) set the first template parameter of [where] to [or]

    ---
    Query q = table("person").qb
        .where   ("age", '>')
        .where!or("height", '>')
    ;
    // --> FROM "person" WHERE "age" > ? OR "height" > ?
    ---

    $(TIP
        [or] is shorthand for [LogicalOperator.or].
    )

    $(TIP
        Of course there is [and] (aka [LogicalOperator.and]), too.

        ---
        Query q = table("person").qb
            .where    ("age", '>')
            .where!and("height", '>')
        ;
        // --> FROM "person" WHERE "age" > ? AND "height" > ?
        ---

        This results in the same query as not specifing the logical juntion in the first place.
    )

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

/// ditto
Statement prepare(DatabaseDriver)(BuiltQuery builtQuery, DatabaseDriver db)
{
    return db.prepareBuiltQuery(builtQuery);
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

/++
    Logical operators
+/
enum LogicalOperator : bool
{
    /// OR (logical operator, SQL)
    or = true,

    /// AND (logical operator, SQL)
    and = false,
}

enum
{
    /// OR (logical operator, SQL)
    or = LogicalOperator.or,

    /// AND (logical operator, SQL)
    and = LogicalOperator.and,
}

/// NOT (SQL)
enum not = false;

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
        columnTable = 't',
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
    Column identifier
 +/
struct Column
{
    string name;
    Table table;
}

inout(Column) column(inout string name) nothrow @nogc
{
    return Column(name, Table(null));
}

inout(Column) column(inout string name, inout Table table) nothrow @nogc
{
    return Column(name, table);
}

inout(Column) column(inout Table table, inout string name) nothrow @nogc
{
    return Column(name, table);
}

alias col = column;

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

/++
    Ascending vs. Descending
 +/
enum OrderingSequence : bool
{
    asc = false,
    desc = true,
}

enum
{
    /++
        ASCending
     +/
    asc = OrderingSequence.asc,

    /++
        DESCending
     +/
    desc = OrderingSequence.desc,
}

struct OrderingTerm
{
    Column column;
    OrderingSequence orderingSequence = OrderingSequence.asc;
}

struct Limit
{
    bool enabled = false;
    Nullable!ulong preSet;

    bool offsetEnabled = false;
    Nullable!ulong offsetPreSet;
}

/++
    JOIN clause abstraction
 +/
struct Join
{
    /++
        JOIN types
     +/
    enum Type
    {
        invalid = '\xFF', ///
        inner = 'i', ///
        leftOuter = 'L', ///
        rightOuter = 'R', ///
        fullOuter = 'F', ///
        cross = 'C' ///
    }

    Type type;
    Column source;
    Column target;
}

enum : Join.Type
{
    /// [INNER] JOIN
    inner = Join.Type.inner,

    /// LEFT [OUTER] JOIN
    leftOuter = Join.Type.leftOuter,

    /// RIGHT [OUTER] JOIN
    rightOuter = Join.Type.rightOuter,

    /// FULL [OUTER] JOIN
    fullOuter = Join.Type.fullOuter,

    /// CROSS JOIN
    cross = Join.Type.cross,
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
    Join[] _join;
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
        this.join = q._join;
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
            JOIN clause
         +/
        Join[] join;

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

/// ditto
alias qb = complexQueryBuilder;

/++
    Appends a JOIN statement to a query

    ---
    // … FROM book JOIN author ON author.id = author_id …
    Query q = table("book").qb.join(
        table("author"),
        "id",
        "author_id"
    );
    // or:
    Query q = table("book").qb.join(
        column(table("author"), "id"),
        "author_id"
    );

    enum book = table("book");
    enum author = table("author");
    Query q = book.qb.join(
        column(author, "id"),
        column(book, "author_id")
    );
    // --> … FROM book JOIN author ON author.id = book.author_id …
    ---

    Params:
        joinTarget = determines which table to join with (and which column to use in the join constraint (“ON”))
 +/
Query join(Join.Type type = Join.Type.inner)(Query q, const Column joinTarget, const Column sourceColumn)
in (!((joinTarget.name is null) ^ (sourceColumn.name is null)))
{
    q._join ~= Join(type, sourceColumn, joinTarget);
    return q;
}

/// ditto
Query join(Join.Type type = Join.Type.inner)(Query q, const Column joinTarget, const string sourceColumn)
{
    pragma(inline, true);
    return join!type(q, joinTarget, col(sourceColumn));
}

/// ditto
Query join(Join.Type type = Join.Type.inner)(
    Query q,
    const Table joinTarget,
    const string joinOnTargetColumn,
    const string onSourceColumn
)
{
    pragma(inline, true);
    return join!type(q, col(joinTarget, joinOnTargetColumn), col(onSourceColumn));
}

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

    // …FROM people WHERE name = ? OR name = ?…
    Query qNameAorB = table("people").qb
        .where   ("name", '=')
        .where!or("name", '=')              // explicit !or as !and is the default
    ;
    ---
 +/
/// ditto
Query where(LogicalOperator logicalJunction = and, TComparisonOperator)(
    Query q,
    Column column,
    TComparisonOperator op
) if (isComparisonOperator!TComparisonOperator)
{
    enum Token tokenLogicalJunction =
        (logicalJunction == or) ? Token(Token.Type.or) : Token(Token.Type.and);

    if ((q._where.tokens.length > 0) && (q._where.tokens[$ - 1].type != Token.Type.leftParenthesis))
        q._where.tokens ~= tokenLogicalJunction;

    if (column.table.name !is null)
        q._where.tokens ~= Token(Token.Type.columnTable, Token.Data(column.table.name));
    q._where.tokens ~= Token(Token.Type.column, Token.Data(column.name));

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
Query where(LogicalOperator logicalJunction = and, TComparisonOperator)(
    Query q,
    string column,
    TComparisonOperator op
) if (isComparisonOperator!TComparisonOperator)
{
    pragma(inline, true);
    return where!logicalJunction(q, col(column), op);
}

/// ditto
Query where(LogicalOperator logicalJunction = and, TComparisonOperator, T)(
    Query q,
    Column column,
    TComparisonOperator op,
    T value
) @trusted // TODO: template constraint
{
    q._where.preSet[q._where.placeholders] = value;

    return q.where!logicalJunction(column, op);
}

Query where(LogicalOperator logicalJunction = and, TComparisonOperator, T)(
    Query q,
    string column,
    TComparisonOperator op,
    T value
)
{
    pragma(inline, true);
    return where!logicalJunction(q, col(column), op, value);
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
Query whereParentheses(LogicalOperator logicalJunction = and)(Query q, Query delegate(scope Query q) @safe pure conditions)
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
    Query whereNot(Query q, Column column, TComparisonOperator op)
    {
        q._where ~= Token(Token.Type.not);
        q._where ~= Token(Token.Type.leftParenthesis);
        q.where!logicalJunction(column, op);
        q._where ~= Token(Token.Type.rightParenthesis);
    }

    Query whereNot(Query q, Column column, TComparisonOperator op, DBValue value)
    {
        q._where ~= Token(Token.Type.not);
        q._where ~= Token(Token.Type.leftParenthesis);
        q.where!logicalJunction(column, op, value);
        q._where ~= Token(Token.Type.rightParenthesis);
    }

    Query whereNot(Query q, string column, TComparisonOperator op)
    {
        pragma(inline, true);
        return whereNot(q, col(column), op);
    }

    Query whereNot(Query q, string column, TComparisonOperator op, DBValue value)
    {
        pragma(inline, true);
        return whereNot(q, col(column), op, value);
    }
}

/++
    Appends a ORDER BY clause to a query

    ---
    q.orderBy("column");                        // ASCending order is the default

    q.orderBy("column", asc);                   // explicit ASCending order
    q.orderBy("column", desc);                  // DESCending order

    q.orderBy("column", OrderingSequence.asc);  // ASC, long form
    q.orderBy("column", OrderingSequence.desc); // DESC, long form
    ---
 +/
Query orderBy(Query q, Column column, OrderingSequence orderingSequence = asc)
{
    q._orderBy ~= OrderingTerm(column, orderingSequence);
    return q;
}

Query orderBy(Query q, string column, OrderingSequence orderingSequence = asc)
{
    pragma(inline, true);
    return orderBy(q, col(column), orderingSequence);
}

/++
    Sets or updates the LIMIT clause of a query

    ---
    q.limit(25);        // LIMIT ?          – pre-set: limit=25
    q.limit(25, 100);   // LIMIT ? OFFSET ? – pre-set: limit=25,offset=100

    q.limit(false);     // LIMIT ?          – set values later
    q.limit(true);      // LIMIT ? OFFSET ? – set values later
 +/
Query limit(bool withOffset = false)(Query q)
{
    q._limit.enabled = true;
    q._limit.preSet.nullify();

    q._limit.offsetEnabled = withOffset;
    q._limit.offsetPreSet.nullify();

    return q;
}

/// ditto
Query limit(Query q, ulong limit)
{
    q._limit.enabled = true;
    q._limit.preSet = limit;

    return q;
}

/// ditto
Query limit(Query q, ulong limit, ulong offset)
{
    q._limit.enabled = true;
    q._limit.preSet = limit;
    q._limit.offsetEnabled = true;
    q._limit.offsetPreSet = offset;

    return q;
}

// -- Select

/++
    SQL aggregate function types
 +/
enum AggregateFunction
{
    none = 0, ///
    avg, ///
    count, ///
    max, ///
    min, ///
    sum, ///
    group_concat, ///
}

/++
    SELECT expression abstraction

    $(WARNING
        A select expression is a unit to be selected by a query (like a column, or the aggregate (count, min, max, …) of a column).

        Not to confuse with “SELECT query”.
        See [Select] for abstraction of the latter.
    )
 +/
struct SelectExpression
{
    /++
        Column to select
     +/
    Column column;

    /++
        Aggregate function to apply
     +/
    AggregateFunction aggregateFunction = AggregateFunction.none;

    /++
        Whether selection should be DISTINCT (or not)
     +/
    Distinct distinct = Distinct.no;
}

/++
    SQL DISTINCT keyword abstraction
 +/
enum Distinct : bool
{
    ///
    no = false,

    ///
    yes = true,
}

/++
    DISTINCT
    i.e. no duplicates
+/
enum Distinct distinct = Distinct.yes;

/++
    Short-hand helper function for SELECT AVG(…)
 +/
SelectExpression avg(Distinct distinct = Distinct.no)(string column)
{
    return SelectExpression(col(column), AggregateFunction.avg, distinct);
}

/++
    Short-hand helper function for SELECT COUNT(…)
 +/
SelectExpression count(Distinct distinct = Distinct.no)(string column = "*")
{
    return SelectExpression(col(column), AggregateFunction.count, distinct);
}

/++
    Short-hand helper function for SELECT MAX(…)
 +/
SelectExpression max(Distinct distinct = Distinct.no)(string column)
{
    return SelectExpression(col(column), AggregateFunction.max, distinct);
}

/++
    Short-hand helper function for SELECT MIN(…)
 +/
SelectExpression min(Distinct distinct = Distinct.no)(string column)
{
    return SelectExpression(col(column), AggregateFunction.min, distinct);
}

/++
    Short-hand helper function for SELECT SUM(…)
 +/
SelectExpression sum(Distinct distinct = Distinct.no)(string column)
{
    return SelectExpression(col(column), AggregateFunction.sum, distinct);
}

/++
    Short-hand helper function for SELECT GROUP_CONCAT(…)
 +/
SelectExpression group_concat(Distinct distinct = Distinct.no)(string column)
{
    return SelectExpression(col(column), AggregateFunction.groupConcat, distinct);
}

/++
    SELECT Query abstraction
 +/
struct Select
{
    Query query;
    const(SelectExpression)[] columns;
}

/++
    Creates an abstracted SELECT query selecting the specified columns.

    ---
    Select mtHigherThan2k = table("mountain").qb
        .where("height", '>', 2000)
        .select("id", "height")
    ;

    Select knownMountains = table("mountain").qb
        .select(count("*"))
    ;

    Select maxHeight = table("mountain").qb
        .select(max("height"))
    ;
    ---

    Params:
        columns = Columns to select; either as strings or [SelectExpression]s
 +/
Select select(ColumnV...)(Query from, ColumnV columns)
{
    static if (columns.length == 0)
        return Select(from, [SelectExpression(col("*"))]);
    else
    {
        auto data = new SelectExpression[](columns.length);

        static foreach (idx, col; columns)
        {
            static if (is(typeof(col) == string))
                data[idx] = SelectExpression(oceandrift.db.dbal.v4.column(col));
            else static if (is(typeof(col) == Column))
            {
                data[idx] = SelectExpression(col);
            }
            else static if (is(typeof(col) == SelectExpression))
                data[idx] = col;
            else
            {
                enum colType = typeof(col).stringof;
                static assert(
                    0,
                    "Column identifiers must be strings, but type of parameter " ~ idx.to!string ~ " is `" ~ colType ~ '`'
                );
            }
        }

        return Select(from, data);
    }
}

// -- Update

/++
    UPDATE query abstraction
 +/
struct Update
{
    Query query;
    const(string)[] columns;
}

/++
    Creates an abstracted UPDATE query for updating the specified columns

    ---
    Update updateMountainNo90 = table("mountain").qb
        .where("id", '=', 90)
        .update("height")
    ;

    Update updateStats = table("mountain").qb.update(
        "visitors",
        "times_summit_reached",
        "updated_at"
    );
    ---
 +/
Update update(Query query, const(string)[] columns)
{
    return Update(query, columns);
}

/// ditto
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

/++
    INSERT query abstraction
 +/
struct Insert
{
    Table table;
    const(string)[] columns;
    uint rowCount = 1;
}

/++
    Creates an abstracted INSERT query for inserting row(s) into the specified table
    filling the passed columns

    ---
    Insert insertMountain = table("mountain").insert(
        "name",
        "location",
        "height"
    );
    // INSERT INTO "mountain"("name", "location", "height") VALUES (?, ?, ?)

    Insert insert3MountainsAtOnce = table("mountain")
        .insert(
            "name",
            "height"
        )
        .times(3)
    ;
    // INSERT INTO "mountain"("name", "height") VALUES (?, ?), (?, ?), (?, ?)
    ---

    See_Also:
        Use [times] to create a query for inserting multiple rows at once.
 +/
Insert insert(Table table, const(string)[] columns)
{
    return Insert(table, columns);
}

/// ditto
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

/++
    Specifies how many rows to INSERT a once

    e.g. `INSERT INTO "mountain"("name", "height") VALUES (?, ?), (?, ?)`
 +/
Insert times(Insert insert, const uint rows)
{
    insert.rowCount = rows;
    return insert;
}

// -- Delete

/++
    DELETE query abstraction
 +/
struct Delete
{
    Query query;
}

/++
    Creates an abstracted DELETE query from the specified query

    ---
    Delete deleteMountainsWithUnknownHeight = table("mountain").qb
        .where("height", ComparisonOperator.isNull)
        .delete_()
    ;
    // DELETE FROM "mountain" WHERE "height" IS NULL
    ---
 +/
Delete delete_(Query query)
{
    return Delete(query);
}

// -- Query building

private struct _PlaceholdersMeta
{
    size_t where;
}

///
public alias PlaceholdersMeta = const(_PlaceholdersMeta);

private struct _PreSets
{
    DBValue[int] where;
    Nullable!ulong limit;
    Nullable!ulong limitOffset;
}

///
public alias PreSets = const(_PreSets);

private struct _BuiltQuery
{
    string sql;
    PlaceholdersMeta placeholders;
    PreSets preSets;
}

/++
    Built query as generated by a QueryBuilder

    or in other words: the result of query building

    ---
    // Construct an abstract query
    Select query = table("mountain").qb.select(count("*"));

    // Build the query with the query builder of your choice (e.g. [oceandrift.db.sqlite3.SQLite3] when using SQLite3)
    BuiltQuery builtQuery = QueryBuilder.build(query);

    // Prepare a statement from your query by calling [prepareBuiltQuery]
    Statement stmt = db.prepareBuiltQuery(builtQuery);

    stmt.execute();
    // …
    ---

    ---
    // More idiomatic way using UFCS:
    BuiltQuery builtQuery = table("mountain").qb
        .select(count("*"))
        .build!QueryCompiler()
    ;

    // Query building works during compile-time as well(!):
    enum BuiltQuery bq = table("mountain").qb
        .select(count("*"))
        .build!QueryCompiler()
    ;
    ---
 +/
public alias BuiltQuery = const(_BuiltQuery);

/++
    Determines whether `T` is a valid SQL “Query Compiler” implementation

    ---
    struct MyDatabaseDriver
    {
        // […]

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

/++
    Builds the passed query through the provided QueryCompiler
    (UFCS helper function)
 +/
BuiltQuery build(QueryCompiler)(Select q) if (isQueryCompiler!QueryCompiler)
{
    pragma(inline, true);
    return QueryCompiler.build(q);
}

/// ditto
BuiltQuery build(QueryCompiler)(Update q) if (isQueryCompiler!QueryCompiler)
{
    pragma(inline, true);
    return QueryCompiler.build(q);
}

/// ditto
BuiltQuery build(QueryCompiler)(Insert q) if (isQueryCompiler!QueryCompiler)
{
    pragma(inline, true);
    return QueryCompiler.build(q);
}

/// ditto
BuiltQuery build(QueryCompiler)(Delete q) if (isQueryCompiler!QueryCompiler)
{
    pragma(inline, true);
    return QueryCompiler.build(q);
}
