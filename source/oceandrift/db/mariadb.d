/++
    MariaDB Database Driver

    Supposed to be MySQL compatible, as well.

    ---
    DatabaseDriver db = new MariaDBDatabaseDriver(
        "localhost",
        "username",
        "password",
        "database name",    // (optional) “Database” on the server to use initially
        3306,               // (optional) MariaDB server port
    );

    db.connect(); // establish database connection
    scope(exit) db.close(); // scope guard, to close the database connection when exiting the current scope
    ---

    $(NOTE
        If you don’t specify an inital database during the connection setup,
        you’ll usually want to manually select one by executing a `USE databaseName;` statement.
    )
 +/
module oceandrift.db.mariadb;

import mysql.safe;
import oceandrift.db.dbal.driver;
import oceandrift.db.dbal.v4;
import std.array : appender, Appender;
import std.conv : to;

@safe:

alias DBALRow = oceandrift.db.dbal.driver.Row;
alias MySQLRow = mysql.safe.Row;

/++
    MariaDB database driver for oceandrift

    Built upon mysql-native; uses its @safe API.

    See_Also:
        https://code.dlang.org/packages/mysql-native
 +/
final class MariaDB : DatabaseDriverSpec
{
@safe:

    private
    {
        Connection _connection;

        string _host;
        ushort _port;
        string _username;
        string _password;
        string _database;
    }

    /++
        Constructor incl. connection setup

        Params:
            host = database host (the underlying mysql-native currently only supports TCP connections, unfortunately)
            username = MariaDB user
            password = password of the MariaDB user
            database = initial database to use
            port = MariaDB server port
     +/
    public this(string host, string username, string password, string database = null, ushort port = 3306)
    {
        _host = host;
        _port = port;
        _username = username;
        _password = password;
        _database = database;
    }

    public
    {
        void connect()
        {
            _connection = new Connection(
                _host,
                _username,
                _password,
                _database,
                _port
            );
        }

        void close()
        {
            _connection.close();
        }

        bool connected()
        {
            return ((this._connection !is null)
                    && !this._connection.closed);
        }
    }

    public
    {
        bool autoCommit()
        {
            return this._connection
                .queryRow("SELECT @@autocommit")
                .get[0] != 0;
        }

        void autoCommit(bool enable)
        {
            if (enable)
                _connection.exec("SET autocommit=1");
            else
                _connection.exec("SET autocommit=0");
        }

        void transactionStart()
        {
            _connection.exec("START TRANSACTION");
        }

        void transactionCommit()
        {
            _connection.exec("COMMIT");
        }

        void transactionRollback()
        {
            _connection.exec("ROLLBACK");
        }
    }

    public
    {
        void execute(string sql)
        {
            this._connection.exec(sql);
        }

        Statement prepare(string sql)
        {
            return new MariaDBStatement(this._connection, sql);
        }

        DBValue lastInsertID()
        {
            return DBValue(_connection.lastInsertID());
        }
    }

    public  // Extras
    {
        ///
        Connection getConnection()
        {
            return this._connection;
        }
    }

    public pure  // Query Compiler
    {
        static BuiltQuery build(const Select select)
        {
            auto sql = appender!string("SELECT");

            foreach (idx, se; select.columns)
            {
                if (idx > 0)
                    sql ~= ',';

                se.toSQL(sql);
            }

            sql ~= " FROM `";
            sql ~= select.query.table.name.escapeIdentifier();
            sql ~= '`';

            const query = CompilerQuery(select.query);
            query.join.joinToSQL(sql);
            query.where.whereToSQL(sql);
            query.orderByToSQL(sql);
            query.limitToSQL(sql);

            return BuiltQuery(
                sql.data,
                PlaceholdersMeta(query.where.placeholders),
                PreSets(query.where.preSet, query.limit.preSet, query.limit.offsetPreSet)
            );
        }

        static BuiltQuery build(const Update update)
        in (update.columns.length >= 1)
        in (CompilerQuery(update.query).join.length == 0)
        {
            auto sql = appender!string("UPDATE");
            sql ~= " `";
            sql ~= update.query.table.name.escapeIdentifier();
            sql ~= "` SET";

            foreach (idx, value; update.columns)
            {
                if (idx > 0)
                    sql ~= ',';

                sql ~= " `";
                sql ~= value.escapeIdentifier;
                sql ~= "` = ?";
            }

            const query = CompilerQuery(update.query);
            query.where.whereToSQL(sql);
            query.orderByToSQL(sql);
            query.limitToSQL(sql);

            return BuiltQuery(
                sql.data,
                PlaceholdersMeta(query.where.placeholders),
                PreSets(query.where.preSet, query.limit.preSet, query.limit.offsetPreSet)
            );
        }

        static BuiltQuery build(const Insert query)
        in (
            (query.columns.length > 1)
            || (query.rowCount == 1)
        )
        {
            auto sql = appender!string("INSERT INTO `");
            sql ~= escapeIdentifier(query.table.name);

            if (query.columns.length == 0)
            {
                sql ~= "` DEFAULT VALUES";
            }
            else
            {
                sql ~= "` (";

                foreach (idx, column; query.columns)
                {
                    if (idx > 0)
                        sql ~= ", ";

                    sql ~= '`';
                    sql ~= escapeIdentifier(column);
                    sql ~= '`';
                }

                sql ~= ") VALUES";

                for (uint n = 0; n < query.rowCount; ++n)
                {
                    if (n > 0)
                        sql ~= ",";

                    sql ~= " (";
                    if (query.columns.length > 0)
                    {
                        sql ~= '?';

                        if (query.columns.length > 1)
                            for (size_t i = 1; i < query.columns.length; ++i)
                                sql ~= ",?";
                    }
                    sql ~= ')';
                }
            }

            return BuiltQuery(sql.data);
        }

        static BuiltQuery build(const Delete delete_)
        in (CompilerQuery(delete_.query).join.length == 0)
        {
            auto sql = appender!string("DELETE FROM `");
            sql ~= delete_.query.table.name.escapeIdentifier();
            sql ~= '`';

            const query = CompilerQuery(delete_.query);

            query.where.whereToSQL(sql);
            query.orderByToSQL(sql);
            query.limitToSQL(sql);

            return BuiltQuery(
                sql.data,
                PlaceholdersMeta(query.where.placeholders),
                PreSets(query.where.preSet, query.limit.preSet, query.limit.offsetPreSet)
            );
        }
    }
}

alias MariaDBDatabaseDriver = MariaDB;

private mixin template bindImpl(T)
{
    void bind(int index, const T value) @safe
    {
        _stmt.setArg(index, value);
    }
}

/+
    Wrapper over mysql-native’s Prepared and ResultRange

    undocumented on purpose – shouldn’t be used directly, just stick to [oceandrift.db.dbal.Statement]
 +/
final class MariaDBStatement : Statement
{
@safe:

    private
    {
        Connection _connection;
        Prepared _stmt;
        ResultRange _result;
        DBALRow _front;
    }

    private this(Connection connection, string sql)
    {
        _connection = connection;
        _stmt = _connection.prepare(sql);
    }

    public
    {
        void execute()
        {
            try
            {
                _result = _connection.query(_stmt);

                if (!_result.empty) // apparently result being empty can be the case
                    _front = _result.front.mysqlToDBAL();
            }
            catch (MYXNoResultRecieved)
            {
                // workaround because of MYXNoResultRecieved

                // The executed query did not produce a result set.
                // mysql-native wants us to «use the exec functions, not query, for commands that don't produce result sets».

                _result = typeof(_result).init;
                _front = null;
            }
        }

        void close()
        {
            _result.close();
        }
    }

    public
    {
        bool empty() pure nothrow
        {
            return _result.empty;
        }

        DBALRow front() pure nothrow @nogc
        {
            return _front;
        }

        void popFront()
        {
            _result.popFront();
            if (!_result.empty)
                _front = _result.front.mysqlToDBAL();
        }
    }

    public
    {
        mixin bindImpl!bool;
        mixin bindImpl!byte;
        mixin bindImpl!ubyte;
        mixin bindImpl!short;
        mixin bindImpl!ushort;
        mixin bindImpl!int;
        mixin bindImpl!uint;
        mixin bindImpl!long;
        mixin bindImpl!ulong;
        mixin bindImpl!double;
        mixin bindImpl!string;
        mixin bindImpl!DateTime;
        mixin bindImpl!TimeOfDay;
        mixin bindImpl!Date;
        mixin bindImpl!(const(ubyte)[]);
        mixin bindImpl!(typeof(null));
    }
}

/++
    Creates an [oceandrift.db.dbal.driver.Row] from a [MySQLRow]
 +/
DBALRow mysqlToDBAL(MySQLRow mysql)
{
    auto rowData = new DBValue[](mysql.length);

    for (size_t i = 0; i < mysql.length; ++i)
        (delegate() @trusted { rowData[i] = mysql[i].mysqlToDBAL(); })();

    return oceandrift.db.dbal.driver.Row(rowData);
}

/++
    Creates a [DBValue] from a [MySQLVal]
 +/
DBValue mysqlToDBAL(MySQLVal mysql)
{
    import taggedalgebraic : get, hasType;

    final switch (mysql.kind) with (MySQLVal)
    {
    case Kind.Blob:
        return DBValue(mysql.get!(ubyte[]));
    case Kind.CBlob:
        return DBValue(mysql.get!(const(ubyte)[]));
    case Kind.Null:
        return DBValue(mysql.get!(null_t));
    case Kind.Bit:
        return DBValue(mysql.get!(bool));
    case Kind.UByte:
        return DBValue(mysql.get!(ubyte));
    case Kind.Byte:
        return DBValue(mysql.get!(byte));
    case Kind.UShort:
        return DBValue(mysql.get!(ushort));
    case Kind.Short:
        return DBValue(mysql.get!(short));
    case Kind.UInt:
        return DBValue(mysql.get!(uint));
    case Kind.Int:
        return DBValue(mysql.get!(int));
    case Kind.ULong:
        return DBValue(mysql.get!(ulong));
    case Kind.Long:
        return DBValue(mysql.get!(long));
    case Kind.Float:
        return DBValue(mysql.get!(float));
    case Kind.Double:
        return DBValue(mysql.get!(double));
    case Kind.DateTime:
        return DBValue(mysql.get!(DateTime));
    case Kind.Time:
        return DBValue(mysql.get!(TimeOfDay));
    case Kind.Timestamp:
        return DBValue(mysql.get!(Timestamp).rep);
    case Kind.Date:
        return DBValue(mysql.get!(Date));
    case Kind.Text:
        return DBValue(mysql.get!(string));
    case Kind.CText:
        return DBValue(mysql.get!(const(char)[]));
    case Kind.BitRef:
        return DBValue(*mysql.get!(const(bool)*));
    case Kind.UByteRef:
        return DBValue(*mysql.get!(const(ubyte)*));
    case Kind.ByteRef:
        return DBValue(*mysql.get!(const(byte)*));
    case Kind.UShortRef:
        return DBValue(*mysql.get!(const(ushort)*));
    case Kind.ShortRef:
        return DBValue(*mysql.get!(const(short)*));
    case Kind.UIntRef:
        return DBValue(*mysql.get!(const(uint)*));
    case Kind.IntRef:
        return DBValue(*mysql.get!(const(int)*));
    case Kind.ULongRef:
        return DBValue(*mysql.get!(const(ulong)*));
    case Kind.LongRef:
        return DBValue(*mysql.get!(const(long)*));
    case Kind.FloatRef:
        return DBValue(*mysql.get!(const(float)*));
    case Kind.DoubleRef:
        return DBValue(*mysql.get!(const(double)*));
    case Kind.DateTimeRef:
        return DBValue(*mysql.get!(const(DateTime)*));
    case Kind.TimeRef:
        return DBValue(*mysql.get!(const(TimeOfDay)*));
    case Kind.DateRef:
        return DBValue(*mysql.get!(const(Date)*));
    case Kind.TextRef:
        return DBValue(*mysql.get!(const(string)*));
    case Kind.CTextRef:
        return DBValue((*mysql.get!(const(char[])*)).dup);
    case Kind.BlobRef:
        return DBValue(*mysql.get!(const(ubyte[])*));
    case Kind.TimestampRef:
        return DBValue(mysql.get!(const(Timestamp)*).rep);
    }
}

private pure
{
    void joinToSQL(const Join[] joinClause, ref Appender!string sql)
    {
        foreach (join; joinClause)
        {
            final switch (join.type) with (Join)
            {
            case Type.invalid:
                assert(0, "Join.Type.invalid");

            case Type.inner:
                sql ~= " JOIN `";
                break;

            case Type.leftOuter:
                sql ~= " LEFT OUTER JOIN `";
                break;

            case Type.rightOuter:
                sql ~= " RIGHT OUTER JOIN `";
                break;

            case Type.fullOuter:
                assert(false, "MariaDB does not support FULL OUTER JOINs");
                //sql ~= " FULL OUTER JOIN `"`;
                //break;

            case Type.cross:
                sql ~= " CROSS JOIN `";
                break;
            }

            sql ~= escapeIdentifier(join.target.table.name);
            sql ~= '`';

            if (join.target.name is null)
                return;

            sql ~= " ON `";
            sql ~= escapeIdentifier(join.target.table.name);
            sql ~= "`.`";
            sql ~= escapeIdentifier(join.target.name);
            sql ~= "` = `";

            if (join.source.table.name !is null)
            {
                sql ~= escapeIdentifier(join.source.table.name);
                sql ~= "`.`";
            }

            sql ~= escapeIdentifier(join.source.name);
            sql ~= '`';
        }
    }

    void whereToSQL(const Where where, ref Appender!string sql)
    {
        if (where.tokens.length == 0)
            return;

        sql ~= " WHERE";

        Token.Type prev;

        foreach (Token t; where.tokens)
        {
            final switch (t.type) with (Token)
            {
            case Type.columnTable:
                sql ~= " `";
                (delegate() @trusted { sql ~= t.data.str.escapeIdentifier(); })();
                sql ~= "`.";
                break;
            case Type.column:
                if (prev != Type.columnTable)
                    sql ~= ' ';
                sql ~= '`';
                (delegate() @trusted { sql ~= t.data.str.escapeIdentifier(); })();
                sql ~= '`';
                break;
            case Type.placeholder:
                sql ~= " ?";
                break;
            case Type.comparisonOperator:
                sql ~= t.data.op.toSQL;
                break;

            case Type.and:
                sql ~= " AND";
                break;
            case Type.or:
                sql ~= " OR";
                break;

            case Type.not:
                sql ~= " NOT";
                break;

            case Type.leftParenthesis:
                sql ~= " (";
                break;
            case Type.rightParenthesis:
                sql ~= " )";
                break;

            case Type.invalid:
                assert(0, "Invalid SQL token in where clause");
            }

            prev = t.type;
        }
    }

    void limitToSQL(CompilerQuery q, ref Appender!string sql)
    {
        if (!q.limit.enabled)
            return;

        sql ~= " LIMIT ?";

        if (!q.limit.offsetEnabled)
            return;

        sql ~= " OFFSET ?";
    }

    void orderByToSQL(CompilerQuery q, ref Appender!string sql)
    {
        if (q.orderBy.length == 0)
            return;

        sql ~= " ORDER BY ";

        foreach (idx, OrderingTerm term; q.orderBy)
        {
            if (idx > 0)
                sql ~= ", ";

            if (term.column.table.name !is null)
            {
                sql ~= '`';
                sql ~= escapeIdentifier(term.column.table.name);
                sql ~= "`.";
            }
            sql ~= '`';
            sql ~= escapeIdentifier(term.column.name);
            sql ~= '`';

            if (term.orderingSequence == OrderingSequence.desc)
                sql ~= " DESC";
        }
    }

    void toSQL(SelectExpression se, ref Appender!string sql)
    {
        sql ~= ' ';

        enum switchCase(string aggr) = `case ` ~ aggr ~ `: sql ~= "` ~ aggr ~ `("; break;`;

        final switch (se.aggregateFunction) with (AggregateFunction)
        {
            mixin(switchCase!"avg");
            mixin(switchCase!"count");
            mixin(switchCase!"max");
            mixin(switchCase!"min");
            mixin(switchCase!"sum");
            mixin(switchCase!"group_concat");
        case none:
            break;
        }

        if (se.distinct)
            sql ~= "DISTINCT ";

        if (se.column.table.name !is null)
        {
            sql ~= '`';
            sql ~= se.column.table.name;
            sql ~= "`.";
        }

        if (se.column.name == "*")
        {
            sql ~= '*';
        }
        else
        {
            sql ~= '`';
            sql ~= se.column.name.escapeIdentifier;
            sql ~= '`';
        }

        if (se.aggregateFunction != AggregateFunction.none)
            sql ~= ')';
    }

    string toSQL(ComparisonOperator op)
    {
        final switch (op) with (ComparisonOperator)
        {
        case invalid:
            assert(0, "Invalid comparison operator");

        case equals:
            return " =";
        case notEquals:
            return " <>";
        case lessThan:
            return " <";
        case greaterThan:
            return " >";
        case lessThanOrEquals:
            return " <=";
        case greaterThanOrEquals:
            return " >=";
        case in_:
            return " IN";
        case notIn:
            return " NOT IN";
        case like:
            return " LIKE";
        case notLike:
            return " NOT LIKE";
        case isNull:
            return " IS NULL";
        case isNotNull:
            return " IS NOT NULL";
        }
    }

    string escapeIdentifier(string tableOrColumn)
    {
        import std.string : replace;

        return tableOrColumn.replace('`', "``");
    }
}
