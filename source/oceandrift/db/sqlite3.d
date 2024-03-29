/++
    SQLite3 Database Driver

    ---
    DatabaseDriver db = new SQLite3("my-database-file.sqlite3");

    db.connect(); // establish database connection
    scope(exit) db.close(); // scope guard, to close the database connection when exiting the current scope
    ---
+/
module oceandrift.db.sqlite3;

import etc.c.sqlite3;
import std.array : appender, Appender;
import std.string : fromStringz, toStringz;
import std.format : format;

import oceandrift.db.dbal.driver;
import oceandrift.db.dbal.v4;

@safe:

private enum
{
    formatDate = "%04d-%02u-%02u",
    formatTime = "%02u:%02u:%02u",
    formatDateTime = formatDate ~ ' ' ~ formatTime,
}

private void enforce(
    const int actual,
    lazy string msg,
    lazy uint extendedResultCode = -1,
    ResultCode expected = ResultCode.ok,
    string file = __FILE__,
    size_t line = __LINE__) pure
{
    if (actual != expected)
        throw new SQLiteX(cast(ResultCode) actual, msg, extendedResultCode, file, line);
}

class SQLiteX : Exception
{
@safe pure:

    public
    {
        ResultCode code;
        uint extendedCode;
    }

    this(ResultCode code, string msg, uint extendedCode, string file = __FILE__, size_t line = __LINE__)
    {
        super(msg, file, line);
        this.code = code;
        this.extendedCode = extendedCode;
    }
}

/++
    Modes to open SQLite3 databases in
 +/
enum OpenMode
{
    /// read-only
    ro = SQLITE_OPEN_READONLY,

    /// reading & writing
    rw = SQLITE_OPEN_READWRITE,

    /// reading & writing, create if not exists
    create = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,

    /// in-memory
    memory = OpenMode.create | SQLITE_OPEN_MEMORY,

    /// “multi-thread” mode, separate threads threads may not share database connections
    multiThreadindMultiDB = SQLITE_OPEN_NOMUTEX,

    /// “serialized” mode, threads can share database connections
    multiThreadingSerialized = SQLITE_OPEN_FULLMUTEX,

    /// database filename cannot be a symbolic link
    noSymLink = SQLITE_OPEN_NOFOLLOW,
}

/++
    SQLite3 function result code

    Named Enum Wrapper for sqlite’s C API result codes
 +/
enum ResultCode
{
    /// Successful result
    ok = SQLITE_OK,

    /// Generic error
    error = SQLITE_ERROR,

    /// Internal logic error in SQLite
    internalDatabaseError = SQLITE_INTERNAL,

    /// Access permission denied
    permission = SQLITE_PERM,

    /// Callback routine requested an abort
    abort = SQLITE_ABORT,

    /// The database file is locked
    busy = SQLITE_BUSY,

    /// A table in the database is locked
    locked = SQLITE_LOCKED,

    /// A malloc() failed
    noMem = SQLITE_NOMEM,

    /// Attempt to write a readonly database
    readOnly = SQLITE_READONLY,

    /// Operation terminated by sqlite3_interrupt()
    interrupt = SQLITE_INTERRUPT,

    /// Some kind of disk I/O error occurred
    ioError = SQLITE_IOERR,

    /// The database disk image is malformed
    corruptDiskImage = SQLITE_CORRUPT,

    /// Unknown opcode in sqlite3_file_control()
    opCodeNotFound = SQLITE_NOTFOUND,

    /// Insertion failed because database is full
    dbFull = SQLITE_FULL,

    /// Unable to open the database file
    cantOpen = SQLITE_CANTOPEN,

    /// Database lock protocol error
    protocolError = SQLITE_PROTOCOL,

    /// Internal use only
    empty = SQLITE_EMPTY,

    /// The database schema changed
    schemaChanged = SQLITE_SCHEMA,

    /// String or BLOB exceeds size limit
    tooBig = SQLITE_TOOBIG,

    /// Abort due to constraint violation
    constraintViolation = SQLITE_CONSTRAINT,

    /// Data type mismatch
    typeMismatch = SQLITE_MISMATCH,

    /// Library used incorrectly
    libraryMisuse = SQLITE_MISUSE,

    /// Uses OS features not supported on host
    noLFS = SQLITE_NOLFS,

    /// Authorization denied
    authDenied = SQLITE_AUTH,

    /// Not used
    format = SQLITE_FORMAT,

    /// 2nd parameter to sqlite3_bind out of range
    outOfRange = SQLITE_RANGE,

    /// File opened that is not a database file
    notADatabase = SQLITE_NOTADB,

    /// sqlite3_step() has finished executing
    done = SQLITE_DONE,

    /// sqlite3_step() has another row ready
    row = SQLITE_ROW,

    ///
    notice = SQLITE_NOTICE,

    ///
    warning = SQLITE_WARNING,
}

/++
    SQLite3 oceandrift database driver

    Built upon the sqlite3 C library
 +/
final class SQLite3 : DatabaseDriverSpec
{
@safe:
    private
    {
        sqlite3* _handle;
        OpenMode _mode;
        string _filename;
    }

    public this(string filename = ":memory:", OpenMode mode = OpenMode.create)
    {
        _filename = filename;
        _mode = mode;
    }

    OpenMode mode() pure nothrow @nogc
    {
        return _mode;
    }

    public
    {
        void connect()
        {
            connectImpl();
        }

        bool connected() nothrow
        {
            return (_handle !is null);
        }

        void close()
        {
            if (connected())
                closeImpl();
        }
    }

    public
    {
        bool autoCommit() @trusted
        {
            return (_handle.sqlite3_get_autocommit != 0);
        }

        void autoCommit(bool enable)
        {
            assert(0); // no
        }

        void transactionStart()
        {
            exec("BEGIN TRANSACTION");
        }

        void transactionCommit()
        {
            exec("COMMIT");
        }

        void transactionRollback()
        {
            exec("ROLLBACK");
        }
    }

    public
    {
        void execute(string sql)
        {
            exec(sql);
        }

        Statement prepare(string sql)
        {
            return new SQLite3Statement(_handle, sql);
        }

        DBValue lastInsertID() @trusted
        {
            return DBValue(_handle.sqlite3_last_insert_rowid());
        }
    }

    public
    {
        ///
        sqlite3* getConnection()
        {
            return _handle;
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

            sql ~= ` FROM "`;
            sql ~= select.query.table.name.escapeIdentifier();
            sql ~= '"';

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
            sql ~= ` "`;
            sql ~= update.query.table.name.escapeIdentifier();
            sql ~= `" SET`;

            foreach (idx, value; update.columns)
            {
                if (idx > 0)
                    sql ~= ',';

                sql ~= ` "`;
                sql ~= value.escapeIdentifier;
                sql ~= `" = ?`;
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
            auto sql = appender!string(`INSERT INTO "`);
            sql ~= escapeIdentifier(query.table.name);

            if (query.columns.length == 0)
            {
                sql ~= `" DEFAULT VALUES`;
            }
            else
            {
                sql ~= `" (`;

                foreach (idx, column; query.columns)
                {
                    if (idx > 0)
                        sql ~= ", ";

                    sql ~= '"';
                    sql ~= escapeIdentifier(column);
                    sql ~= '"';
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
            auto sql = appender!string(`DELETE FROM "`);
            sql ~= delete_.query.table.name.escapeIdentifier();
            sql ~= '"';

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

    private
    {
        void connectImpl() @trusted
        {
            immutable connected = sqlite3_open_v2(_filename.toStringz, &_handle, int(_mode), null);
            enforce(connected, "Connection failed");
            enforce(_handle.sqlite3_extended_result_codes(1), "Enabling SQLite's extended result codes failed");
            _mode = mode;
        }

        void closeImpl() @trusted
        {
            immutable closed = _handle.sqlite3_close_v2();
            enforce(closed, "Failed to close connection");
            _handle = null;
        }

        void exec(string sql) @trusted
        {
            char* errorMsg;
            immutable status = sqlite3_exec(_handle, sql.toStringz, null, null, &errorMsg);
            try
            {
                enforce(
                    status,
                    errorMsg.fromStringz.idup,
                    _handle.sqlite3_extended_errcode(),
                    ResultCode.ok,
                );
            }
            finally
            {
                if (errorMsg !is null)
                    sqlite3_free(errorMsg);
            }
        }
    }
}

// undocumented on purpose
final class SQLite3Statement : Statement
{
@safe:

    private
    {
        sqlite3* _dbHandle;
        sqlite3_stmt* _stmtHandle;
        int _status;
        Row _front;

    }

    private this(sqlite3* dbHandle, string sql) @trusted
    {
        _dbHandle = dbHandle;

        // If nByte is positive, then it is the number of bytes read from zSql.
        // No zero-terminated required.
        immutable prepared = _dbHandle.sqlite3_prepare_v2(sql.ptr, cast(int) sql.length, &_stmtHandle, null);

        enforce(
            prepared,
            "Preparation failed:\n" ~ _dbHandle.sqlite3_errmsg()
                .fromStringz.idup, // “The application does not need to worry about freeing the result.”
                _dbHandle.sqlite3_extended_errcode(),
        );
    }

    public
    {
        void execute() @trusted
        {
            this.popFront();
        }

        void close() @trusted
        {
            // Invoking sqlite3_finalize() on a NULL pointer is a harmless no-op.
            _stmtHandle.sqlite3_finalize();
            _stmtHandle = null;
        }
    }

    public
    {
        bool empty() pure nothrow @nogc
        {
            return (this._status != SQLITE_ROW);
        }

        void popFront() @trusted
        {
            _status = _stmtHandle.sqlite3_step();

            if ((_status != SQLITE_ROW) && (_status != SQLITE_DONE))
            {
                enforce(
                    _status,
                    cast(immutable) _dbHandle.sqlite3_errmsg.fromStringz,
                    ResultCode.row,
                );
            }

            this.populateRow();
        }

        Row front() pure nothrow @nogc
        {
            return _front;
        }
    }

    public
    {
        void bind(int index, const bool value)
        {
            return this.bind(index, int(value));
        }

        void bind(int index, const byte value)
        {
            return this.bind(index, int(value));
        }

        void bind(int index, const ubyte value)
        {
            return this.bind(index, int(value));
        }

        void bind(int index, const short value)
        {
            return this.bind(index, int(value));
        }

        void bind(int index, const ushort value)
        {
            return this.bind(index, int(value));
        }

        void bind(int index, const int value) @trusted
        {
            this.resetIfNeeded();

            immutable x = _stmtHandle.sqlite3_bind_int(index + 1, value);
            enforce(x, "Parameter binding failed");
        }

        void bind(int index, const uint value) @trusted
        {
            return this.bind(index, long(value));
        }

        void bind(int index, const long value) @trusted
        {
            this.resetIfNeeded();

            immutable x = _stmtHandle.sqlite3_bind_int64(index + 1, value);
            enforce(x, "Parameter binding failed");
        }

        // WARNING: will store value as if it were signed (→ long)
        void bind(int index, const ulong value)
        {
            return this.bind(index, long(value));
        }

        void bind(int index, const double value) @trusted
        {
            this.resetIfNeeded();

            immutable x = _stmtHandle.sqlite3_bind_double(index + 1, value);
            enforce(x, "Parameter binding failed");
        }

        void bind(int index, const const(ubyte)[] value) @trusted
        {
            this.resetIfNeeded();

            immutable x = _stmtHandle.sqlite3_bind_blob64(index + 1, cast(void*) value.ptr, value.length, SQLITE_STATIC);
            enforce(x, "Parameter binding failed");
        }

        void bind(int index, const string value) @trusted
        {
            this.resetIfNeeded();

            immutable x = _stmtHandle.sqlite3_bind_text64(index + 1, value.ptr, value.length, SQLITE_STATIC, SQLITE_UTF8);
            enforce(x, "Parameter binding failed");
        }

        void bind(int index, const DateTime value)
        {
            return this.bind(
                index,
                format!formatDateTime(
                    value.year, value.month, value.day,
                    value.hour, value.minute, value.second),
            );
        }

        void bind(int index, const TimeOfDay value)
        {
            return this.bind(
                index,
                format!formatTime(value.hour, value.minute, value.second)
            );
        }

        void bind(int index, const Date value)
        {
            return this.bind(
                index,
                format!formatDate(value.year, value.month, value.day)
            );
        }

        void bind(int index, const typeof(null)) @trusted
        {
            this.resetIfNeeded();
            immutable x = _stmtHandle.sqlite3_bind_null(index + 1);
            enforce(x, "Parameter binding failed");
        }
    }

    private
    {
        void resetIfNeeded() @trusted
        {
            if (_status == 0)
                return;

            immutable r = _stmtHandle.sqlite3_reset();
            enforce(r, "Reset failed");
            _status = 0;
        }

        void populateRow() @trusted
        {
            int cntColumns = _stmtHandle.sqlite3_column_count;
            auto rowData = new DBValue[](cntColumns);

            foreach (n; 0 .. cntColumns)
            {
                int colType = _stmtHandle.sqlite3_column_type(n);
                final switch (colType)
                {
                case SQLITE_INTEGER:
                    rowData[n] = DBValue(_stmtHandle.sqlite3_column_int64(n));
                    break;

                case SQLITE_FLOAT:
                    rowData[n] = DBValue(_stmtHandle.sqlite3_column_double(n));
                    break;

                case SQLITE3_TEXT:
                    const sqliteText = _stmtHandle.sqlite3_column_text(n);
                    auto textSlice = sqliteText.fromStringz;
                    string textString = textSlice.idup;
                    rowData[n] = DBValue(textString);
                    break;

                case SQLITE_BLOB:
                    immutable cntBytes = _stmtHandle.sqlite3_column_bytes(n);
                    const blob = _stmtHandle.sqlite3_column_blob(n);
                    const blobBytes = cast(ubyte[]) blob[0 .. cntBytes];
                    const(ubyte)[] blobArray = blobBytes.idup;
                    rowData[n] = DBValue(blobArray);
                    break;

                case SQLITE_NULL:
                    rowData[n] = DBValue(null);
                    break;
                }
            }

            _front = Row(rowData);
        }
    }
}

static assert(isQueryCompiler!SQLite3);

private
{
pure:

    void joinToSQL(const Join[] joinClause, ref Appender!string sql)
    {
        foreach (join; joinClause)
        {
            final switch (join.type) with (Join)
            {
            case Type.invalid:
                assert(0, "Join.Type.invalid");

            case Type.inner:
                sql ~= ` JOIN "`;
                break;

            case Type.leftOuter:
                sql ~= ` LEFT OUTER JOIN "`;
                break;

            case Type.rightOuter:
                sql ~= ` RIGHT OUTER JOIN "`;
                break;

            case Type.fullOuter:
                sql ~= ` FULL OUTER JOIN "`;
                break;

            case Type.cross:
                sql ~= ` CROSS JOIN "`;
                break;
            }

            sql ~= escapeIdentifier(join.target.table.name);
            sql ~= `"`;

            if (join.target.name is null)
                return;

            sql ~= ` ON "`;
            sql ~= escapeIdentifier(join.target.table.name);
            sql ~= `"."`;
            sql ~= escapeIdentifier(join.target.name);
            sql ~= `" = "`;

            if (join.source.table.name !is null)
            {
                sql ~= escapeIdentifier(join.source.table.name);
                sql ~= `"."`;
            }

            sql ~= escapeIdentifier(join.source.name);
            sql ~= '"';
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
                sql ~= ` "`;
                (delegate() @trusted { sql ~= t.data.str.escapeIdentifier(); })();
                sql ~= `".`;
                break;
            case Type.column:
                if (prev != Type.columnTable)
                    sql ~= ' ';
                sql ~= '"';
                (delegate() @trusted { sql ~= t.data.str.escapeIdentifier(); })();
                sql ~= '"';
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
                sql ~= '"';
                sql ~= escapeIdentifier(term.column.table.name);
                sql ~= `".`;
            }
            sql ~= '"';
            sql ~= escapeIdentifier(term.column.name);
            sql ~= '"';

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
            sql ~= '"';
            sql ~= se.column.table.name;
            sql ~= `".`;
        }

        if (se.column.name == "*")
        {
            sql ~= '*';
        }
        else
        {
            sql ~= '"';
            sql ~= se.column.name.escapeIdentifier;
            sql ~= '"';
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

    string escapeIdentifier(string tableOrColumn) pure
    {
        import std.string : replace;

        return tableOrColumn.replace('"', `""`);
    }
}
