module oceandrift.db.maridb;

import mysql.safe;
import oceandrift.db.dbal.driver;
import std.conv : to;

@safe:

alias DBALRow = oceandrift.db.dbal.driver.Row;
alias MySQLRow = mysql.safe.Row;

class MariaDBDatabaseDriver : DatabaseDriver
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

    public this(string host, string username, string password, string database, ushort port = 3306)
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
    }

    public  // Extras
    {
        Connection getConnection()
        {
            return this._connection;
        }
    }
}

private mixin template bindImpl(T)
{
    void bind(int index, T value) @safe
    {
        _stmt.setArg(index - 1, value);
    }
}

class MariaDBStatement : Statement
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

DBALRow mysqlToDBAL(MySQLRow mysql)
{
    auto rowData = new DBValue[](mysql.length);

    for (size_t i = 0; i < mysql.length; ++i)
        rowData[i] = mysql[i].mysqlToDBAL();

    return oceandrift.db.dbal.driver.Row(rowData);
}

DBValue mysqlToDBAL(MySQLVal mysql)
{
    import taggedalgebraic : get, hasType;

    enum direct(T) = "if (mysql.hasType!("
        ~ T.stringof
        ~ ")()) return DBValue(mysql.get!("
        ~ T.stringof
        ~ ")());";

    enum indirect(T, TCast) = "if (mysql.hasType!("
        ~ T.stringof
        ~ ")()) return DBValue(cast("
        ~ TCast.stringof
        ~ ") mysql.get!("
        ~ T.stringof
        ~ ")());";

    mixin(direct!(typeof(null)));

    mixin(direct!ulong);
    mixin(direct!long);

    mixin(direct!uint);
    mixin(direct!int);

    mixin(direct!string);

    mixin(direct!ubyte);
    mixin(direct!byte);
    mixin(direct!short);
    mixin(direct!ushort);
    mixin(direct!bool);

    mixin(indirect!(float, double));
    mixin(direct!double);
    mixin(direct!DateTime);
    mixin(direct!TimeOfDay);
    mixin(direct!Date);
    mixin(direct!(const(ubyte)[]));
    mixin(indirect!(ubyte[], const(ubyte)[]));
    mixin(indirect!(const(char)[], const(ubyte)[]));

    if (mysql.hasType!Timestamp)
    {
        // This is just there as a precaution and will hopefully never be triggered.
        // No known bug.
        assert(0, "mysql-native caught lying:"
                ~ "«When TIMESTAMPs are retrieved as part of a result set it will be as DateTime structs.»"
        );
    }

    assert(0, "No DBAL conversion routine implemented for MySQL type: " ~ mysql.kind.to!string);
}
