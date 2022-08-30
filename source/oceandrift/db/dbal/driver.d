/++
    Database “drivers” abstract database specific implementation details through a universal interface.

    This approach allows to build higher level abstractions on top that are independant from the underlying database.


    ## User’s Guide

    $()Well, almost: Different database implementations will stiff use different SQL dialects for their queries.

    returns a so-called $(I Prepared Statement) compiled from the provided SQL code.


    ### Examples

    #### Basic usage

    ---
    // connection setup (driver-specific!)
    DatabaseDriver db = new SQLite3DatabaseDriver();

    // disconnect on exit
    // alternative: you could also call .close() manually later instead
    scope(exit) db.close();

    // establish a fresh database connection
    db.connect();

    // use .connected to check whether there is an active connection
    if (!db.connected) {
        writeln("Disconnected :(");
    }
    ---


    #### Accessing data

    through prepared Statements

    ---
    // prepare a new statement
    Statement stmt = db.prepare("SELECT * FROM mytable WHERE id >= ? AND id < ?");

    // cleanup on exit
    scope(exit) stmt.close();

    // set // dynamic parameters are 1-indexed, first param is 1 (not zero!)
    stmt.bind(1, 200);
    stmt.bind(1, 400);

    // execute the prepared statement
    stmt.execute();

    // is there a result?
    if (!stmt.empty) {
        // - yes
        Row row1 = stmt.front;
        writeln(row1);
    }
    else {
        // - no
        writeln("No data found");
    }

    // advance to the next row
    stmt.popFront();

    const s = (!stmt.empty) ? "a" : "no";
    writeln("There is ", s, " 2nd row");
    ---

    ---
    // bind'n'execute helper
    stmt.executeWith(30, 90);

    // loop over the result (Statement is an InputRange)
    foreach(Row row; stmt) {
        const firstColumn = row[0];
        writeln("Entry found: ", firstColumn);
    }
    ---


    #### Transactions

    ---
    // begin a new database transaction (assuming we’re connected to a transactional database!)
    db.transactionStart();

    // […] do stuff here

    if (saveChanges)
        db.transactionCommit(); // either commit the current transaction…
    else
        db.transactionRollback(); // …or roll it back
    ---


    ### Prepared Statements

    Most database access in $(I oceandrift) happens through Prepared Statements.

    Prepared Statements are compiled before execution
    and allow the user of dynamic parameters (see: [Stattement.bind]).
    This implies there’s no need for manual quoting or escaping with regard to dynamic parameter.s
    In such a prepared statement actual values are substituted by “placeholders” (usually in form of a question mark `?` character).

    $(TIP
        Reuse of prepared statements (for multiple executions) can improve performance.
        This is because the SQL query needs to parsed only once
        and the database engine can also reuse the query plan it determined etc.
    )

    Unlike in many other database libraries there’s no “vanilla” `.query` function.
    This also means there’s no need for a SQL string escape function.




    ## Driver Developer’s Guide

    $(NOTE
        Feel free to skip this chapters if you don’t plan on implementing your own database driver(s).
    )

    In order to add support for another database implementation/engine/whatsoever,
    create a new driver class implementing the [DatabaseDriver] interface.

    Connection setup should happen via its constructor,
    but do not establish a connection yet;
    that’s what the [DatabaseDriver.connect] method is for.

    Provide the current connection status via the [DatabaseDriver.connected] method.

    Clean connection shutdown happens through a call to [DatabaseDriver.close].
    Disconnect the underlying database connection there.
    Beware a user may call [DatabaseDriver.connect] later of course.

    If the underlying database supports toggling “auto commit” mode,
    this functionality should be provided via [DatabaseDriver.autoCommit(bool)].
    There’s also a corresponding getter method.

    Manual transaction control is done through:
    $(LIST
        * [DatabaseDriver.transactionStart]
            – starts/begins a new transaction

            might commit a currently active transaction, just stick to your database’s active defaults;
            usually done by executing a SQL statement like `BEGIN TRANSACTION;`
        * [DatabaseDriver.transactionCommit]
            – commit/save the current transaction

            usually done by executing a SQL statement like `COMMIT;`
        * [DatabaseDriver.transactionRollback]
            – rollback/abort the current transaction

            usually done by executing a SQL statement like `ROLLBACK;`
    )
    For non-transactional databases you probably want to throw an Exception here.

    [DatabaseDriver.execute] is a method that takes any SQL statement
    that (usually) shouldn’t return any data (e.g. `CREATE TABLE …`)
    and immediatly executes it.
    Indicate errors by throwing an appropriate [Exception].
    In other database libraries such functions would often return the number of affected rows or similar,
    $(I oceandrift) currently defines this function as a `void` one; might be subject to change.

    [DatabaseDriver.prepare]: compile the passed SQL statement and return a Prepared Statement.
    Preparation (compilation) errors should be indicated by throwing an appropriate [Exception];
    You must not return [null].


    ### Prepared Statements (Driver)

    Prepared Statements are represented by instances of [Statement].
    Create a class that implements said interface and wrap your database specific statement implementation.

    #### Dynamic Parameters (Driver)

    Dynamic parameters are set via the [Statement.bind] overloads.
    If the underlying database clients expects zero(0)-indexed indices, substract `1` from the passed index number.
    For datatypes your database engine doesn’t support, convert them to alternatives where appropriate.
    Perhaps [string] can act as a last resort catch-all solution.

    #### Statement Exeuction (Driver)

    When [Statement.execute] is called, execute the prepared statement with the currently bound parameters.
    Make the first result row available as `.front` if available (otherwise `.empty` should evaluate to [true]).

    $(PITFALL
        Ensure Row does not point to any memory owned by the database driver.
        Memory could get corrupted when references to it outlive the statement handle.
        `.idup` is your friend.
    )

    Statements form an $(B InputRange).
    $(LIST
        * [Statement.front]
            – each range element represents an individual row
        * [Statement.popFront] advances the cursor to the next row.
        * [Statement.empty] indicates whether `.front` has no further row loaded.
            For empty result sets this should be immediatly set to return [true].
    )

    Regarding such query results:
    $(LIST
        * Convert column values to a compatible type of the [DBValue] tagged algebraic (“union”)
        * [Row] is just a struct that wraps an array of [DBValue]s.
            (This approach leads to better readable error messages than an plain alias.)

    Similar to [DatabaseDriver] there’s a [Statement.close] method.
    Finalize/close the underlying prepared statement and do any necessary cleanup.

    $(NOTE
        Special thanks to Paul “Snarwin” Backus.
    )
 +/
module oceandrift.db.dbal.driver;

import std.range : isInputRange;
import std.sumtype;

public import std.datetime : Date, DateTime, TimeOfDay;

@safe:

/++
    Database Driver Handle

    Provides a unified interface to the underlying database client implementation.

    See_Also:
        Check out [oceandrift.db.dbal.driver] for usage details
 +/
interface DatabaseDriver
{
    @safe
    {
        /++
            Establishes a fresh connection to the database
         +/
        void connect();

        /++
            Shutdown the database current connection

            Idiomatically used with a scope guard:
            ---
            // DatabaseDriver db = …;
            db.connect();
            scope(exit) db.close();
            ---
         +/
        void close();

        /++
            Gets the current connection status

            Returns:
                true = connection active
         +/
        bool connected();
    }

    @safe
    {
        /++
            Determines whether “auto commit” is enabled for the current database connection
         +/
        bool autoCommit();

        /++
            Enables/disables “auto commit”
            (if available on the underlying database)
         +/
        void autoCommit(bool enable);

        /++
            Begins a database transaction
            (Requires support by the underlying database, of course)
         +/
        void transactionStart();

        /++
            Commits the current transaction
         +/
        void transactionCommit();

        /++
            Rolls back the current transaction
         +/
        void transactionRollback();
    }

    @safe
    {
        /++
            Executes the provided SQL statement

            Not intended to be used with querys.
            Result does not get fetched (or is discarded immediatly).

            Params:
                sql = SQL statement to execute

            Throws:
                Exception on failure (specific Exception type(s) may vary from implementation to implementation)
         +/
        void execute(string sql);

        /++
            Prepares the passed SQL statement

            Params:
                sql = SQL statement to prepare

            Throws:
                Exception on failure (specific Exception type(s) may vary from implementation to implementation)
         +/
        Statement prepare(string sql);

        DBValue lastInsertID();
    }
}

/++
    Prepared Statement Handle
 +/
interface Statement
{
@safe:
    void close();

    /++
        Binds the passed value to the specified dynamic parameter

        Params:
            index = 1-indexed index-number of the dynamic parameter to bind to
            value = value to bind
     +/
    void bind(int index, const bool value);
    /// ditto
    void bind(int index, const byte value);
    /// ditto
    void bind(int index, const ubyte value);
    /// ditto
    void bind(int index, const short value);
    /// ditto
    void bind(int index, const ushort value);
    /// ditto
    void bind(int index, const int value);
    /// ditto
    void bind(int index, const uint value);
    /// ditto
    void bind(int index, const long value);
    /// ditto
    void bind(int index, const ulong value);
    /// ditto
    void bind(int index, const double value);
    /// ditto
    void bind(int index, const const(ubyte)[] value);
    /// ditto
    void bind(int index, const string value);
    /// ditto
    void bind(int index, const DateTime value);
    /// ditto
    void bind(int index, const TimeOfDay value);
    /// ditto
    void bind(int index, const Date value);
    /// ditto
    void bind(int index, const typeof(null));

    /++
        Executes the prepared statement using the currently bound values

        Access the result through the $(I InputRange) interface of the [Statement] type.
        Check whether there are any rows via the `.empty` property.
     +/
    void execute();

    /++
        Determines whether there are any rows loaded (or left).
     +/
    bool empty();

    /++
        Advance to the next row

        Makes the next row available via `.front`.

        $(NOTE This is an $(I InputRange). Do not call `popFront()` on empty ranges.)
     +/
    void popFront();

    /++
        Currently fetched (loaded) row

        $(NOTE This is an $(I InputRange). Do not call `front()` on empty ranges.)
     +/
    Row front();
}

void bindDBValue(Statement stmt, int index, const DBValue value)
{
    value.match!(
        (ref const typeof(null) value) => stmt.bind(index, value),
        (ref const bool value) => stmt.bind(index, value),
        (ref const byte value) => stmt.bind(index, value),
        (ref const ubyte value) => stmt.bind(index, value),
        (ref const short value) => stmt.bind(index, value),
        (ref const ushort value) => stmt.bind(index, value),
        (ref const int value) => stmt.bind(index, value),
        (ref const uint value) => stmt.bind(index, value),
        (ref const long value) => stmt.bind(index, value),
        (ref const ulong value) => stmt.bind(index, value),
        (ref const double value) => stmt.bind(index, value),
        (ref const const(ubyte)[] value) => stmt.bind(index, value),
        (ref const string value) => stmt.bind(index, value),
        (ref const DateTime value) => stmt.bind(index, value),
        (ref const TimeOfDay value) => stmt.bind(index, value),
        (ref const Date value) => stmt.bind(index, value),
    );
}

static assert(isInputRange!Statement);

/++
    Executes a «Prepared Statement» after binding the specified parameters to it

    $(TIP
        Best used with $(B UFCS):

        ---
        stmt.executeWith(param1, param2);
        ---
    )

    $(SIDEBAR
        UFCS is also the reason why this isn’t named just `execute`.
        It wouldn’t work that way because [Statement] already defines a method with said name.

        Moving this function into the Statement interface would bloat it for no good reason.
    )

    $(WARNING
        If called with less arguments than the Statement has parameters (excluding the statement itself, obviously),
        the rest of the parameters will retain their current values.

        This might lead to unexpected results. At least, it’s bad pratice.

        Future versions may check for this and error when there’s a mismatch in paramter count.
    )
 +/
void executeWith(Args...)(Statement stmt, Args bindValues)
{
    int idx = 1;
    foreach (val; bindValues)
    {
        stmt.bind(idx, val);
        ++idx;
    }

    stmt.execute();
}

/++
    Database Value (representing a row’s column)
 +/
alias DBValue = SumType!(
    typeof(null),
    bool,
    byte,
    ubyte,
    short,
    ushort,
    int,
    uint,
    long,
    ulong,
    double,
    const(ubyte)[],
    string,
    DateTime,
    TimeOfDay,
    Date,
);

alias null_t = typeof(null);

T get(T)(DBValue value)
{
    return value.tryMatch!((T t) => t);
}

bool isNull(DBValue value)
{
    try
    {
        return value.tryMatch!((typeof(null)) => true);
    }
    catch (MatchException)
    {
        return false;
    }
}

/++
    Database Result Row
 +/
struct Row
{
    DBValue[] _value;
    alias _value this;
}
