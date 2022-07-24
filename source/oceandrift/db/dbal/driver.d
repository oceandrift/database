module oceandrift.db.dbal.driver;

import taggedalgebraic : TaggedAlgebraic;
public import std.datetime : Date, DateTime, TimeOfDay;

@safe:

interface DatabaseDriver
{
@safe:
    void connect();
    void close();
    bool connected();

    bool autoCommit();
    void autoCommit(bool enable);

    void transactionStart();
    void transactionCommit();
    void transactionRollback();

    void execute(string sql);
    Statement prepare(string sql);
}

interface Statement
{
@safe:
    void close();

    void bind(int index, bool value);
    void bind(int index, byte value);
    void bind(int index, ubyte value);
    void bind(int index, short value);
    void bind(int index, ushort value);
    void bind(int index, int value);
    void bind(int index, uint value);
    void bind(int index, long value);
    void bind(int index, ulong value);
    void bind(int index, double value);
    void bind(int index, const(ubyte)[] value);
    void bind(int index, string value);
    void bind(int index, DateTime value);
    void bind(int index, TimeOfDay value);
    void bind(int index, Date value);
    void bind(int index, typeof(null));

    void execute();

    bool empty();
    void popFront();
    Row front();
}

/// helper
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

import std.range : isInputRange;

static assert(isInputRange!Statement);

private union _DBValue
{
    bool bool_;
    byte byte_;
    ubyte ubyte_;
    short short_;
    ushort ushort_;
    int int_;
    uint uint_;
    long long_;
    ulong ulong_;
    double double_;
    const(ubyte)[] ubytes_;
    string string_;
    DateTime dateTime_;
    TimeOfDay timeOfDay_;
    Date date_;
    typeof(null) null_;
}

///
alias DBValue = TaggedAlgebraic!_DBValue;
public import taggedalgebraic : get;

///
struct Row
{
    DBValue[] _value;
    alias _value this;
}
