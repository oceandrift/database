module oceandrift.db.dbal.driver;

import taggedalgebraic.taggedalgebraic;

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
    void bind(int index, short value);
    void bind(int index, ushort value);
    void bind(int index, int value);
    void bind(int index, uint value);
    void bind(int index, long value);
    void bind(int index, ulong value);
    void bind(int index, double value);
    void bind(int index, ubyte[] value);
    void bind(int index, string value);
    void bind(int index, typeof(null));

    void execute();

    bool empty();
    void popFront();
    Row front();
}

unittest
{
    import std.range : isInputRange;

    static assert(isInputRange!SQLite3Statement);
}

private union _DBValue
{
    bool bool_;
    short short_;
    ushort ushort_;
    int int_;
    uint uint_;
    long long_;
    ulong ulong_;
    double double_;
    ubyte[] ubytes_;
    string string_;
    typeof(null) null_;
}

///
alias DBValue = TaggedAlgebraic!_DBValue;

///
struct Row
{
    DBValue[] _value;
    alias _value this;
}
