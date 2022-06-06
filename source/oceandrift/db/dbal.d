module oceandrift.db.dbal;

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
}
