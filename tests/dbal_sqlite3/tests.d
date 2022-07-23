module tests.dbal_sqlite3.tests;

import oceandrift.db.dbal.driver;
import oceandrift.db.sqlite3;

@safe:

void print(SQLiteX ex) @trusted
{
    import std.stdio;

    stderr.writeln(
        "SQLite3 Exception; status code: ", cast(int) ex.code, " = ", ex.code,
        "; msg: ", ex.msg
    );
}

unittest
{
    try
    {
        DatabaseDriver driver = new SQLite3DatabaseDriver(":memory:", OpenMode.rw);
        assert(!driver.connected);

        driver.connect();
        assert(driver.connected);
        assert(driver.autoCommit() == true);

        driver.transactionStart();
        assert(driver.autoCommit() == false);

        driver.transactionRollback();
        assert(driver.autoCommit() == true);

        driver.close();
        assert(!driver.connected);
    }
    catch (SQLiteX ex)
    {
        ex.print();
        assert(0);
    }
}

unittest
{
    try
    {
        DatabaseDriver driver = new SQLite3DatabaseDriver(":memory:", OpenMode.create);
        assert(!driver.connected);

        driver.connect();
        assert(driver.connected);

        driver.execute(`CREATE TABLE "demo" ("id" INTEGER PRIMARY KEY, "col1" TEXT, "col2" TEXT)`);

        driver.transactionStart();

        Statement insert = driver.prepare(`INSERT INTO "demo" ("col1", "col2") VALUES (?, ?)`);
        scope (exit)
            insert.close();

        insert.bind(1, "asdf");
        insert.bind(2, "jklö");
        insert.execute();

        insert.bind(1, "qwer");
        insert.bind(2, "uiop");
        insert.execute();

        insert.bind(1, "yxcv");
        insert.bind(2, "bnm,");
        insert.execute();

        insert.bind(1, "qaz");
        insert.bind(2, "wsx");
        insert.execute();

        insert.bind(1, "edc");
        insert.bind(2, "rfv");
        insert.execute();

        Statement stmt = driver.prepare(`
            SELECT * FROM "demo"
                WHERE id > ?
                    AND id < ?
                ORDER BY id ASC
        `);
        scope (exit)
            stmt.close();
        stmt.bind(1, 2);
        stmt.bind(2, 5);
        stmt.execute();
        assert(!stmt.empty);

        Row row1 = stmt.front();
        assert(row1[0] == 3);
        assert(row1[1] == "yxcv");
        assert(row1[2] == "bnm,");

        stmt.popFront();
        assert(!stmt.empty);
        Row row2 = stmt.front();
        assert(row2[0] == 4);
        assert(row2[1] == "qaz");
        assert(row2[2] == "wsx");

        driver.transactionRollback();
        assert(driver.autoCommit() == true);

        stmt.execute();
        assert(stmt.empty, "Rollback broken");

        stmt.close();

        driver.close();
        assert(!driver.connected);
    }
    catch (SQLiteX ex)
    {
        ex.print();
        assert(0);
    }
}
