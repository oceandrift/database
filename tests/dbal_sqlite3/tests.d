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
        DatabaseDriver driver = new SQLite3DatabaseDriver(":memory:", OpenMode.create);
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
                WHERE
                    "id" > ?
                    AND "id" < ?
                ORDER BY
                    "id" ASC
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
        driver.connect();
        scope (exit)
            driver.close();

        driver.execute(`
            CREATE TABLE "person" (
                "id" INTEGER PRIMARY KEY,
                "name" TEXT,
                "nickname" TEXT,
                "age" INTEGER
            )
        `);

        Statement insert = driver.prepare(
            `INSERT INTO "person" ("name", "age", "nickname") VALUES (?, ?, ?)`
        );
        scope (exit)
            insert.close();

        insert.executeWith("Thomas", 21, "Tom");
        insert.executeWith("Daniel", 30, "Dan");

        insert.bind(1, "Jan");
        insert.bind(2, 22);
        insert.bind(3, "WebFreak");
        insert.execute();

        {
            Statement stmt = driver.prepare(`
                SELECT "id", "age" FROM "person"
                    WHERE
                        "age" >= ?
                    ORDER BY
                        "age" ASC
            `);
            scope (exit)
                stmt.close();

            stmt.bind(1, 22);
            stmt.execute();
            assert(!stmt.empty);

            Row row1 = stmt.front();
            assert(row1[0] == 3);

            stmt.popFront();
            assert(!stmt.empty);
            Row row2 = stmt.front();
            assert(row2[0] == 2);
            assert(row2[1] == 30);

            stmt.popFront();
            assert(stmt.empty);
        }

        {
            insert.executeWith("David", 35, "Dave");

            Statement stmtCount = driver.prepare(`SELECT COUNT(*) FROM "person"`);
            stmtCount.execute();
            assert(stmtCount.front()[0] == 4);
            stmtCount.popFront();
            assert(stmtCount.empty);
        }

        {
            Statement stmt = driver.prepare(
                `SELECT "nickname" FROM "person" WHERE "nickname" LIKE ?`
            );
            scope (exit)
                stmt.close();

            immutable string pattern = "Da%";
            stmt.executeWith(pattern);

            size_t cnt = 0;
            foreach (Row row; stmt)
            {
                string nickname = row[0].get!string;
                assert(nickname[0 .. 2] == "Da");
                ++cnt;
            }
            assert(cnt == 2);
        }
    }
    catch (SQLiteX ex)
    {
        ex.print();
        assert(0);
    }
}

unittest
{
    import std.algorithm : canFind;
    import std.random : rndGen;

    try
    {
        DatabaseDriver driver = new SQLite3DatabaseDriver(":memory:", OpenMode.create);
        driver.connect();
        scope (exit)
            driver.close();

        driver.execute(`
            CREATE TABLE "misc" (
                "id" INTEGER PRIMARY KEY,
                "blob" BLOB
            )
        `);

        ubyte[] blablabla;
        for (size_t n = 0; n < 20; ++n)
        {
            blablabla ~= ubyte(rndGen.front % ubyte.max);
            rndGen.popFront();
        }

        {
            Statement insert = driver.prepare(`INSERT INTO "misc" ("blob") VALUES (?)`);
            insert.executeWith(blablabla);
        }

        {
            Statement select = driver.prepare(`SELECT "blob" FROM "misc"`);
            scope (exit)
                select.close();

            select.execute();
            assert(!select.empty);

            Row row = select.front;
            assert(row[0] == blablabla);

            select.popFront();
            assert(select.empty);
        }

    }
    catch (SQLiteX ex)
    {
        ex.print();
        assert(0);
    }
}
