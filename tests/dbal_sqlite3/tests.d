module tests.dbal_sqlite3.tests;

import oceandrift.db.dbal;
import oceandrift.db.sqlite3;

@safe:

void print(SQLiteX ex) @trusted
{
    import std.stdio;

    stderr.writeln(
        "SQLite3 Exception; status code: ", cast(int) ex.code, " = ", ex.code,
        "; msg: ", ex.msg,
        "\n\tEX Trace:", ex.info, "\n\t--- End of Trace"
    );
}

unittest
{
    try
    {
        auto driver = new SQLite3(":memory:", OpenMode.create);
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
        auto driver = new SQLite3(":memory:", OpenMode.create);
        assert(!driver.connected);

        driver.connect();
        assert(driver.connected);

        driver.execute(`CREATE TABLE "demo" ("id" INTEGER PRIMARY KEY, "col1" TEXT, "col2" TEXT)`);

        driver.transactionStart();

        Statement insert = driver.prepare(`INSERT INTO "demo" ("col1", "col2") VALUES (?, ?)`);
        scope (exit)
            insert.close();

        insert.bind(0, "asdf");
        insert.bind(1, "jklö");
        insert.execute();

        insert.bind(0, "qwer");
        insert.bind(1, "uiop");
        insert.execute();

        insert.bind(0, "yxcv");
        insert.bind(1, "bnm,");
        insert.execute();

        insert.bind(0, "qaz");
        insert.bind(1, "wsx");
        insert.execute();

        insert.bind(0, "edc");
        insert.bind(1, "rfv");
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

        stmt.bind(0, 2);
        stmt.bind(1, 5);
        stmt.execute();
        assert(!stmt.empty);

        Row row1 = stmt.front();
        assert(row1[0].get!long == 3);
        assert(row1[1].get!string == "yxcv");
        assert(row1[2].get!string == "bnm,");

        stmt.popFront();
        assert(!stmt.empty);
        Row row2 = stmt.front();
        assert(row2[0].get!long == 4);
        assert(row2[1].get!string == "qaz");
        assert(row2[2].get!string == "wsx");

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
        auto driver = new SQLite3(":memory:", OpenMode.create);
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

        insert.bind(0, "Jan");
        insert.bind(1, 22);
        insert.bind(2, "WebFreak");
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

            stmt.bind(0, 22);
            stmt.execute();
            assert(!stmt.empty);

            Row row1 = stmt.front();
            assert(row1[0].get!long == 3);

            stmt.popFront();
            assert(!stmt.empty);
            Row row2 = stmt.front();
            assert(row2[0].get!long == 2);
            assert(row2[1].get!long == 30);

            stmt.popFront();
            assert(stmt.empty);
        }

        {
            insert.executeWith("David", 35, "Dave");

            Statement stmtCount = driver.prepare(`SELECT COUNT(*) FROM "person"`);
            stmtCount.execute();
            assert(stmtCount.front()[0].get!long == 4);
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
        auto driver = new SQLite3(":memory:", OpenMode.create);
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
            assert(row[0].get!(const(ubyte)[]) == blablabla);

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

public
{
    private import oceandrift.db.dbal.v4;

    // Query builder

    unittest
    {
        try
        {
            auto driver = new SQLite3(":memory:");

            driver.connect();
            scope (exit)
                driver.close();

            driver.execute(`
                CREATE TABLE "misc" (
                    "id" INTEGER PRIMARY KEY,
                    "name" TEXT NOT NULL,
                    "age" INTEGER NOT NULL
                )
            `);

            enum a = table("misc").qb
                    .where("id", ComparisonOperator.greaterThan)
                    .where("age", '>')
                    .select("*", count!distinct("name")); //.where("id", '>').where("name", like);

            enum BuiltQuery bs = SQLite3.build(a);
            assert(
                bs.sql == `SELECT *, count(DISTINCT "name") FROM "misc" WHERE "id" > ? AND "age" > ?`
            );

            Statement stmt = driver.prepareBuiltQuery(bs);
            stmt.execute();
            assert(!stmt.empty());
            assert(stmt.front[0].isNull);
            assert(stmt.front[1].isNull);
            assert(stmt.front[2].isNull);
            assert(stmt.front[3].get!long == 0);

            enum b = table("misc").qb
                    .where("id", ComparisonOperator.lessThan)
                    .whereParentheses(q => q
                            .where("age", '>')
                            .where!or("age", '<')
                    )
                    .select("*").build!SQLite3;
            assert(b.sql == `SELECT * FROM "misc" WHERE "id" < ? AND ( "age" > ? OR "age" < ? )`);

            /*auto c = table("misc").qb
                .where("age", '>', DBValue(60))
                .select("*"); //.where("id", '>').where("name", like);

            auto d = table("misc").qb
                .where("age", '>', 60)
                .select(count("*")); //.where("id", '>').where("name", like);*/

            enum e1 = table("misc").qb
                    .where("age", '>', 0)
                    .limit(12);
            enum e = e1
                    .select("id").build!SQLite3;

            Statement eStmt = driver.prepareBuiltQuery(e);
            eStmt.execute();
            assert(eStmt.empty);

            assert(e.sql == `SELECT "id" FROM "misc" WHERE "age" > ? LIMIT ?`);
        }
        catch (SQLiteX ex)
        {
            ex.print();
            assert(0);
        }
    }

    unittest
    {
        enum BuiltQuery bq = table("x").qb.select().build!SQLite3;
        assert(bq.sql == `SELECT * FROM "x"`);
    }

    unittest
    {
        Query qMountainsGreaterThanInUSorCA = table("mountain").qb
            .where("height", '>')
            .whereParentheses(q => q
                    .where("location", '=', "US")
                    .where!or("location", '=', "CA")
            );

        Select selectQ = qMountainsGreaterThanInUSorCA.select("*");

        BuiltQuery bq = selectQ.build!SQLite3();

        assert(
            bq.sql == `SELECT * FROM "mountain" WHERE "height" > ? AND ( "location" = ? OR "location" = ? )`
        );
        assert(bq.preSets.where[1].get!string == "US");
        assert(bq.preSets.where[2].get!string == "CA");
        assert(bq.placeholders.where == 3);
        assert(bq.preSets.limit.isNull);
    }

    unittest
    {
        enum Update updateQ = table("mountain").qb.update("name", "location", "height");
        enum BuiltQuery bq = updateQ.build!SQLite3();
        assert(bq.sql == `UPDATE "mountain" SET "name" = ?, "location" = ?, "height" = ?`);
    }

    unittest
    {
        enum BuiltQuery bq = table("mountain").qb
                .where("height", ComparisonOperator.greaterThanOrEquals, 8000)
                .update("name")
                .build!SQLite3();
        assert(bq.sql == `UPDATE "mountain" SET "name" = ? WHERE "height" >= ?`);
        assert(bq.preSets.where[0].get!int == 8000);
    }

    unittest
    {
        enum BuiltQuery bq = table("mountain").qb
                .whereParentheses(
                    q => q
                        .where("location", '=', "US")
                        .where!or("name", ComparisonOperator.notEquals)
                )
                .limit(4)
                .update("category", "notes")
                .build!SQLite3();

        assert(
            bq.sql
                == `UPDATE "mountain" SET "category" = ?, "notes" = ? WHERE ( "location" = ? OR "name" <> ? ) LIMIT ?`
        );
        assert(bq.preSets.limit == 4);
        assert(bq.preSets.where[0].get!string == "US");
        assert(bq.placeholders.where == 2);
    }

    unittest
    {
        enum Insert insertQ = table("mountain").insert("name", "location", "height");
        assert(insertQ.rowCount == 1);

        enum BuiltQuery bq = insertQ.build!SQLite3();
        assert(bq.sql == `INSERT INTO "mountain" ("name", "location", "height") VALUES (?,?,?)`);
    }

    unittest
    {
        enum BuiltQuery bq =
            table("mountain")
                .insert("name", "location", "height")
                .times(2)
                .build!SQLite3();

        assert(
            bq.sql == `INSERT INTO "mountain" ("name", "location", "height") VALUES (?,?,?), (?,?,?)`
        );
    }

    unittest
    {
        enum BuiltQuery bq =
            table("mountain")
                .insert("name", "location", "height")
                .times(3)
                .build!SQLite3();

        assert(
            bq.sql == `INSERT INTO "mountain" ("name", "location", "height") VALUES (?,?,?), (?,?,?), (?,?,?)`
        );
    }

    unittest
    {
        enum BuiltQuery bq =
            table("mountain")
                .insert("name")
                .build!SQLite3();

        assert(bq.sql == `INSERT INTO "mountain" ("name") VALUES (?)`);
    }

    unittest
    {
        enum BuiltQuery bq =
            table("mountain")
                .insert()
                .build!SQLite3();

        assert(bq.sql == `INSERT INTO "mountain" DEFAULT VALUES`);
    }

    @system unittest
    {
        bool assertion = false;
        try
        {
            table("mountain")
                .insert()
                .times(3)
                .build!SQLite3();
        }
        catch (Error)
        {
            assertion = true;
        }
        finally
        {
            assert(assertion);
        }
    }

    unittest
    {
        enum Delete deleteQ = table("mountain").qb.delete_();
        enum BuiltQuery bq = deleteQ.build!SQLite3();
        assert(bq.sql == `DELETE FROM "mountain"`);
    }

    unittest
    {
        enum BuiltQuery bq = table("mountain").qb
                .where("height", ComparisonOperator.isNull)
                .delete_()
                .build!SQLite3();
        assert(bq.sql == `DELETE FROM "mountain" WHERE "height" IS NULL`);
    }

    unittest
    {
        enum BuiltQuery bq = table("mountain").qb
                .where("height", '<', 2000)
                .where!or("height", ComparisonOperator.isNull)
                .delete_()
                .build!SQLite3();
        assert(bq.sql == `DELETE FROM "mountain" WHERE "height" < ? OR "height" IS NULL`);
        assert(bq.preSets.where[0].get!int == 2000);
    }

    unittest
    {
        enum BuiltQuery bq = table("mountain").qb
                .where("height", ComparisonOperator.isNotNull)
                .whereParentheses(q => q
                        .whereParentheses(q => q
                            .where("location", '=')
                            .where!or("location", ComparisonOperator.like)
                        )
                        .where("snow_top", '=')
                )
                .delete_()
                .build!SQLite3();
        assert(
            bq.sql == `DELETE FROM "mountain" WHERE "height" IS NOT NULL AND ( ( "location" = ? OR "location" LIKE ? ) AND "snow_top" = ? )`
        );
    }

    unittest
    {
        enum BuiltQuery bq =
            table("mountain").qb
                .limit!true()
                .delete_()
                .build!SQLite3();
        assert(bq.sql == `DELETE FROM "mountain" LIMIT ? OFFSET ?`);
    }

    unittest
    {
        enum BuiltQuery bq =
            table("point").qb
                .limit(20, 40)
                .where("x", '>', 10)
                .where("y", ComparisonOperator.lessThanOrEquals, 0)
                .select("x", "y")
                .build!SQLite3();
        assert(bq.sql == `SELECT "x", "y" FROM "point" WHERE "x" > ? AND "y" <= ? LIMIT ? OFFSET ?`);
        assert(bq.preSets.where[0].get!int == 10);
        assert(bq.preSets.where[1].get!int == 0);
        assert(bq.preSets.limit == 20);
        assert(bq.preSets.limitOffset == 40);
    }

    unittest
    {
        enum BuiltQuery bq =
            table("mountain").qb
                .orderBy("height")
                .select()
                .build!SQLite3();
        assert(bq.sql == `SELECT * FROM "mountain" ORDER BY "height"`);

        enum BuiltQuery bqDesc =
            table("mountain").qb
                .orderBy("height", true)
                .select()
                .build!SQLite3();
        assert(bqDesc.sql == `SELECT * FROM "mountain" ORDER BY "height" DESC`);
    }

    unittest
    {
        enum BuiltQuery bq =
            table("mountain").qb
                .orderBy("height")
                .select()
                .build!SQLite3();
        assert(bq.sql == `SELECT * FROM "mountain" ORDER BY "height"`);

        enum BuiltQuery bqDesc =
            table("mountain").qb
                .orderBy("height", true)
                .select()
                .build!SQLite3();
        assert(bqDesc.sql == `SELECT * FROM "mountain" ORDER BY "height" DESC`);
    }

    unittest
    {
        enum BuiltQuery bq =
            table("mountain").qb
                .where("location", ComparisonOperator.notEquals)
                .orderBy("height")
                .limit(10)
                .select()
                .build!SQLite3();

        assert(bq.sql == `SELECT * FROM "mountain" WHERE "location" <> ? ORDER BY "height" LIMIT ?`);
    }

    unittest
    {
        enum BuiltQuery bq =
            table("mountain").qb
                .orderBy("height")
                .orderBy("name", true)
                .orderBy("location", true)
                .select()
                .build!SQLite3();

        assert(bq.sql == `SELECT * FROM "mountain" ORDER BY "height", "name" DESC, "location" DESC`);

        enum BuiltQuery bq2 =
            table("mountain").qb
                .orderBy("height")
                .orderBy("name", true)
                .orderBy("location", false)
                .select()
                .build!SQLite3();

        assert(bq2.sql == `SELECT * FROM "mountain" ORDER BY "height", "name" DESC, "location"`);
    }
}
