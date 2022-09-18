module tests.dbal_sqlite3.tests;

import oceandrift.db.dbal;
import oceandrift.db.sqlite3;
import oceandrift.db.orm;

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

struct Person
{
    ulong id = 0;
    string name;
    ubyte age;
}

unittest
{
    auto driver = new SQLite3(":memory:", OpenMode.create);
    driver.connect();
    scope (exit)
        driver.close();

    driver.execute(
        `CREATE TABLE "person" (id INTEGER PRIMARY KEY, name TEXT, age INTEGER UNSIGNED)`
    );

    auto em = EntityManager!SQLite3(driver);

    {
        Person p;
        immutable bool success = driver.get!(Person)(1, p);
        assert(!success);
    }

    {
        Person p;
        immutable bool success = em.get!(Person)(1, p);
        assert(!success);
    }

    driver.execute(
        `INSERT INTO "person" (name, age) VALUES ("Walter", 65), ("Tom", 30), ("Carl", 60)`
    );

    {
        Person p;
        immutable bool success = em.get!Person(1, p);
        assert(success);
        assert(p.name == "Walter");
        assert(p.age == 65);
    }

    {
        EntityCollection!Person ec =
            em.find!Person()
            .where("age", ComparisonOperator.greaterThanOrEquals, 60)
            .select()
            .via(driver);
        assert(!ec.empty);

        Person p1 = ec.front;
        assert(p1.name == "Walter");
        assert(p1.age >= 60);

        ec.popFront();
        assert(!ec.empty);

        Person p2 = ec.front;
        assert(p2.name == "Carl", p2.name);
        assert(p2.age >= 60);

        ec.popFront();
        assert(ec.empty);
    }

    {
        EntityCollection!Person ec = em
            .find!Person()
            .where("age", ComparisonOperator.greaterThanOrEquals, 60)
            .select()
            .via(driver);
        assert(!ec.empty);

        Person p1 = ec.front;
        assert(p1.name == "Walter");
        assert(p1.age >= 60);

        ec.popFront();
        assert(!ec.empty);

        Person p2 = ec.front;
        assert(p2.name == "Carl");
        assert(p2.age >= 60);

        ec.popFront();
        assert(ec.empty);
    }

    {
        EntityCollection!Person ec = em
            .find!Person()
            .whereParentheses((Query q) => q
                    .where("age", ComparisonOperator.greaterThanOrEquals, 60)
            )
            .select()
            .via(driver);
        assert(!ec.empty);

        Person p1 = ec.front;
        assert(p1.name == "Walter");
        assert(p1.age >= 60);

        ec.popFront();
        assert(!ec.empty);

        Person p2 = ec.front;
        assert(p2.name == "Carl");
        assert(p2.age >= 60);

        ec.popFront();
        assert(ec.empty);
    }

    {
        EntityCollection!Person ec = em
            .find!Person()
            .whereParentheses(q => q
                    .where("age", ComparisonOperator.greaterThanOrEquals, 60)
                    .limit(1)
            )
            .select()
            .via(driver);
        assert(!ec.empty);

        Person p1 = ec.front;
        assert(p1.name == "Walter");
        assert(p1.age >= 60);

        ec.popFront();
        assert(ec.empty);
    }

    {
        EntityCollection!Person ec = em
            .find!Person()
            .whereParentheses(
                (Query q) => q
                    .where("age", ComparisonOperator.greaterThanOrEquals, 60)
                    .limit(1, 1)
            )
            .select()
            .via(driver);
        assert(!ec.empty);

        Person p1 = ec.front;
        assert(p1.name == "Carl");
        assert(p1.age >= 60);

        ec.popFront();
        assert(ec.empty);
    }

    {
        Person p;
        immutable bool success = em.get!Person(2, p);
        assert(success);
        assert(p.name == "Tom");
        assert(p.age == 30);

        em.remove(p);
        Person p2;
        immutable bool notRemoved = em.get!Person(2, p2);
        assert(!notRemoved);

        Person p3;
        immutable bool p3stillThere = em.get!Person(3, p3);
        assert(p3stillThere);
        assert(p3.name == "Carl");
    }

    {
        auto p = Person(0, "Peter", 45);

        em.save(p);
        assert(p.id == 4); // 1-4 is used by previous rows

        Person tmp;
        immutable personFound = em.get!Person(4, tmp);
        assert(personFound);

        p.age += 1;
        em.save(p);
        assert(p.id == 4); // ID remains unchanged

        Person tmp2;
        immutable personFoundAgain = em.get!Person(p.id, tmp2);
        assert(personFoundAgain);
        assert(tmp2.age == 46); // update propagated

        immutable idxCopy = em.store(p); // store a copy
        assert(idxCopy == 5);

        em.update(p);
    }
}

class Mountain
{
    ulong id = 0;
    string name;
    string location;
    uint height;

    public this() // ORM
    {
    }

    public this(string name, string location, uint height)
    {
        this.name = name;
        this.location = location;
        this.height = height;
    }
}

static assert(isEntityType!Mountain);

unittest
{

    auto db = new SQLite3(":memory:", OpenMode.create);
    db.connect();
    scope (exit)
        db.close();

    db.execute(
        `CREATE TABLE "mountain" (id INTEGER PRIMARY KEY, name TEXT, location TEXT, height INTEGER UNSIGNED)`
    );

    auto em = EntityManager!SQLite3(db);

    {
        auto mt1 = new Mountain("Hill 1", "Nowhere", 3000);
        em.save(mt1);
        auto mt2 = new Mountain("Snowmountain", "Elsewhere", 4111);
        em.save(mt2);
        auto mt3 = new Mountain("Mt. Skyscrape", "Somewhere", 4987);
        em.save(mt3);
        auto mt4 = new Mountain("Little Hill", "Nowhere", 1200);
        em.save(mt4);
        auto mt5 = new Mountain("K2010", "Nowhere", 2010);
        em.save(mt5);
        auto mt6 = new Mountain("Icy Heights", "Elsewhere", 3201);
        em.save(mt6);
        auto mt7 = new Mountain("Mt. Nowhere", "Nowhere", 6408);
        em.save(mt7);

        Statement stmt = db.prepare("SELECT COUNT(*) FROM mountain");
        stmt.execute();
        assert(stmt.front[0].getAs!int == 7);
    }

    {
        enum PreCollection!(Mountain, SQLite3) pcMt4000 =
            em.find!Mountain()
                .where("height", ComparisonOperator.greaterThanOrEquals, 4000);

        enum BuiltPreCollection!Mountain bpcMt4000 = pcMt4000.select();

        EntityCollection!Mountain mt4000 = db.map(bpcMt4000);

        assert(!mt4000.empty);

        int n = 0;
        foreach (Mountain mt; mt4000)
        {
            assert(mt.height >= 4000);
            ++n;
        }
        assert(n == 3);
    }

    {
        static immutable int cap = 3000;
        static immutable string loc = "Nowhere";
        PreCollection!(Mountain, SQLite3) pc = em.find!Mountain()
            .where("height", '<', cap)
            .where("location", '=', loc);

        assert(pc.count(db) == 2);
    }
}
