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
        `CREATE TABLE "person" (id INTEGER PRIMARY KEY, name TEXT, age INTEGER UNSIGNED)`);

    auto em = EntityManager!SQLite3(driver);

    {
        Person p;
        immutable bool success = (cast(SQLite3) driver).get!(Person)(1, p);
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

    /*{
        EntityCollection!Person ec = em.find!Person(
            (Query q) => q.where("age", ComparisonOperator.greaterThanOrEquals, 60)
        );
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
        EntityCollection!Person ec = em.find!(
            Person,
            (Query q) => q.where("age", ComparisonOperator.greaterThanOrEquals, 60)
        )();
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
        EntityCollection!Person ec = em.find!(
            Person,
            (Query q) => q.where("age", ComparisonOperator.greaterThanOrEquals)
        )(delegate(Statement stmt) { stmt.bind(0, 60); });
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
        EntityCollection!Person ec = em.find!(
            Person,
            (Query q) => q
                .where("age", ComparisonOperator.greaterThanOrEquals, 60)
                .limit(1)
        );
        assert(!ec.empty);

        Person p1 = ec.front;
        assert(p1.name == "Walter");
        assert(p1.age >= 60);

        ec.popFront();
        assert(ec.empty);
    }

    {
        EntityCollection!Person ec = em.find!(
            Person,
            (Query q) => q
                .where("age", ComparisonOperator.greaterThanOrEquals, 60)
                .limit(1, 1)
        );
        assert(!ec.empty);

        Person p1 = ec.front;
        assert(p1.name == "Carl");
        assert(p1.age >= 60);

        ec.popFront();
        assert(ec.empty);
    }*/

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
