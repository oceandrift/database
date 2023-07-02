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
            .whereParentheses(q => q
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
        enum PreCollection!(Mountain, SQLite3) pc = em.find!Mountain()
                .where("height", '<', cap)
                .where("location", '=', loc);

        immutable ulong cnt = pc.countVia(db);
        assert(cnt == 2);

        enum BuiltQuery qCntCT = pc.count();
        Statement stmt = db.prepareBuiltQuery(qCntCT);
        stmt.execute();
        immutable cnt2 = stmt.front[0].getAs!ulong;
        assert(cnt2 == 2);
    }

    {
        static immutable string loc = "Elsewhere";
        enum PreCollection!(Mountain, SQLite3) pc = em.find!Mountain().where("location", '=', loc);

        immutable DBValue maxV = pc.aggregateVia(AggregateFunction.max, "height", db);

        immutable int max = maxV.getAs!int;
        assert(max == 4111);
    }

    {
        enum BuiltQuery bq = em
            .find!Mountain()
            .aggregate(AggregateFunction.min, "height");

        Statement stmt = db.prepareBuiltQuery(bq);
        stmt.execute();
        assert(!stmt.empty);

        immutable int min = stmt.front[0].getAs!int;
        assert(min == 1200);
    }

    {
        enum BuiltPreCollection!Mountain bpc = em
                .find!Mountain()
                .limit(1)
                .select();

        EntityCollection!Mountain ec = bpc.via(db);
        assert(!ec.empty);

        ec.popFront();
        assert(ec.empty);
    }

    {
        enum BuiltPreCollection!Mountain bpc = em
                .find!Mountain()
                .limit(3)
                .select();

        EntityCollection!Mountain ec = bpc.via(db);
        assert(!ec.empty);

        ec.popFront();
        assert(!ec.empty);

        ec.popFront();
        assert(!ec.empty);

        ec.popFront();
        assert(ec.empty);
    }

    {
        enum BuiltPreCollection!Mountain bpc = em
                .find!Mountain()
                .orderBy("height")
                .limit(2, 3)
                .select();

        EntityCollection!Mountain ec = bpc.via(db);
        assert(!ec.empty);

        const Mountain m1 = ec.front;
        assert(m1.height == 3201);

        ec.popFront();
        assert(!ec.empty);
        const Mountain m2 = ec.front;
        assert(m2.name == "Snowmountain");

        ec.popFront();
        assert(ec.empty);
    }

    {
        EntityCollection!Mountain ec = em
            .find!Mountain()
            .orderBy("height", desc)
            .limit(1)
            .selectVia(db);

        assert(!ec.empty);

        const Mountain highest = ec.front;
        assert(highest.name == "Mt. Nowhere");

        ec.popFront();
        assert(ec.empty);
    }

    {
        static immutable string loc = "Elsewhere";
        enum PreCollection!(Mountain, SQLite3) pc = em.find!Mountain().where("location", '=', loc);
        enum BuiltQuery bq = pc.delete_();

        Statement stmt = db.prepareBuiltQuery(bq);
        stmt.execute();
        assert(stmt.empty);
    }

    {
        em.find!Mountain().where("height", '<', 3000).deleteVia(db);

        ulong left = em.find!Mountain().countVia(db);
        assert(left == 3);

        EntityCollection!Mountain mtsLeft = em.find!Mountain().orderBy("height").selectVia(db);

        assert(!mtsLeft.empty);
        assert(mtsLeft.front.name == "Hill 1");

        mtsLeft.popFront();
        assert(!mtsLeft.empty);
        assert(mtsLeft.front.name == "Mt. Skyscrape");

        mtsLeft.popFront();
        assert(!mtsLeft.empty);
        assert(mtsLeft.front.name == "Mt. Nowhere");

        mtsLeft.popFront();
        assert(mtsLeft.empty);
    }
}

unittest  // many2one + one2many
{
    auto db = new SQLite3(":memory:", OpenMode.create);
    db.connect();
    scope (exit)
        db.close();

    db.execute(
        `CREATE TABLE "author" (
            "id" INTEGER PRIMARY KEY,
            "name" TEXT,
            "birthday" DATETIME
        );`
    );

    db.execute(
        `CREATE TABLE "book" (
            "id" INTEGER PRIMARY KEY,
            "name" TEXT,
            "published" DATE,
            "author_id" INTEGER UNSIGNED REFERENCES author(id)
        );`
    );

    auto em = EntityManager!SQLite3(db);

    struct Author
    {
        string name;
        mixin EntityID;
    }

    struct Book
    {
        string name;
        Date published;
        ulong authorID;
        mixin EntityID;
    }

    {
        auto adam = Author("Adam D. Ruppe");
        em.save(adam);
        auto someone = Author("Some One");
        em.save(someone);
        auto nobody = Author("No Body");
        em.save(nobody);

        em.store(Book("D Cookbook", Date(2014, 5, 26), adam.id));
        em.store(Book("Funny Things", Date(2016, 2, 4), someone.id));
        em.store(Book("Very funny Things", Date(2017, 3, 3), someone.id));
        em.store(Book("Fancy Things", Date(2017, 9, 9), someone.id));
        em.store(Book("Other Things", Date(2018, 12, 20), someone.id));
        em.store(Book("Stories", Date(2012, 7, 1), nobody.id));
        em.store(Book("Stories, vol.2", Date(2018, 4, 8), nobody.id));
        em.store(Book("Stories, vol.3", Date(2019, 8, 10), nobody.id));
        em.store(Book("Much more Stories", Date(2020, 11, 11), nobody.id));
    }

    {
        Book b;
        immutable bookFound = em.get(4, b);
        assert(bookFound);

        Author a;
        immutable authorFound = em.manyToOne!Author(b, a);
        assert(authorFound);
        assert(a.name == "Some One");

        assert(a.id == b.authorID);
        assert(a.name == "Some One");
    }

    {
        auto authorsFound = em.find!Author.where("name", '=', "No Body").selectVia(db);
        assert(!authorsFound.empty);

        Author a = authorsFound.front;
        assert(a.name == "No Body");

        authorsFound.popFront();
        assert(authorsFound.empty);

        auto pcBooks = em.oneToMany!Book(a);

        immutable ulong cntBooks = pcBooks.countVia(db);
        assert(cntBooks == 4);

        EntityCollection!Book booksSince2019 = pcBooks
            .where("published", '>', Date(2019, 1, 1))
            .orderBy("published")
            .selectVia(db);
        assert(!booksSince2019.empty);

        assert(booksSince2019.front.authorID == a.id);
        assert(booksSince2019.front.published.year >= 2019);
        assert(booksSince2019.front.name == "Stories, vol.3");

        booksSince2019.popFront();
        assert(!booksSince2019.empty);

        assert(booksSince2019.front.authorID == a.id);
        assert(booksSince2019.front.published.year >= 2019);
        assert(booksSince2019.front.name == "Much more Stories");

        booksSince2019.popFront();
        assert(booksSince2019.empty);
    }
}

unittest  // many2many
{
    auto db = new SQLite3(":memory:", OpenMode.create);
    db.connect();
    scope (exit)
        db.close();

    struct Thing
    {
        string name;
        mixin EntityID;
    }

    struct Tag
    {
        string name;
        mixin EntityID;
    }

    assert(joinTableName!(Thing, Tag) == "tag_thing");
    assert(joinTableName!(Tag, Thing) == "tag_thing");

    db.execute(
        `CREATE TABLE "thing" (
            "id" INTEGER PRIMARY KEY,
            "name" TEXT
        );`
    );

    db.execute(
        `CREATE TABLE "tag" (
            "id" INTEGER PRIMARY KEY,
            "name" TEXT
        );`
    );

    db.execute(
        `CREATE TABLE "tag_thing" (
            "tag_id" INTEGER REFERENCES tag(id),
            "thing_id" INTEGER REFERENCES thing(id),
            PRIMARY KEY ("tag_id", "thing_id")
        );`
    );

    auto em = EntityManager!SQLite3(db);

    auto fruit = Tag("Fruit");
    em.save(fruit);
    auto berry = Tag("Berry");
    em.save(berry);
    auto red = Tag("red");
    em.save(red);

    auto apple = Thing("Apple");
    em.save(apple);
    em.manyToManyAssign(fruit, apple);
    em.manyToManyAssign(red, apple);
    auto pear = Thing("Pear");
    em.save(pear);
    em.manyToManyAssign(fruit, pear);
    auto plum = Thing("Plum");
    em.save(plum);
    em.manyToManyAssign(fruit, plum);
    auto banana = Thing("Banana");
    em.save(banana);
    em.manyToManyAssign(fruit, banana);
    auto raspberry = Thing("Raspberry");
    em.save(raspberry);
    em.manyToManyAssign(raspberry, fruit);
    em.manyToManyAssign(raspberry, berry);
    auto blueberry = Thing("Blueberry");
    em.save(blueberry);
    em.manyToManyAssign(blueberry, fruit);
    em.manyToManyAssign(blueberry, berry);
    auto strawberry = Thing("Strawberry");
    em.save(strawberry);
    em.manyToManyAssign(strawberry, fruit);
    em.manyToManyAssign(strawberry, berry);
    em.manyToManyAssign(strawberry, red);
    auto book = Thing("Book");
    em.save(book);

    assert(em.manyToMany!Tag(apple).countVia(db) == 2);
    assert(em.manyToMany!Tag(pear).countVia(db) == 1);
    assert(em.manyToMany!Tag(strawberry).countVia(db) == 3);
    assert(em.manyToMany!Tag(book).countVia(db) == 0);

    assert(em.manyToMany!Thing(fruit).countVia(db) == 7);
    assert(em.manyToMany!Thing(berry).countVia(db) == 3);
    assert(em.manyToMany!Thing(red).countVia(db) == 2);

    {
        em.manyToManyAssign(book, fruit); // actually wrong…
        assert(em.manyToMany!Tag(book).countVia(db) == 1);

        em.manyToManyUnassign(book, fruit); // …so unassign
        assert(em.manyToMany!Tag(book).countVia(db) == 0);
    }
}
