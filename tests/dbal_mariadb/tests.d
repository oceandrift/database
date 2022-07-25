module tests.dbal_mariadb.tests;

import oceandrift.db.dbal;
import oceandrift.db.mariadb;

@safe:

DatabaseDriver getDB()
{
    return new MariaDBDatabaseDriver("localhost", "u_oceandrift", "p_oceandrift", null);
}

void dropRecreateUseDB(DatabaseDriver driver)
{
    driver.execute("DROP DATABASE IF EXISTS `d_oceandrift`");
    driver.execute("CREATE DATABASE `d_oceandrift`");
    driver.execute("USE `d_oceandrift`");
}

unittest
{
    DatabaseDriver driver = getDB();
    assert(!driver.connected);

    driver.connect();
    assert(driver.connected);

    driver.dropRecreateUseDB();

    driver.execute("CREATE TABLE `demo`(`id` INT UNSIGNED AUTO_INCREMENT PRIMARY KEY)");

    Statement count = driver.prepare("SELECT COUNT(*) FROM `demo`");
    scope (exit)
        count.close();

    count.execute();
    assert(!count.empty);
    assert(count.front[0] == 0);

    Statement insert = driver.prepare("INSERT INTO `demo`() VALUES()");
    scope (exit)
        insert.close();

    {
        driver.autoCommit = true;
        assert(driver.autoCommit);

        insert.execute();
        assert(insert.empty);

        driver.transactionRollback();

        driver.autoCommit = false;
        assert(!driver.autoCommit);

        count.execute();
        assert(!count.empty);
        assert(count.front[0] == 1);

        {
            driver.transactionStart();

            insert.execute();
            assert(insert.empty);
            insert.execute();
            assert(insert.empty);

            count.execute();
            assert(!count.empty);
            assert(count.front[0] == 3);

            driver.transactionRollback();
            count.execute();
            assert(!count.empty);
            assert(count.front[0] == 1);
        }

        {
            driver.transactionStart();

            insert.execute();
            assert(insert.empty);
            insert.execute();
            assert(insert.empty);
            insert.execute();
            assert(insert.empty);

            count.execute();
            assert(!count.empty);
            assert(count.front[0] == 4);

            driver.transactionCommit();
            count.execute();
            assert(!count.empty);
            assert(count.front[0] == 4);
        }
    }

    driver.execute("DROP DATABASE `d_oceandrift`");
    driver.close();
    assert(!driver.connected);
}

unittest
{
    DatabaseDriver driver = getDB();
    driver.connect();
    scope (exit)
        driver.close();

    driver.dropRecreateUseDB();

    driver.execute("
        CREATE TABLE `demo` (
            `id` INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            `col1` VARCHAR(4) NOT NULL,
            `col2` VARCHAR(4) NOT NULL
        )
    ");

    driver.transactionStart();

    Statement insert = driver.prepare("INSERT INTO `demo`(`col1`, `col2`) VALUES(?, ?)");
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

    Statement select = driver.prepare("
        SELECT * FROM `demo`
            WHERE
                `id` > ?
                AND `id` < ?
            ORDER BY
                `id` ASC
    ");
    scope (exit)
        select.close();

    select.bind(1, 2);
    select.bind(2, 5);
    select.execute();
    assert(!select.empty);

    Row row1 = select.front();
    assert(row1[0] == 3);
    assert(row1[1] == "yxcv");
    assert(row1[2] == "bnm,");

    select.popFront();
    assert(!select.empty);
    Row row2 = select.front();
    assert(row2[0] == 4);
    assert(row2[1] == "qaz");
    assert(row2[2] == "wsx");

    driver.transactionRollback();

    select.execute();
    assert(select.empty, "Rollback broken");
}

unittest
{
    DatabaseDriver driver = getDB();
    driver.connect();
    scope (exit)
        driver.close();

    driver.autoCommit = true;
    driver.dropRecreateUseDB();

    driver.execute("
        CREATE TABLE `person` (
            `id` INTEGER AUTO_INCREMENT PRIMARY KEY,
            `name` VARCHAR(6) NOT NULL,
            `nickname` VARCHAR(8),
            `age` TINYINT UNSIGNED NOT NULL
        )
    ");

    Statement insert = driver.prepare(
        "INSERT INTO `person` (`name`, `age`, `nickname`) VALUES (?, ?, ?)"
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
        Statement select = driver.prepare("
                SELECT `id`, `age` FROM `person`
                    WHERE
                        `age` >= ?
                    ORDER BY
                        `age` ASC
            ");
        scope (exit)
            select.close();

        select.bind(1, 22);
        select.execute();
        assert(!select.empty);

        Row row1 = select.front();
        assert(row1[0] == 3);

        select.popFront();
        assert(!select.empty);
        Row row2 = select.front();
        assert(row2[0] == 2);
        assert(row2[1] == 30);

        select.popFront();
        assert(select.empty);
    }

    {
        insert.executeWith("David", 35, "Dave");

        Statement stmtCount = driver.prepare("SELECT COUNT(*) FROM `person`");
        stmtCount.execute();
        assert(stmtCount.front()[0] == 4);
        stmtCount.popFront();
        assert(stmtCount.empty);
    }

    {
        Statement select = driver.prepare(
            "SELECT `nickname` FROM `person` WHERE `nickname` LIKE ?"
        );
        scope (exit)
            select.close();

        immutable string pattern = "Da%";
        select.executeWith(pattern);

        size_t cnt = 0;
        foreach (Row row; select)
        {
            string nickname = row[0].get!string;
            assert(nickname[0 .. 2] == "Da");
            ++cnt;
        }
        assert(cnt == 2);
    }
}

unittest
{
    import std.algorithm : canFind;
    import std.random : rndGen;

    DatabaseDriver driver = getDB();
    driver.connect();
    scope (exit)
        driver.close();

    driver.autoCommit = true;
    driver.dropRecreateUseDB();

    driver.execute("
        CREATE TABLE `misc` (
            `id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            `blob` BINARY(20)
        )
    ");

    ubyte[] blablabla;
    for (size_t n = 0; n < 20; ++n)
    {
        blablabla ~= ubyte(rndGen.front % ubyte.max);
        rndGen.popFront();
    }

    {
        Statement insert = driver.prepare("INSERT INTO `misc`(`blob`) VALUES(?)");
        insert.executeWith(blablabla);
    }

    {
        Statement select = driver.prepare("SELECT `blob` FROM `misc`");
        scope (exit)
            select.close();

        select.execute();
        assert(!select.empty);

        Row row = select.front;
        assert(row[0] == blablabla);

        ubyte[] bla2 = blablabla.dup;
        bla2[2] += 1;
        assert(row[0] != bla2);

        select.popFront();
        assert(select.empty);
    }
}

unittest
{
    DatabaseDriver driver = getDB();
    driver.connect();
    scope (exit)
        driver.close();

    driver.autoCommit = true;
    driver.dropRecreateUseDB();

    driver.execute("
        CREATE TABLE `holiday` (
            `id` INTEGER AUTO_INCREMENT PRIMARY KEY,
            `date` DATE NOT NULL,
            `name` VARCHAR(20) NOT NULL DEFAULT 'national'
        )
    ");

    Statement insert = driver.prepare("INSERT INTO `holiday`(`date`, `name`) VALUES(?, ?)");
    scope (exit)
        insert.close();

    Statement insertDateOnly = driver.prepare("INSERT INTO `holiday`(`date`) VALUES(?)");
    scope (exit)
        insertDateOnly.close();

    Statement countAny = driver.prepare("SELECT COUNT(*) FROM `holiday`");
    scope (exit)
        countAny.close();

    Statement countByName = driver.prepare("SELECT COUNT(*) FROM `holiday` WHERE `name` = ?");
    scope (exit)
        countByName.close();

    Statement getByName = driver.prepare("SELECT `date` FROM `holiday` WHERE `name` = ?");
    scope (exit)
        getByName.close();

    insert.executeWith(Date(2022, 1, 1), "New Year");
    insert.executeWith(Date(2022, 4, 18), "Easter Monday");
    insertDateOnly.executeWith(Date(2022, 5, 1));
    insertDateOnly.executeWith(Date(2022, 10, 26));

    countAny.execute();
    assert(!countAny.empty);
    assert(countAny.front[0] == 4);

    countByName.executeWith("national");
    assert(!countByName.empty);
    assert(countByName.front[0] == 2);

    getByName.executeWith("New Year");
    assert(!getByName.empty);
    Date newYear = getByName.front[0].get!Date;
    assert(newYear.day == 1);
    assert(newYear.month == 1);
    assert(newYear.year == 2022);

    getByName.popFront();
    assert(getByName.empty);
}

unittest
{
    DatabaseDriver driver = getDB();
    driver.connect();
    scope (exit)
        driver.close();

    driver.autoCommit = true;
    driver.dropRecreateUseDB();

    driver.execute("
        CREATE TABLE `moment` (
            `id` INTEGER AUTO_INCREMENT PRIMARY KEY,
            `date` DATE NOT NULL,
            `time` TIME NOT NULL,
            `redundant` DATETIME NOT NULL
        )
    ");

    auto date = Date(2022, 7, 24);
    auto time = TimeOfDay(22, 30, 1);

    {
        Statement insert = driver.prepare(
            "INSERT INTO `moment`(`date`, `time`, `redundant`) VALUES(?, ?, ?)");
        scope (exit)
            insert.close();

        insert.executeWith(date, time, DateTime(date, time));
        assert(insert.empty);
    }

    {
        Statement test = driver.prepare("
            SELECT
                COUNT(*)
            FROM `moment`
            WHERE
                `date` = DATE(`redundant`)
                AND `time` = TIME(`redundant`)
        ");
        scope (exit)
            test.close();

        test.execute();
        assert(!test.empty);
        assert(test.front[0] == 1);
    }

    {
        Statement select = driver.prepare("SELECT `date`, `time`, `redundant` FROM `moment`");
        scope (exit)
            select.close();

        select.execute();
        assert(select.front[0] == date);
        assert(select.front[1] == time);
        assert(select.front[2] == DateTime(date, time));
    }
}
