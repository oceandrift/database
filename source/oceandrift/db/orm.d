module oceandrift.db.orm;

import std.meta;
import std.string;
import std.traits;

import oceandrift.db.dbal.driver;
import oceandrift.db.dbal.v4;

public import oceandrift.db.dbal.v4 : limit, where, whereNot, whereParentheses;

@safe:

enum bool isORMCompatible(Driver) =
    isDatabaseDriver!Driver
    && isQueryCompiler!Driver;

enum bool isInSumType(T, SumType) = (staticIndexOf!(T, SumType.Types) >= 0);

enum bool isEntityType(TEntity) = (
        (
            is(TEntity == struct)
            || is(TEntity == class)
    )
    && is(ReturnType!((TEntity t) => t.id) == ulong)
    );

enum string tableName(alias TEntity) =
    __traits(identifier, TEntity).toLower;

enum auto columnNames(alias TEntity) =
    aliasSeqOf!(columnNamesImpl!TEntity());

enum auto columnNamesNoID(alias TEntity) =
    aliasSeqOf!(columnNamesImpl!(TEntity, false)());

private auto columnNamesImpl(TEntity, bool includeID = true)()
{
    string[] columnNames = [];

    foreach (idx, field; FieldNameTuple!TEntity)
    {
        static if (!isInSumType!(Fields!TEntity[idx], DBValue))
            static assert(0, "Column not serializable to DBValue");
        else static if (includeID || (field.toLower != "id"))
            columnNames ~= field.toLower;
    }

    return columnNames;
}

TEntity toEntity(TEntity)(Row row)
{
    static if (is(TEntity == class))
        TEntity e = new TEntity();
    else static if (is(TEntity == struct))
        TEntity e = TEntity();
    else
        static assert(0, "faulty template constraint implementation.");

    static foreach (idx, name; columnNames!TEntity)
        mixin("e." ~ name ~ " = row[idx].getAs!(typeof(e." ~ name ~ "));");

    return e;
}

EntityCollection!TEntity toEntities(TEntity)(Statement stmt) pure nothrow @nogc
{
    return EntityCollection!TEntity(stmt);
}

struct EntityCollection(TEntity) if (isEntityType!TEntity)
{
@safe:

    private
    {
        Statement _stmt;
    }

    private this(Statement stmt) pure nothrow @nogc
    {
        _stmt = stmt;
    }

    bool empty()
    {
        return _stmt.empty;
    }

    TEntity front()
    {
        return _stmt.front.toEntity!TEntity();
    }

    void popFront()
    {
        _stmt.popFront();
    }
}

bool get(TEntity, DatabaseDriver)(DatabaseDriver db, ulong id, out TEntity output)
        if (isEntityType!TEntity)
{
    enum BuiltQuery query = table(tableName!TEntity).qb
            .where("id", '=')
            .select(columnNames!TEntity)
            .build!DatabaseDriver();

    Statement stmt = db.prepareBuiltQuery(query);
    stmt.bind(0, id);

    stmt.execute();
    if (stmt.empty)
        return false;

    output = stmt.front.toEntity!TEntity;
    return true;
}

struct EntityManager(DatabaseDriver) if (isORMCompatible!DatabaseDriver)
{
@safe:

    private
    {
        DatabaseDriver _db;
    }

    this(DatabaseDriver db)
    {
        _db = db;
    }

    bool get(TEntity)(ulong id, out TEntity output) if (isEntityType!TEntity)
    {
        enum BuiltQuery query = table(tableName!TEntity).qb
                .where("id", '=')
                .select(columnNames!TEntity)
                .build!DatabaseDriver();

        Statement stmt = _db.prepareBuiltQuery(query);
        stmt.bind(0, id);

        stmt.execute();
        if (stmt.empty)
            return false;

        output = stmt.front.toEntity!TEntity;
        return true;
    }

    void save(TEntity)(ref TEntity entity) if (isEntityType!TEntity)
    {
        if (entity.id == 0)
            entity.id = this.store(entity);
        else
            this.update(entity);
    }

    ulong store(TEntity)(const TEntity entity) if (isEntityType!TEntity)
    {
        enum BuiltQuery query = table(tableName!TEntity)
                .insert(columnNamesNoID!TEntity)
                .build!DatabaseDriver();

        Statement stmt = _db.prepareBuiltQuery(query);

        static foreach (int idx, column; columnNamesNoID!TEntity)
            mixin("stmt.bind(idx, cast(const) entity." ~ column ~ ");");

        stmt.execute();

        return _db.lastInsertID().getAs!ulong;
    }

    void update(TEntity)(const TEntity entity) if (isEntityType!TEntity)
    in (entity.id != 0)
    {
        enum BuiltQuery query = table(tableName!TEntity).qb
                .where("id", '=')
                .update(columnNamesNoID!TEntity)
                .build!DatabaseDriver();

        Statement stmt = _db.prepareBuiltQuery(query);

        static foreach (int idx, column; columnNamesNoID!TEntity)
            mixin("stmt.bind(idx, cast(const) entity." ~ column ~ ");");

        stmt.bind(cast(int) columnNamesNoID!TEntity.length, entity.id);

        stmt.execute();
    }

    void remove(TEntity)(ulong id) if (isEntityType!TEntity)
    {
        enum BuiltQuery query = table(tableName!TEntity).qb
                .where("id", '=')
                .delete_()
                .build!DatabaseDriver();

        Statement stmt = _db.prepareBuiltQuery(query);
        stmt.bind(0, id);
        stmt.execute();
    }

    void remove(TEntity)(TEntity entity) if (isEntityType!TEntity)
    {
        return this.remove!TEntity(entity.id);
    }

    void _pragma(TEntity)()
    {
        pragma(msg, "==== EntityManager._pragma!(" ~ TEntity.stringof ~ "):");
        pragma(msg, "- Table Name:\n" ~ tableName!TEntity);
        pragma(msg, "- Column Names:");
        pragma(msg, columnNames!TEntity);
        pragma(msg, "- Column Names (no ID):");
        pragma(msg, columnNamesNoID!TEntity);
        pragma(msg, "/====");
    }
}
