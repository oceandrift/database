/++
    Object-Relational Mapping (ORM)
 +/
module oceandrift.db.orm;

import std.meta;
import std.string;
import std.traits;

import oceandrift.db.dbal.driver;
import oceandrift.db.dbal.v4;

public import oceandrift.db.dbal.v4 : asc, desc, distinct, where, whereNot, whereParentheses;

@safe:

enum bool isORMCompatible(Driver) =
    isDatabaseDriver!Driver
    && isQueryCompiler!Driver;

enum bool isEntityType(TEntity) = (
        (
            is(TEntity == struct)
            || is(TEntity == class)
    )
    && is(ReturnType!((TEntity t) => t.id) == ulong)
    && __traits(compiles, delegate(TEntity t) @safe { t.id = ulong(0); })
    );

enum string tableName(alias TEntity) =
    __traits(identifier, TEntity).toLower;

enum string joinTableName(alias TEntity1, alias TEntity2) =
    joinTableNameImpl!(TEntity1, TEntity2)();

private string joinTableNameImpl(alias TEntity1, alias TEntity2)()
{
    import std.string : cmp;

    string name1 = tableName!TEntity1;
    string name2 = tableName!TEntity2;

    int x = cmp(name1, name2);

    // dfmt off
    return (x < 0)
        ? name1 ~ '_' ~ name2
        : name2 ~ '_' ~ name1
    ;
    // dfmt on
}

enum auto columnNames(alias TEntity) =
    aliasSeqOf!(columnNamesImpl!TEntity());

enum auto columnNamesNoID(alias TEntity) =
    aliasSeqOf!(columnNamesImpl!(TEntity, false)());

private auto columnNamesImpl(TEntity, bool includeID = true)()
{
    string[] columnNames = [];

    static foreach (idx, field; FieldNameTuple!TEntity)
    {
        static if (!isDBValueCompatible!(Fields!TEntity[idx]))
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

struct PreCollection(TEntity, DatabaseDriver)
        if (isEntityType!TEntity && isORMCompatible!DatabaseDriver)
{
@safe:
    BuiltPreCollection!TEntity select()
    {
        BuiltQuery bq = _query
            .select(columnNames!TEntity)
            .build!DatabaseDriver();
        return BuiltPreCollection!TEntity(bq);
    }

    EntityCollection!TEntity selectVia(DatabaseDriver db)
    {
        return this.select().via(db);
    }

    BuiltQuery count()
    {
        BuiltQuery bq = _query
            .select(oceandrift.db.dbal.v4.count("*"))
            .build!DatabaseDriver();
        return bq;
    }

    ulong countVia(DatabaseDriver db)
    {
        BuiltQuery bq = this.count();

        Statement stmt = db.prepareBuiltQuery(bq);
        stmt.execute();

        debug assert(!stmt.empty);
        return stmt.front[0].getAs!ulong();
    }

    BuiltQuery aggregate(Distinct distinct = Distinct.no)(AggregateFunction aggr, string column)
    {
        BuiltQuery bq = _query
            .select(SelectExpression(col(column), aggr, distinct))
            .build!DatabaseDriver();
        return bq;
    }

    DBValue aggregateVia(Distinct distinct = Distinct.no)(AggregateFunction aggr, string column, DatabaseDriver db)
    {
        BuiltQuery bq = this.aggregate!(distinct)(aggr, column);

        Statement stmt = db.prepareBuiltQuery(bq);
        stmt.execute();

        debug assert(!stmt.empty);
        debug assert(stmt.front.length == 1);
        return stmt.front[0];
    }

    BuiltQuery delete_()
    {
        BuiltQuery bq = _query
            .delete_()
            .build!DatabaseDriver();
        return bq;
    }

    void deleteVia(DatabaseDriver db)
    {
        BuiltQuery bq = this.delete_();

        Statement stmt = db.prepareBuiltQuery(bq);
        stmt.execute();
        stmt.close();
    }

    PreCollection!(TEntity, DatabaseDriver) where(LogicalOperator logicalJunction = and, TComparisonOperator)(
        string column, TComparisonOperator op, const DBValue value)
            if (isComparisonOperator!TComparisonOperator)
    {
        return typeof(this)(_query.where!logicalJunction(column, op, value));
    }

    PreCollection!(TEntity, DatabaseDriver) where(LogicalOperator logicalJunction = and, TComparisonOperator, T)(
        string column, TComparisonOperator op, const T value)
            if (isComparisonOperator!TComparisonOperator && isDBValueCompatible!T)
    {
        return typeof(this)(_query.where!logicalJunction(column, op, value));
    }

    PreCollection!(TEntity, DatabaseDriver) whereParentheses(LogicalOperator logicalJunction = and)(
        Query delegate(Query q) @safe pure conditions)
    {
        return typeof(this)(_query.whereParentheses!logicalJunction(conditions));
    }

    PreCollection!(TEntity, DatabaseDriver) orderBy(string column, OrderingSequence orderingSequence = asc)
    {
        return typeof(this)(_query.orderBy(column, orderingSequence));
    }

    PreCollection!(TEntity, DatabaseDriver) limit(ulong limit)
    {
        return typeof(this)(_query.limit(limit));
    }

    PreCollection!(TEntity, DatabaseDriver) limit(ulong limit, int offset)
    {
        return typeof(this)(_query.limit(limit, offset));
    }

private:
    Query _query;
}

struct BuiltPreCollection(TEntity) if (isEntityType!TEntity)
{
@safe pure nothrow @nogc:

    string sql()
    {
        return _query.sql;
    }

private:
    BuiltQuery _query;
}

EntityCollection!TEntity map(TEntity, DatabaseDriver)(DatabaseDriver db, BuiltPreCollection!TEntity builtPreCollection)
        if (isDatabaseDriver!DatabaseDriver && isEntityType!TEntity)
{
    pragma(inline, true);
    return via(builtPreCollection, db);
}

EntityCollection!TEntity via(TEntity, DatabaseDriver)(
    BuiltPreCollection!TEntity builtPreCollection, DatabaseDriver db)
        if (isDatabaseDriver!DatabaseDriver && isEntityType!TEntity)
{
    Statement stmt = db.prepareBuiltQuery(builtPreCollection._query);
    stmt.execute();
    return EntityCollection!TEntity(stmt);
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

        enum int idParamN = columnNamesNoID!TEntity.length;
        stmt.bind(idParamN, entity.id);

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

    deprecated EntityCollection!TEntity find(TEntity)(Query delegate(Query) @safe buildQuery)
            if (isEntityType!TEntity)
    in (buildQuery !is null)
    {
        Query q = table(tableName!TEntity).qb;
        q = buildQuery(q);
        BuiltQuery query = q
            .select(columnNames!TEntity)
            .build!DatabaseDriver();

        Statement stmt = _db.prepareBuiltQuery(query);
        stmt.execute();

        return EntityCollection!TEntity(stmt);
    }

    deprecated EntityCollection!TEntity find(TEntity, Query function(Query) @safe pure buildQuery)(
        void delegate(Statement) @safe bindValues = null)
            if (isEntityType!TEntity && (buildQuery !is null))
    {
        enum Query q = buildQuery(table(tableName!TEntity).qb);
        enum BuiltQuery query = q
                .select(columnNames!TEntity)
                .build!DatabaseDriver();

        Statement stmt = _db.prepareBuiltQuery(query);

        if (bindValues !is null)
            bindValues(stmt);

        stmt.execute();

        return EntityCollection!TEntity(stmt);
    }

    static PreCollection!(TEntity, DatabaseDriver) find(TEntity)()
            if (isEntityType!TEntity)
    {
        enum Query q = table(tableName!TEntity).qb;
        enum pc = PreCollection!(TEntity, DatabaseDriver)(q);
        return pc;
    }

    ///
    bool manyToOne(TEntityOne, TEntityMany)(TEntityMany many, out TEntityOne output)
            if (isEntityType!TEntityOne && isEntityType!TEntityMany)
    {
        enum BuiltQuery bq = table(tableName!TEntityOne).qb
                .where("id", '=')
                .select(columnNames!TEntityOne)
                .build!DatabaseDriver();

        Statement stmt = _db.prepareBuiltQuery(bq);
        mixin("immutable ulong oneID = many." ~ tableName!TEntityOne ~ "_id;");
        stmt.bind(0, oneID);
        stmt.execute();

        if (stmt.empty)
            return false;

        output = stmt.front.toEntity!TEntityOne();
        return true;
    }

    ///
    bool oneToOne(TEntityTarget, TEntitySource)(TEntitySource source, out TEntityTarget toOne)
            if (isEntityType!TEntityTarget && isEntityType!TEntitySource)
    {
        pragma(inline, true);
        return manyToOne(source, toOne);
    }

    static PreCollection!(TEntityTarget, DatabaseDriver) manyToMany(
        TEntityTarget,
        TEntitySource,
        string joinTableName_ = joinTableName!(TEntitySource, TEntityTarget)
    )(TEntitySource source)
            if (isEntityType!TEntityTarget && isEntityType!TEntitySource)
    {
        enum Table joinTable = table(joinTableName_);

        enum string targetName = tableName!TEntityTarget;
        enum Column columnForeignKeyTarget = col(joinTable, targetName ~ "_id");

        enum string sourceName = tableName!TEntitySource;
        enum Column columnForeignKeySource = col(joinTable, sourceName ~ "_id");

        enum Column columnPrimaryKeyTarget = col(table(targetName), "id");

        enum Query q =
            joinTable.qb
                .join(
                    columnPrimaryKeyTarget,
                    columnForeignKeyTarget
                )
                .where(columnForeignKeySource, '=');
        enum pcT = PreCollection!(TEntityTarget, DatabaseDriver)(q);

        auto pc = pcT;
        pc._query.updatePreSetWhereValue(0, DBValue(source.id));
        return pc;
    }

    void manyToManyAssign(
        TEntity1,
        TEntity2,
        string joinTableName_ = joinTableName!(TEntity1, TEntity2),
    )(TEntity1 e1, TEntity2 e2) if (isEntityType!TEntity1 && isEntityType!TEntity2)
    {
        enum Table joinTable = table(joinTableName_);
        enum string e2Col = tableName!TEntity2 ~ "_id";
        enum string e1Col = tableName!TEntity1 ~ "_id";

        enum BuiltQuery bq = joinTable.insert(e1Col, e2Col).build!DatabaseDriver();

        Statement stmt = _db.prepareBuiltQuery(bq);
        stmt.bind(0, e1.id);
        stmt.bind(1, e2.id);
        stmt.execute();
    }

    void manyToManyUnassign(
        TEntity1,
        TEntity2,
        string joinTableName_ = joinTableName!(TEntity1, TEntity2),
    )(TEntity1 e1, TEntity2 e2) if (isEntityType!TEntity1 && isEntityType!TEntity2)
    {
        enum Table joinTable = table(joinTableName!(TEntity1, TEntity2));
        enum string e2Col = tableName!TEntity2 ~ "_id";
        enum string e1Col = tableName!TEntity1 ~ "_id";

        enum BuiltQuery bq = joinTable.qb
                .where(e1Col, '=')
                .where(e2Col, '=')
                .delete_()
                .build!DatabaseDriver();

        Statement stmt = _db.prepareBuiltQuery(bq);
        stmt.bind(0, e1.id);
        stmt.bind(1, e2.id);
        stmt.execute();
    }

    static PreCollection!(TEntityMany, DatabaseDriver) oneToMany(
        TEntityMany,
        TEntityOne,
    )(TEntityOne source) if (isEntityType!TEntityMany && isEntityType!TEntityOne)
    {
        enum string foreignKeyColumn = tableName!TEntityOne ~ "_id";

        enum Query q = table(tableName!TEntityMany).qb.where(foreignKeyColumn, '=');
        enum pcT = PreCollection!(TEntityMany, DatabaseDriver)(q);

        auto pc = pcT;
        pc._query.updatePreSetWhereValue(0, DBValue(source.id));
        return pc;
    }

    static void _pragma(TEntity)()
    {
        pragma(msg, "==== EntityManager._pragma!(" ~ TEntity.stringof ~ "):");

        static if (!isEntityType!TEntity)
        {
            pragma(msg, "- ERROR: Not a compatible type");
        }
        else
        {
            pragma(msg, "- Table Name:\n" ~ tableName!TEntity);
            pragma(msg, "- Column Names:");
            pragma(msg, columnNames!TEntity);
            pragma(msg, "- Column Names (no ID):");
            pragma(msg, columnNamesNoID!TEntity);
        }

        pragma(msg, "/====");
    }
}

mixin template EntityID()
{
    ulong id = 0;
}
