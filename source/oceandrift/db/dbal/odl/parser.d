module oceandrift.db.dbal.odl.parser;

import oceandrift.db.dbal.odl.lexer;

@safe:

// Parser

interface ASTVisitor
{
    void visit(BetweenPredicate);
    void visit(BooleanFactor);
    void visit(BooleanTerm);
    void visit(ComparisonPredicate);
    void visit(InPredicate);
    void visit(LikePredicate);
    void visit(NullPredicate);
    void visit(NullRowValueConstructorElement);
    void visit(Parantheses);
    void visit(PlaceholderRowValueConstructorElement);
    void visit(SearchCondition);
    void visit(ValueExpressionValueConstructorElement);
}

abstract class ASTNode
{
    abstract void accept(ASTVisitor v);
}

private enum astNodeAccept = q{
    override void accept(ASTVisitor v)
    {
        v.visit(this);
    }
};

class ParserException : Exception
{
    private
    {
        size_t _offset;
        string _expectation;
        string _actual;
    }

    this(size_t offset, string expectation, string actual)
    {
        _offset = offset;
        _expectation = expectation;
        _actual = actual;

        immutable string msg = "Error while parsing ODL query:\nExpected:\t" ~ expectation ~ "\nBut got:\t" ~ actual;
        super(msg, null, "{ODL Query}", _offset);
    }

    this(size_t offset, Token.Type expectation, Token.Type actual)
    {
        import std.conv : to;

        this(offset, expectation.to!string, actual.to!string);
    }

    this(size_t offset, string expectation, Token.Type actual)
    {
        import std.conv : to;

        this(offset, expectation, actual.to!string);
    }

    this(size_t offset, Token.Type[] expectations, Token.Type actual)
    {
        import std.conv : to;
        import std.algorithm : joiner, map;
        import std.range : array;

        immutable(ubyte)[] expectation = expectations
            .map!(e => cast(immutable(ubyte)[]) e.to!string)
            .joiner(cast(immutable(ubyte)[]) " or ")
            .array;

        this(offset, cast(string) expectation, actual.to!string);
    }
}

enum CompOp
{
    invalid = -1,
    equals,
    notEquals,
    lessThan,
    greaterThan,
    lessThanOrEquals,
    greaterThanOrEquals,
}

CompOp toCompOp(Token.Type tokenType)
{
    switch (tokenType) with (Token.Type)
    {
    case opEqual:
        return CompOp.equals;
    case opNotEqual:
        return CompOp.notEquals;
    case opLess:
        return CompOp.lessThan;
    case opGreater:
        return CompOp.greaterThan;
    case opLessOrEqual:
        return CompOp.lessThanOrEquals;
    case opGreaterOrEqual:
        return CompOp.greaterThanOrEquals;
    default:
        return CompOp.invalid;
    }
}

class SearchCondition : ASTNode
{
    BooleanTerm term;
    SearchCondition or;

    this(BooleanTerm term, SearchCondition or)
    {
        this.term = term;
        this.or = or;
    }

    static typeof(this) parse(Lexer code)
    {
        BooleanTerm term = BooleanTerm.parse(code);

        code.popFront();
        SearchCondition or =
            (code.front.type == Token.Type.opOr)
            ? typeof(this).parse(code) : null;

        return new SearchCondition(term, or);
    }

    mixin(astNodeAccept);
}

class BooleanTerm : ASTNode
{
    BooleanFactor factor;
    BooleanTerm and;

    this(BooleanFactor term, BooleanTerm and)
    {
        this.factor = factor;
        this.and = and;
    }

    static typeof(this) parse(Lexer code)
    {
        BooleanFactor term = BooleanFactor.parse(code);

        code.popFront();
        BooleanTerm and =
            (code.front.type == Token.Type.opAnd)
            ? typeof(this).parse(code) : null;

        return new BooleanTerm(term, and);
    }

    mixin(astNodeAccept);
}

class BooleanFactor : ASTNode
{
    bool not;
    BooleanPrimary test;

    static typeof(this) parse(Lexer code)
    {
        assert(0); // TODO
    }

    mixin(astNodeAccept);
}

abstract class BooleanPrimary : ASTNode
{
    static typeof(this) parse(Lexer code)
    {
        if (code.front.type == Token.Type.leftParenthesis)
            return Parantheses.parse(code);
        else
            return Predicate.parse(code);
    }
}

abstract class Predicate : BooleanPrimary
{
    static typeof(this) parse(Lexer code)
    {
        // left
        if (code.front.type != Token.Type.identifier)
            throw new ParserException(code.offset, Token.Type.identifier, code.front.type);

        auto left = new ValueExpressionValueConstructorElement(code.front.data);

        // op
        code.popFront();
        CompOp op = code.front.type.toCompOp;

        if (op != CompOp.invalid) // non-comparison?
            return NegatablePredicate.parse(left, code);

        // right
        code.popFront();

        if (code.front.type != Token.Type.placeholder)
            throw new ParserException(code.offset, Token.Type.placeholder, code.front.type);

        auto right = new PlaceholderRowValueConstructorElement();
        return new ComparisonPredicate(left, op, right);
    }
}

class Parantheses : BooleanPrimary
{
    SearchCondition condition;

    static typeof(this) parse(Lexer code)
    {
        auto p = new Parantheses();
        p.condition = SearchCondition.parse(code);
        return p;
    }

    mixin(astNodeAccept);
}

class ComparisonPredicate : Predicate
{
    RowValueConstructorElement left;
    CompOp op;
    RowValueConstructorElement right;

    this(RowValueConstructorElement left, CompOp op, RowValueConstructorElement right)
    {
        this.left = left;
        this.op = op;
        this.right = right;
    }

    mixin(astNodeAccept);
}

abstract class NegatablePredicate : Predicate
{
    string column;
    bool not;

    this(string column, bool not)
    {
        this.column = column;
        this.not = not;
    }

    static typeof(this) parse(ValueExpressionValueConstructorElement left, Lexer code)
    {
        // IS… NULL/NOT NULL
        if (code.front.type == Token.Type.opIs)
        {
            return NullPredicate.parse(left, code);
        }

        bool not = false;
        if (code.front.type == Token.Type.opNot)
        {
            not = true;
            code.popFront();
        }

        Token.Type opType = code.front.type;
        size_t opOffset = code.offset;

        code.popFront();
        if (code.front.type != Token.Type.placeholder)
        {
            throw new ParserException(code.offset, Token.Type.placeholder, code.front.type);
        }

        switch (opType) with (Token.Type)
        {
        case opIn:
            return new InPredicate(left.valueExpression, not);
        case opLike:
            return new LikePredicate(left.valueExpression, not);
        default:
            throw new ParserException(opOffset, "Predicate operator", opType);
        }
    }
}

class BetweenPredicate : NegatablePredicate
{
    RowValueConstructorElement lower;
    RowValueConstructorElement upper;

    this(string column, bool not, RowValueConstructorElement lower, RowValueConstructorElement upper)
    {
        super(column, not);
        this.lower = lower;
        this.upper = upper;
    }

    mixin(astNodeAccept);
}

class InPredicate : NegatablePredicate
{
    this(string column, bool not)
    {
        super(column, not);
    }

    mixin(astNodeAccept);
}

class LikePredicate : NegatablePredicate
{
    this(string column, bool not)
    {
        super(column, not);
    }

    mixin(astNodeAccept);
}

/**
    - IS NULL
    - IS NOT NULL
*/
class NullPredicate : NegatablePredicate
{
    this(string column, bool not)
    {
        super(column, not);
    }

    static typeof(this) parse(ValueExpressionValueConstructorElement left, Lexer code)
    {
        code.popFront(); // "IS"

        switch (code.front.type) with (Token.Type)
        {
        case opNot:
            code.popFront();
            if (code.front.type != null_)
                throw new ParserException(code.offset, null_, code.front.type);
            return new NullPredicate(left.valueExpression, true);

        case null_:
            return new NullPredicate(left.valueExpression, false);

        default:
            throw new ParserException(code.offset, [opNot, null_], code.front.type);
        }
    }

    mixin(astNodeAccept);
}

abstract class RowValueConstructorElement : ASTNode
{
    static typeof(this) parse(Lexer code)
    {
        switch (code.front.type) with (Token.Type)
        {
        case null_:
            return new NullRowValueConstructorElement();
        case placeholder:
            return new PlaceholderRowValueConstructorElement();
        case identifier:
            return new ValueExpressionValueConstructorElement(code.front.data);
        default:
            throw new ParserException(
                code.offset,
                [null_, placeholder, identifier],
                code.front.type
            );
        }
    }
}

class NullRowValueConstructorElement : RowValueConstructorElement
{
    mixin(astNodeAccept);
}

class PlaceholderRowValueConstructorElement : RowValueConstructorElement
{
    mixin(astNodeAccept);
}

class ValueExpressionValueConstructorElement : RowValueConstructorElement
{
    string valueExpression;

    this(string valueExpression)
    {
        this.valueExpression = valueExpression;
    }

    mixin(astNodeAccept);
}

ASTNode parseWhereClause(Lexer code)
{
    return SearchCondition.parse(code);
}
