module oceandrift.db.dbal.odl.lexer;

/// Lexes the input string
Lexer lexODL(string code)
{
    return Lexer(code);
}

// Lexical token
struct Token
{
    enum Type
    {
        error = -2,
        ice = -1, /** internal compiler error */
        endOfQuery = 0,

        // Comparison operators
        opEqual,
        opNotEqual,
        opGreater,
        opGreaterOrEqual,
        opLess,
        opLessOrEqual,
        opIs,
        opNot,
        opLike,
        opIn,
        opBetween,

        // Logical operators
        opAnd,
        opOr,

        // etc.
        null_,
        leftParenthesis,
        rightParenthesis,
        placeholder,
        identifier,
    }

    Type type;
    string data;
}

struct Lexer
{
@safe:

    import std.regex;
    import std.typecons : No;

    alias StringTokenRange = typeof(splitter("", ctRegex!(`\s`)));

    private
    {
        StringTokenRange _code = void;
        Token _front = void;
        size_t _offset;
    }

    @disable this();
    private this(string code)
    {
        _code = splitter(code, ctRegex!(`\s`));

        if (!_code.empty)
            popFront();
        else
            _front = Token(Token.Type.endOfQuery);
    }

    size_t offset()
    {
        return _offset;
    }

    Token front()
    {
        return _front;
    }

    bool empty()
    {
        return (_front.type == Token.Type.endOfQuery);
    }

    void popFront()
    {
        if (!_code.empty)
            _code.popFront();

        if (_code.empty)
        {
            _front = Token(Token.Type.endOfQuery);
            return;
        }

        ++_offset;

        Token.Type type = Token.Type.ice;

        switch (_code.front)
        {
        case "=":
            type = Token.Type.opEqual;
            break;

        case "<>":
        case "!=":
            type = Token.Type.opNotEqual;
            break;

        case ">":
            type = Token.Type.opGreater;
            break;

        case ">=":
            type = Token.Type.opGreaterOrEqual;
            break;

        case "<":
            type = Token.Type.opLess;
            break;

        case "<=":
            type = Token.Type.opLessOrEqual;
            break;

        case "IS":
            type = Token.Type.opIs;
            break;

        case "NOT":
            type = Token.Type.opNot;
            break;

        case "LIKE":
            type = Token.Type.opLike;
            break;

        case "IN":
            type = Token.Type.opIn;
            break;

            // Logical operators

        case "AND":
        case "&&":
            type = Token.Type.opAnd;
            break;

        case "OR":
            type = Token.Type.opOr;
            break;

            // etc

        case "NULL":
            type = Token.Type.null_;
            break;

        case "(":
            type = Token.Type.leftParenthesis;
            break;

        case ")":
            type = Token.Type.rightParenthesis;
            break;

        case "?":
            type = Token.Type.placeholder;
            break;

        default:
            type = Token.Type.identifier;
            break;
        }

        _front = Token(type, _code.front);
    }
}
