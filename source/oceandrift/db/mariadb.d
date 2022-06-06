module oceandrift.db.maridb;

import oceandrift.db.dbal;

import mysql.safe;

class MariaDBDatabaseDriver : DatabaseDriver
{
@safe:

    private
    {
        Connection _connection;
    }

    public this(string host, ushort port, string username, string password, string database)
    {
        this._connection = new Connection();
    }

    public  // DatabaseDriver
    {
        bool connected()
        {
            return true;
        }
    }

    public  // Extras
    {
        Connection getConnection()
        {
            return this._connection;
        }
    }
}
