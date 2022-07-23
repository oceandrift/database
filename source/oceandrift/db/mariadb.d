module oceandrift.db.maridb;

import mysql.safe;
import oceandrift.db.dbal.driver;

@safe:

class MariaDBDatabaseDriver : DatabaseDriver
{
@safe:

    private
    {
        Connection _connection;

        string _host;
        ushort _port;
        string _username;
        string _password;
        string _database;
    }

    public this(string host, ushort port, string username, string password, string database)
    {
        _host = host;
        _port = port;
        _username = username;
        _password = password;
        _database = database;
    }

    public  // MinimalDatabaseDriver
    {
        void connect()
        {
            _connection = new Connection(
                _host,
                _username,
                _password,
                _database,
                _port
            );
        }

        void close()
        {
            _connection.close();
        }

        bool connected()
        {
            return ((this._connection !is null)
                    && !this._connection.closed);
        }
    }

    public
    {
        bool autoCommit()
        {
            return this._connection
                .queryRow("SHOW VARIABLES LIKE 'autocommit'")
                .get[1] == "ON";
        }

        void autoCommit(bool enable)
        {
            if (enable)
                _connection.exec("SET autocommit=1");
            else
                _connection.exec("SET autocommit=0");
        }

        void transactionStart()
        {
            _connection.exec("START TRANSACTION");
        }

        void transactionCommit()
        {
            _connection.exec("COMMIT");
        }

        void transactionRollback()
        {
            _connection.exec("ROLLBACK");
        }
    }

    public  // ORMDatabaseDriver
    {
    }

    public  // Extras
    {
        Connection getConnection()
        {
            return this._connection;
        }
    }
}
