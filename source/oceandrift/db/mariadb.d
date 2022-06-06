module oceandrift.db.maridb;

import mysql.safe;
import oceandrift.db.dbal;

class MariaDBDatabaseDriver : oceandrift.db.dbal.DatabaseDriver
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
        this._host = host;
        this._port = port;
        this._username = username;
        this._password = password;
        this._database = database;
    }

    public  // DatabaseDriver
    {
        void connect()
        {
            this._connection = new Connection(
                this._host,
                this._username,
                this._password,
                this._database,
                this._port
            );
        }

        void close()
        {
            this._connection.close();
        }

        bool connected()
        {
            return ((this._connection !is null)
                    && !this._connection.closed);
        }

        bool autoCommit()
        {
            return this._connection
                .queryRow("SHOW VARIABLES LIKE 'autocommit'")
                .get[1] == "ON";
        }

        void autoCommit(bool enable)
        {
            if (enable)
                this._connection.exec("SET autocommit=1");
            else
                this._connection.exec("SET autocommit=0");
        }

        void transactionStart()
        {
            this._connection.exec("START TRANSACTION");
        }

        void transactionCommit()
        {
            this._connection.exec("COMMIT");
        }

        void transactionRollback()
        {
            this._connection.exec("ROLLBACK");
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
