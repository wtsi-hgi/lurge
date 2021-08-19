import mysql.connector


def getSQLConnection(config) -> mysql.connector.MySQLConnection:
    # connects to the MySQL server used to store the report data, change the
    # credentials here to point at your desired database
    port = config.PORT if config.PORT is not None else 3306

    db_con = mysql.connector.connect(
        host=config.HOST,
        database=config.DATABASE,
        port=port,
        user=config.USER,
        passwd=config.PASSWORD
    )

    return db_con
