import pymysql
import click

CONFIG_FILE_PATH = 'src/main/python_service/config/config.py'


def update_config_file(db_host, db_user, dbw_password, db_name):
    config_lines = [
        f"DB_HOST_NAME = '{db_host}'\n",
        f"DB_USER = '{db_user}'\n",
        f"DB_PASSWORD = '{db_password}'\n",
        f"DB_NAME = '{db_name}'\n",
        "REDIS_HOST_NAME = 'localhost'\n",
        "REDIS_PORT = 6379\n"
    ]

    with open(CONFIG_FILE_PATH, 'w') as config_file:
        config_file.writelines(config_lines)

@click.command()
@click.option('--username', prompt='MySQL username', help='The username for your MySQL database.')
@click.option('--password', prompt=True, hide_input=True, help='The password for your MySQL database.')
@click.option('--database', prompt='Database name', help='The name of the database to use.')
@click.option('--host', prompt='Database host', default='localhost', help='The host of your MySQL database.')
def setup(username, password, database, host):
    """
    Sets up the MySQL database with the necessary tables.
    """
    try:
        connection = pymysql.connect(
            host=host,
            user=username,
            passwd=password,
            db=database
        )
        cursor = connection.cursor()

        # Create sec_table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sec_table (
            cik INT,
            year INT,
            company TEXT,
            report_type TEXT,
            url TEXT,
            PRIMARY KEY (cik, year)
        );
        """)

        # Create company_financials table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS company_financials (
            cik INT,
            year INT,
            PRIMARY KEY (cik, year)
        );
        """)

        connection.commit()
        cursor.close()
        connection.close()

        # Update the configuration file
        update_config_file(host, username, password, database)

        click.echo("Database setup complete. Configuration file updated. You can now use the CLI to add financial data and process it.")
    except pymysql.MySQLError as e:
        click.echo(f"Error: {e}")


if __name__ == "__main__":
    setup()
