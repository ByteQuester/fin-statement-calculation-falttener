import logging
from typing import List, Dict, Tuple

import pandas as pd
import pymysql
import redis

from src.main.python_service.config import config


class IndexDataAccess:

    DBConnection = pymysql.connect(
        host=config.DB_HOST_NAME,
        user=config.DB_USER,
        passwd=config.DB_PASSWORD,
        db=config.DB_NAME
    )
    redis_client = redis.Redis(
        host=config.REDIS_HOST_NAME,
        port=config.REDIS_PORT,
        decode_responses=True
    )

    REDIS_TICKER_SET = 'ticker_set'
    REDIS_CIK2TICKER_KEY = 'cik2ticker'

    # --- first level
    def get_all_tables(self) -> List[str]:
        """
        Retrieves all table names from the database excluding specific ones.
        """
        with self.DBConnection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

        tables_to_exclude = {'sec_table', 'company_financials', 'master_table'}
        tables_to_include = [table[0] for table in tables if table[0] not in tables_to_exclude]

        return tables_to_include

    def get_all_years_for_table(self, table_name: str) -> List[int]:
        """
        Retrieves all unique years for a specific table from the database.
        """
        with self.DBConnection.cursor() as cursor:
            query = f"SELECT DISTINCT year FROM `{table_name}`"
            cursor.execute(query)
            years = [row[0] for row in cursor.fetchall()]
        return years

    def get_all_linkroles_for_year(self, table_name: str, year: int) -> List[str]:
        """
        Retrieves all unique linkroles for a specific year from the database.
        """
        with self.DBConnection.cursor() as cursor:
            query = f"SELECT DISTINCT linkrole FROM `{table_name}` WHERE year = {year}"
            cursor.execute(query)
            linkroles = [row[0] for row in cursor.fetchall()]
        return linkroles

    def get_data_for_linkrole(self, table_name: str, year: int, linkrole: str) -> List[Dict[str, any]]:
        """
        Retrieves data for a specific linkrole in a specific year from the database.
        """
        with self.DBConnection.cursor() as cursor:
            query = f"""
            SELECT concept, sub_concept, weight 
            FROM `{table_name}` 
            WHERE year = {year} 
            AND linkrole = '{linkrole}'
            """
            cursor.execute(query)
            data = cursor.fetchall()
        return [{'concept': row[0], 'sub_concept': row[1], 'weight': row[2]} for row in data]
    def add_columns_if_not_exist(self, table_name: str) -> None:
        """
        Adds the 'level' and 'root_concept' columns to the table if they do not exist.
        """
        with self.DBConnection.cursor() as cursor:
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE 'level'")
            result = cursor.fetchone()
            if not result:
                cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `level` INT")

            cursor.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE 'root_concept'")
            result = cursor.fetchone()
            if not result:
                cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `root_concept` TEXT")
        self.DBConnection.commit()

    def update_table_with_results(self, table_name: str, df: pd.DataFrame) -> None:
        """
        Updates the given table with the processed results from the DataFrame.
        """
        self.add_columns_if_not_exist(table_name)
        with self.DBConnection.cursor() as cursor:
            for _, row in df.iterrows():
                update_query = f"""
                UPDATE `{table_name}`
                SET level = %s, root_concept = %s
                WHERE concept = %s AND sub_concept = %s AND linkrole = %s AND year = %s
                """
                cursor.execute(update_query, (
                    row['level'],
                    ','.join(row['root_concept']),
                    row['concept'],
                    row['sub_concept'],
                    row['linkrole'],
                    row['year']
                ))
        self.DBConnection.commit()
    def store_index(self, data: List[str], year, filling: str) -> None:
        with self.DBConnection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `sec_table` (`cik`, `year`, `company`, `report_type`, `url`) VALUES (%s, %s, %s, %s, %s)"
            for item in data:
                if filling in item:
                    values = item.split('|')
                    try:
                        cursor.execute(sql, (values[0], int(year), values[1], values[2], values[4]))
                    # TBD - some companies have multiple 10-K
                    except pymysql.err.IntegrityError:
                        pass
        self.DBConnection.commit()

    def add_company_financial(self, cik: int, year: int) -> None:
        """
        Adds a new entry to the company_financials table.
        """
        with self.DBConnection.cursor() as cursor:
            query = f"INSERT INTO company_financials (cik, year) VALUES (%s, %s)"
            cursor.execute(query, (cik, year))
        self.DBConnection.commit()

    def store_ticker_cik_mapping(self, ticker: str, cik: str) -> None:
        try:
            self.redis_client.hset(self._info_key(ticker), 'cik', cik)
            self.redis_client.hset(f'{self.REDIS_CIK2TICKER_KEY}', cik, ticker)
            self.redis_client.sadd(self.REDIS_TICKER_SET, ticker)
        except redis.ResponseError as error:
            logging.debug(f'{error} ticker: {ticker}')

    @staticmethod
    def _info_key(ticker: str) -> str:
        return f'{ticker}:info'

    def is_index_stored(self, year: int) -> bool:
        with self.DBConnection.cursor() as cursor:
            sql = f'SELECT COUNT(*) FROM {config.DB_NAME}.sec_table where year={year}'
            cursor.execute(sql)
            result = cursor.fetchone()
            #return result[0]
            return result[0] > 0

    def is_ticker_list_exist(self) -> bool:
        try:
            return self.redis_client.exists(self.REDIS_TICKER_SET)
        except redis.ResponseError as error:
            logging.debug(f'{error}')

    def commit_ticker_data(self):
        try:
            return self.redis_client.bgsave()
        except redis.ResponseError as error:
            logging.debug(error)

    def store_ticker_info(self, ticker: str, data: dict):
        try:
            self.redis_client.hset(self._info_key(ticker), mapping=data)
        except redis.ResponseError as error:
            logging.debug(f'{error} ticker: {ticker}')

    def get_ticker_by_cik(self, ticker):
        try:
            return self.redis_client.hget(self.REDIS_CIK2TICKER_KEY, ticker)
        except redis.ResponseError as error:
            logging.debug(f'{error} ticker: {ticker}')

    def get_index_row_by_cik(self, cik: int, year: int) -> List[str]:
        with self.DBConnection.cursor() as cursor:
            # Create a new record
            sql = f'SELECT company, url FROM sec_table where cik={cik} and year={year}'
            cursor.execute(sql)
            return cursor.fetchone()

    def get_index_by_year(self, year: int) -> List[List[str]]:
        with self.DBConnection.cursor() as cursor:
            # Create a new record
            sql = f'SELECT company, url, cik FROM sec_table where year={year}'
            cursor.execute(sql)
            return cursor.fetchall()

    def get_ticker_cik(self, ticker: str):
        try:
            return self.redis_client.hget(self._info_key(ticker), 'cik')
        except redis.ResponseError as error:
            logging.debug(f'{error} ticker: {ticker}')

    # --- second level with df as in/-output
    def create_and_populate_mysql_database(self, dfs: Dict[str, pd.DataFrame]) -> None:
        """
        Creates a MySQL database (if it doesn't exist) and populates it with the DataFrame data.
        Args:
            dfs: A dictionary where keys are CIKs and values are DataFrames.
        """
        with self.DBConnection.cursor() as cursor:
            # Create the database if it doesn't exist
            try:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config.DB_NAME}")
                logging.info(f"Database '{config.DB_NAME}' created or already exists")
                self.DBConnection.database = config.DB_NAME  # Switch to the new database
            except pymysql.err.ProgrammingError as e:
                logging.error(f"Error creating database: {e}")
                return

            for cik, df in dfs.items():
                logging.info(f"Processing CIK {cik}")
                table_name = f"cal_{cik}"

                # Data cleaning and preparation (same as before)
                df['cik'] = df['cik'].astype(str).str.replace(r'\D', '', regex=True)
                df['weight'] = pd.to_numeric(df['weight'], errors='coerce')

                # Drop existing table if it exists
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                except pymysql.err.ProgrammingError as e:
                    logging.error(f"Error dropping table: {e}")
                    return

                # Create table (same as before)
                create_table_query = f"""
                     CREATE TABLE `{table_name}` (
                         `id` INT AUTO_INCREMENT PRIMARY KEY,
                         `cik` VARCHAR(20),
                         `year` INTEGER,
                         `linkrole` VARCHAR(255),
                         `concept` VARCHAR(255),
                         `sub_concept` VARCHAR(255),
                         `weight` FLOAT
                     )
                 """
                try:
                    cursor.execute(create_table_query)
                except pymysql.err.ProgrammingError as e:
                    logging.error(f"Error creating table: {e}")
                    return

                    # Insert data (same as before)
                insert_query = f"""
                     INSERT INTO `{table_name}` (cik, year, linkrole, concept, sub_concept, weight) 
                     VALUES (%s, %s, %s, %s, %s, %s)
                 """
                for _, row in df.iterrows():
                    values = (
                    row['cik'], row['year'], row['linkrole'], row['concept'], row['sub_concept'], row['weight'])
                    try:
                        cursor.execute(insert_query, values)
                    except pymysql.err.IntegrityError:
                        pass  # Handle duplicates
                    except pymysql.err.ProgrammingError as e:
                        logging.error(f"Error inserting data: {e}")
                        return

                self.DBConnection.commit()

    def fetch_calculation_data_single(self, cik: int, year: int, link_role: str) -> pd.DataFrame:
        """Fetches calculation data from the specified table for the given CIK, year, and link role.

        Args:
            cik: The CIK number of the company.
            year: The year of the report.
            link_role: The specific link role to filter the data.

        Returns:
            A pandas DataFrame containing the fetched calculation data.
        """
        table_name = f"cal_{cik}"

        # Define the query to extract concept hierarchy and weight from the database
        query = f"""
        SELECT
            linkrole,
            year,
            REPLACE(REPLACE(REPLACE(concept, '(+1)', ''), '(-1)', ''), 'us-gaap:', '') AS concept,
            REPLACE(REPLACE(REPLACE(sub_concept, '(+1)', ''), '(-1)', ''), 'us-gaap:', '') AS sub_concept,
            weight
        FROM `{table_name}`
        WHERE year = %s AND linkrole = %s
        """

        try:
            # Execute the query with parameters (year, link_role)
            with self.DBConnection.cursor() as cursor:
                cursor.execute(query, (year, link_role))
                results = cursor.fetchall()
        except pymysql.err.ProgrammingError:
            logging.error(f"Error executing query on table {table_name}")
            return pd.DataFrame()  # Return an empty DataFrame in case of errors

        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(results, columns=columns)

        return df

    def fetch_calculation_data(self) -> Dict[Tuple[str, int, str], pd.DataFrame]:
        """Fetches calculation data from all relevant tables, years, and link roles.

        Returns:
            A dictionary where keys are tuples (table_name, year, link_role)
            and values are corresponding pandas DataFrames with the fetched data.
        """

        all_data = {}  # To store data from all tables

        # Get all tables starting with 'cal_'
        with self.DBConnection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'cal_%'")
            cal_tables = [table[0] for table in cursor.fetchall()]

        for table_name in cal_tables:
            cik = table_name.split("_")[1]  # Extract CIK from table name

            # Get distinct years from the table
            cursor.execute(f"SELECT DISTINCT year FROM `{table_name}`")
            years = [year[0] for year in cursor.fetchall()]

            # Get distinct link roles from the table
            cursor.execute(f"SELECT DISTINCT linkrole FROM `{table_name}`")
            link_roles = [link_role[0] for link_role in cursor.fetchall()]

            for year in years:
                for link_role in link_roles:
                    # Fetch data for the current table, year, and link role
                    query = f"""
                    SELECT
                        linkrole,
                        year,
                        REPLACE(REPLACE(REPLACE(concept, '(+1)', ''), '(-1)', ''), 'us-gaap:', '') AS concept,
                        REPLACE(REPLACE(REPLACE(sub_concept, '(+1)', ''), '(-1)', ''), 'us-gaap:', '') AS sub_concept,
                        weight
                    FROM `{table_name}`
                    WHERE year = %s AND linkrole = %s
                    """
                    try:
                        cursor.execute(query, (year, link_role))
                        results = cursor.fetchall()

                        # If results are found, create a DataFrame and store it
                        if results:
                            columns = [col[0] for col in cursor.description]
                            df = pd.DataFrame(results, columns=columns)
                            all_data[(table_name, year, link_role)] = df

                    except pymysql.err.ProgrammingError:
                        logging.error(f"Error executing query on table {table_name}")

        return all_data
