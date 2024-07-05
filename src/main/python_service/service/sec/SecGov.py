import logging
from typing import List

import requests

from src.main.python_service.service.dataAccess.indexDataAccess import IndexDataAccess
#from src.main.python_service.service.sec.SecGov import SecGov

class SecGov:

    SEC_ARCHIVE_URL = 'https://www.sec.gov/Archives/'
    TICKER_CIK_LIST_URL = 'https://www.sec.gov/include/ticker.txt'
    HEADERS = {'User-Agent': 'user@gmail.com'}

    def fetch_tickers_list(self) -> List[str]:
        """Fetch a list of tickers from sec, and store them in the DB.
        Skip if already in cache.
        Returns:
            a list of tickers
        """
        ticker_list = []
        resp = requests.get(self.TICKER_CIK_LIST_URL, headers=self.HEADERS)
        ticker_cik_list_lines = resp.content.decode("utf-8").split('\n')
        for entry in ticker_cik_list_lines:
            ticker, cik = entry.strip().split()
            ticker = ticker.strip()
            cik = cik.strip()
            IndexDataAccess().store_ticker_cik_mapping(ticker, cik)
            ticker_list.append(ticker)
        logging.info(f'Successfully mapped tickers to cik')
        return ticker_list

    def fetch_ticker_financials_by_year(self, year: int, ticker:str = None) -> None:
        """Fetch ticker data according to the passed year and store to DB
        Args:
            year int: The year to fetch stocks data_assets
            ticker str: if not None cache this ticker, otherwise cache all
        Returns:
            None
        """
        # check if the index exists
        is_ixd_stored = IndexDataAccess().is_index_stored(year)
        if not is_ixd_stored:
            logging.info(f"Index file for year {year} is not accessible, fetching from web")
            for q in range(1, 5):  # 1 to 4 inclusive
                self._prepare_index(year, q)

        if ticker:
            ticker_cik = IndexDataAccess().get_ticker_cik(ticker)
            result = IndexDataAccess.get_index_row_by_cik(ticker_cik, year)
            if result is not None:
                ticker_info_hash = {
                    'company_name': result[0],
                    f'txt_url:{year}': result[1]
                }
                IndexDataAccess().store_ticker_info(ticker, ticker_info_hash)
                IndexDataAccess().commit_ticker_data()
            else:
                logging.info(f'Could not fetch data for {ticker} for year {year}')
        else:
            idx = IndexDataAccess().get_index_by_year(year)
            for result in idx:
                current_ticker = IndexDataAccess().get_ticker_by_cik(result[2])
                if result is not None:
                    ticker_info_hash = {
                        'company_name': result[0],
                        f'txt_url:{year}': result[1]
                    }
                    IndexDataAccess().store_ticker_info(current_ticker, ticker_info_hash)
                else:
                    logging.info(f'Could not fetch data for {ticker} for year {year}')
            IndexDataAccess().commit_ticker_data()

    def _prepare_index(self, year: int, quarter: int) -> None:
        """Prepare the edgar index for the passed year and quarter
        The data_assets will be saved to DB
        Args:
            year int: The year to build the index for
            quarter int: The quarter to build the index between 1-4
        Returns:
            None
        """
        filing = '|10-K|'
        download = requests.get(f'{self.SEC_ARCHIVE_URL}/edgar/full-index/{year}/QTR{quarter}/master.idx', headers=self.HEADERS).content
        decoded = download.decode("ISO-8859-1").split('\n')

        IndexDataAccess().store_index(decoded, year, filing)
        logging.info(f"Inserted year {year} qtr {quarter} to DB")
