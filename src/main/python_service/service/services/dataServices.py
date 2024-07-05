from src.main.python_service.service.dataAccess.indexDataAccess import IndexDataAccess
from src.main.python_service.service.sec.SecGov import SecGov
#from src.main.python_service.service.services.dataServices import DataServices
#DataServices().fetch_ticker_financials_by_year_range(2020,2021)


class DataServices:

    def __init__(self):
        self.sec_gov = SecGov()
        self.data_access = IndexDataAccess()
        if not self.data_access.is_ticker_list_exist():
            self.sec_gov.fetch_tickers_list()

    def fetch_index(self):
        pass

    @staticmethod
    def fetch_ticker_financials_by_year_range(start_year: int, end_year: int) -> None:
        """Fetch ticker data for a range of years and store to DB
        Args:
            start_year int: The starting year of the range
            end_year int: The ending year of the range
        Returns:
            None
        """
        for year in range(start_year, end_year + 1):
            SecGov().fetch_ticker_financials_by_year(year)
