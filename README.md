# Financial Data Analysis Tool

This project provides a command-line tool to automate the extraction, analysis, and storage of financial data from SEC filings. It leverages network graph analysis techniques to identify relationships and patterns within the data.

## Features

- **Automated Data Extraction:**  Efficiently fetches financial data from SEC filings for specified companies and time periods.
- **Structured Storage:** Stores extracted data in a relational database for easy querying and analysis.
- **Network Graph Analysis:** Utilizes network graph algorithms to uncover relationships and patterns within financial statements.
- **Customizable:** Allows configuration of data sources, timeframes, and analysis parameters.

## Installation

1. **Clone the Repository:**
   ```bash
   git clone (https://github.com/ByteQuester/fin-statement-calculation-falttener)
   ```

2. **Create a Virtual Environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configuration:**
   - Create a `config.py` file in `src/main/python_service/config/` with your database credentials (see example in `config.py`).

## Usage

### Command-Line Interface

1. **Database Setup:**
   ```bash
   python src/main/setup.py
   ```

2. **Add Financial Data (by CIK and Year):**
   ```bash
   python cli.py add-financial-data <CIK> <YEAR> 
   ```

3. **Process Data (Network Analysis):**
   ```bash
   python cli.py process-data
   ```

### Full Pipeline

```bash
python src/main/main.py --start_year <YEAR> --end_year <YEAR>
```

## Project Structure

```
project/
    cli.py      # Command-line interface
    src/
        main/
            setup.py    # Database setup
            main.py     # Full pipeline script
            python_service/  # Data access and service logic
            python_process/  # Data processing and analysis logic
    requirements.txt
    README.md  
    ...
```

## Contributing

Contributions are welcome! 
