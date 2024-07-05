import click
from src.main.python_service.service.dataAccess.indexDataAccess import IndexDataAccess
from src.main.python_process.NetworkXProcessor.graphX import process_all_tables


@click.group()
def cli():
    pass


@click.command()
@click.argument('cik', type=int)
@click.argument('year', type=int)
def dump(cik, year):
    """
    Adds financial data for a given CIK and year to the company_financials table.
    """
    index_data_access = IndexDataAccess()
    index_data_access.add_company_financial(cik, year)
    click.echo(f"Added financial data for CIK {cik} and year {year} to company_financials.")


@click.command()
def process():
    """
    Processes data for all tables, years, and link roles, and updates the respective tables.
    """
    index_data_access = IndexDataAccess()
    process_all_tables(index_data_access)
    click.echo("Processed all data and updated tables.")


cli.add_command(dump)
cli.add_command(process)

if __name__ == "__main__":
    cli()
