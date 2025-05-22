from rich import print, box
from rich.table import Table
from rich.console import Console

console = Console(width=140)

def log(
    message: str,
    status: str = "Success",
    is_success: bool = False,
    is_failure: bool = False,
    is_warning: bool = False,
    component: str = "GlassFlow",
    **print_kwargs,
):
    if is_success and not is_failure and not is_warning:
        status_icon = "[green]✔[/green]"
        status_message = f"[green]{status}[/green]"
    elif is_failure and not is_success and not is_warning:
        status_icon = "[red]✗[/red]"
        status_message = f"[red]{status}[/red]"
    elif is_warning and not is_success and not is_failure:
        status_icon = "[yellow]△[/yellow]"
        status_message = f"[yellow]{status}[/yellow]"
    elif not any([is_success, is_failure, is_warning]):
        raise ValueError(
            "At least one of is_success, is_failure, or is_warning must be True"
        )
    else:
        raise ValueError(
            "Only one of is_success, is_failure, or is_warning can be True"
        )
            
    if component == "Kafka":
        component_str = "[bold sky_blue3][Kafka][/bold sky_blue3]"
    elif component == "Clickhouse":
        component_str = "[bold yellow][Clickhouse][/bold yellow]"
    else:
        component_str = f"[bold orange_red1][{component}][/bold orange_red1]"
    
    table = Table(
        show_header=False,
        show_edge=False,
        padding=(0, 1),
        show_lines=False,
        box=box.SIMPLE_HEAD,
    )
    table.add_column("Status", justify="left", width=2)
    table.add_column("Component", justify="left", width=12)
    table.add_column("Message", justify="left", width=80)
    table.add_column("Status", justify="left", width=20)
    table.add_row(status_icon, component_str, message, status_message)
    print(table, **print_kwargs) 