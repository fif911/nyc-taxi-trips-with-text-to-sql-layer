"""
Custom Chart Tool for Vanna 2.0

This tool enables chart generation from SQL query results using Plotly.
"""
from typing import Any, Dict, List, Optional
import pandas as pd
from vanna.core.tool import Tool, ToolContext, ToolResult
from vanna.components import UiComponent, SimpleTextComponent
from vanna.components.rich import ChartComponent
from pydantic import BaseModel
from athena_tool import AthenaRunner


class ChartArgs(BaseModel):
    """Arguments for the chart tool"""
    sql: str
    chart_type: str = "bar"  # bar, line, scatter, pie, etc.
    x_column: Optional[str] = None
    y_column: Optional[str] = None
    title: Optional[str] = None
    x_label: Optional[str] = None
    y_label: Optional[str] = None


class AthenaChartTool(Tool[ChartArgs]):
    """
    Custom tool that executes SQL queries and generates Plotly charts from results.
    
    This tool:
    1. Executes a SQL query using AthenaRunner
    2. Generates a Plotly chart from the results
    3. Returns the chart as a UI component
    """
    
    def __init__(self, sql_runner: AthenaRunner):
        """
        Initialize the chart tool
        
        Args:
            sql_runner: AthenaRunner instance for executing SQL queries
        """
        self.sql_runner = sql_runner
    
    @property
    def name(self) -> str:
        return "create_chart"
    
    @property
    def description(self) -> str:
        return """Create an interactive chart from SQL query results. 
        
        This tool executes a SQL query and visualizes the results as a chart.
        
        Args:
            sql: SQL query to execute
            chart_type: Type of chart (bar, line, scatter, pie, area, etc.)
            x_column: Column name for X-axis (optional, will use first column if not specified)
            y_column: Column name for Y-axis (optional, will use second column if not specified)
            title: Chart title (optional)
            x_label: X-axis label (optional)
            y_label: Y-axis label (optional)
        """
    
    def get_args_schema(self):
        return ChartArgs
    
    async def execute(self, context: ToolContext, args: ChartArgs) -> ToolResult:
        """
        Execute SQL query and generate chart
        
        Args:
            context: Tool execution context
            args: Chart arguments
            
        Returns:
            ToolResult with chart component
        """
        try:
            # Execute SQL query
            df = await self.sql_runner.run_sql(args.sql, user=context.user)
            
            if df is None or df.empty:
                return ToolResult(
                    success=False,
                    result_for_llm="Query returned no results",
                    ui_component=UiComponent(
                        simple_component=SimpleTextComponent(text="No data to visualize")
                    )
                )
            
            # Determine columns for X and Y axes
            columns = df.columns.tolist()
            x_col = args.x_column or (columns[0] if len(columns) > 0 else None)
            y_col = args.y_column or (columns[1] if len(columns) > 1 else columns[0])
            
            if x_col not in columns:
                x_col = columns[0]
            if y_col not in columns:
                y_col = columns[0] if len(columns) == 1 else columns[1]
            
            # Generate Plotly chart specification
            chart_spec = self._generate_plotly_spec(
                df=df,
                x_column=x_col,
                y_column=y_col,
                chart_type=args.chart_type,
                title=args.title or f"{args.chart_type.title()} Chart",
                x_label=args.x_label or x_col,
                y_label=args.y_label or y_col
            )
            
            # Create chart component
            try:
                # ChartComponent requires: type='chart', data (Plotly spec), chart_type='plotly'
                chart = ChartComponent(
                    type="chart",  # ComponentType.CHART
                    data=chart_spec,  # The Plotly spec goes in 'data' field
                    chart_type="plotly",  # Specify it's a Plotly chart
                    title=args.title or f"{args.chart_type.title()} Chart"
                )
                print(f"✅ Chart component created successfully")
            except Exception as chart_error:
                import traceback
                print(f"❌ Error creating ChartComponent: {chart_error}")
                print(f"Traceback: {traceback.format_exc()}")
                # Fallback: return data as text
                return ToolResult(
                    success=True,
                    result_for_llm=f"Query executed successfully with {len(df)} rows. Chart creation failed: {str(chart_error)}",
                    ui_component=UiComponent(
                        simple_component=SimpleTextComponent(
                            text=f"Data ({len(df)} rows):\n{df.to_string()}\n\nChart error: {str(chart_error)}"
                        )
                    )
                )
            
            return ToolResult(
                success=True,
                result_for_llm=f"Created {args.chart_type} chart from query results with {len(df)} rows",
                ui_component=UiComponent(
                    rich_component=chart,
                    simple_component=SimpleTextComponent(
                        text=f"Chart: {args.title or args.chart_type} ({len(df)} data points)"
                    )
                )
            )
            
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            print(f"❌ Chart tool error: {str(e)}")
            print(f"Traceback: {error_trace}")
            return ToolResult(
                success=False,
                result_for_llm=f"Error creating chart: {str(e)}",
                ui_component=UiComponent(
                    simple_component=SimpleTextComponent(text=f"Error: {str(e)}")
                )
            )
    
    def _generate_plotly_spec(
        self,
        df: pd.DataFrame,
        x_column: str,
        y_column: str,
        chart_type: str,
        title: str,
        x_label: str,
        y_label: str
    ) -> Dict[str, Any]:
        """
        Generate Plotly chart specification
        
        Args:
            df: DataFrame with data
            x_column: X-axis column name
            y_column: Y-axis column name
            chart_type: Type of chart
            title: Chart title
            x_label: X-axis label
            y_label: Y-axis label
            
        Returns:
            Plotly chart specification dictionary
        """
        # Prepare data
        x_data = df[x_column].tolist()
        y_data = df[y_column].tolist()
        
        # Base trace configuration
        trace = {
            'x': x_data,
            'y': y_data,
            'type': self._map_chart_type(chart_type),
            'name': y_column
        }
        
        # Add chart-specific configurations
        if chart_type in ['line', 'scatter']:
            trace['mode'] = 'lines+markers' if chart_type == 'line' else 'markers'
        elif chart_type == 'pie':
            trace = {
                'labels': x_data,
                'values': y_data,
                'type': 'pie'
            }
        elif chart_type == 'area':
            trace['fill'] = 'tozeroy'
            trace['type'] = 'scatter'
            trace['mode'] = 'lines'
        
        # Create layout
        layout = {
            'title': title,
            'xaxis': {'title': x_label},
            'yaxis': {'title': y_label},
            'hovermode': 'closest',
            'template': 'plotly_white'
        }
        
        # Add legend for multi-series charts
        if len(df.columns) > 2:
            layout['legend'] = {'x': 0, 'y': 1}
        
        return {
            'data': [trace],
            'layout': layout
        }
    
    def _map_chart_type(self, chart_type: str) -> str:
        """
        Map chart type to Plotly trace type
        
        Args:
            chart_type: User-specified chart type
            
        Returns:
            Plotly trace type
        """
        mapping = {
            'bar': 'bar',
            'line': 'scatter',
            'scatter': 'scatter',
            'pie': 'pie',
            'area': 'scatter',
            'histogram': 'histogram',
            'box': 'box',
            'violin': 'violin'
        }
        return mapping.get(chart_type.lower(), 'bar')
