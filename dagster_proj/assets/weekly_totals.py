import os
from datetime import datetime, timedelta
from pathlib import Path

from dagster import (
    asset,
    get_dagster_logger,
    EnvVar,
    AssetExecutionContext,
    MetadataValue
)

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

from ..utils import dynamic_query

DAGSTER_ENVIRONMENT = EnvVar("DAGSTER_ENVIRONMENT").get_value()

logger = get_dagster_logger()

@asset(
    deps=["sport_type_weekly_totals"],
    compute_kind="Snowflake",
    required_resource_keys={"database"},
)
def sport_type_totals(
    context: AssetExecutionContext,
):
    """Gets weekly sport_type totals data"""
    query = """
        select *
        from dbt.sport_type_weekly_totals
    """
    df = dynamic_query(
        dagster_environment=DAGSTER_ENVIRONMENT,
        context=context,
        query=query,
    )

    context.log.info(f"Size of data: {df.shape}")

    return df

@asset
def sports_activity_dashboard(context: AssetExecutionContext, sport_type_totals):
    df = sport_type_totals

    # week_start to datetime
    df['week_start'] = pd.to_datetime(df['week_start'])

    most_recent_date = df['week_start'].max()

    # get the last 8 weeks
    eight_weeks_ago = most_recent_date - timedelta(weeks=7)
    df = df[df['week_start'] > eight_weeks_ago]

    # totals
    total_prs = df['pr_count'].sum()
    total_achievements = df['achievement_count'].sum()

    pio.templates.default = "plotly"

    # main plotly dashboard
    fig = make_subplots(
        rows=4, cols=2,
        specs=[
            [{"type": "indicator"}, {"type": "indicator"}],
            [{"type": "pie"}, {"type": "scatter"}],
            [{"type": "bar", "colspan": 2}, None],
            [{"type": "bar", "colspan": 2}, None]
        ],
        subplot_titles=("Total PRs", "Total Achievements", 
                        "Sport Type Distribution", "Distance per Week", 
                        "Total Hours per Week",
                        "Number of Activities per Week"),
        vertical_spacing=0.1,
        row_heights=[0.2, 0.3, 0.25, 0.25]
    )

    # 1. Total PRs indicator
    fig.add_trace(
        go.Indicator(
            mode="number",
            value=total_prs,
            title={"text": "Total PRs", "font": {"size": 20}},
            number={"font": {"size": 40}},
            domain={"row": 0, "column": 0}
        ),
        row=1, col=1
    )

    # 2. Total Achievements indicator
    fig.add_trace(
        go.Indicator(
            mode="number",
            value=total_achievements,
            title={"text": "Total Achievements", "font": {"size": 20}},
            number={"font": {"size": 40}},
            domain={"row": 0, "column": 1}
        ),
        row=1, col=2
    )

    # 3. Pie chart for sport type distribution
    sport_type_counts = df['sport_type'].value_counts()
    fig.add_trace(
        go.Pie(labels=sport_type_counts.index, values=sport_type_counts.values, name="Sport Types"),
        row=2, col=1
    )

    # 4. Line graph for distance per week with average
    df_weekly = df.groupby('week_start').agg({
        'distance_miles': 'sum',
        'moving_time_minutes': 'sum',
        'num_activities': 'sum'
    }).reset_index()

    avg_distance = df_weekly['distance_miles'].mean()

    fig.add_trace(
        go.Scatter(x=df_weekly['week_start'], y=df_weekly['distance_miles'], 
                   name="Distance (miles)", line=dict(color='red')),
        row=2, col=2
    )
    fig.add_trace(
        go.Scatter(x=[df_weekly['week_start'].min(), df_weekly['week_start'].max()], 
                   y=[avg_distance, avg_distance], 
                   name="Avg Distance", line=dict(color='red', dash='dash')),
        row=2, col=2
    )

    # 5. Bar chart for total hours per week with average
    df_weekly['total_hours'] = df_weekly['moving_time_minutes'] / 60
    avg_hours = df_weekly['total_hours'].mean()

    fig.add_trace(
        go.Bar(x=df_weekly['week_start'], y=df_weekly['total_hours'], name="Total Hours"),
        row=3, col=1
    )
    fig.add_trace(
        go.Scatter(x=[df_weekly['week_start'].min(), df_weekly['week_start'].max()], 
                   y=[avg_hours, avg_hours], 
                   name="Avg Hours", line=dict(color='orange', dash='dash')),
        row=3, col=1
    )

    #6. Bar chart of total activities per week
    fig.add_trace(
        go.Bar(x=df_weekly['week_start'], y=df_weekly['num_activities'], name="Number of Activities"),
        row=4, col=1
    )

    fig.update_xaxes(title_text="Week", row=4, col=1)
    fig.update_yaxes(title_text="Number of Activities", row=4, col=1)

    # final layout updates
    fig.update_layout(height=1200, width=1200, title_text="Sports Activity Dashboard (Last 8 Weeks)")
    fig.update_xaxes(title_text="Week", row=3, col=1)
    fig.update_yaxes(title_text="Hours", row=3, col=1)
    fig.update_xaxes(title_text="Week", row=2, col=2)
    fig.update_yaxes(title_text="Distance (miles)", row=2, col=2)

    # HTML file path to save
    save_chart_path = Path(context.instance.storage_directory()).joinpath("sports_activity_dashboard.html")
    
    # save file
    fig.write_html(save_chart_path, auto_open=False)

    # this is needed to allow generated filepath to be accessible to UI
    context.add_output_metadata({
        "plot_url": MetadataValue.url("file://" + os.fspath(save_chart_path))
    })

    return fig
