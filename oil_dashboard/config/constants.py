EIA_URL: str = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"
BAKER_HUGES_US_RIG_COUNT_URL: str = (
    "https://rigcount.bakerhughes.com/rig-count-overview"
)

BAKER_HUGHES_COLUMNS_US = {
    "date": "Last Count",  # most recent count date
    "count": "Count",  # number of active rigs
    "weekly_change": "Change from Prior Count",
    "prior_week_count_date": "Date of Prior Count",
    "yearly_change": "Change from Last Year",
    "last_years_count_date": "Date of Last Year's Count",
}
