

from shareloader.modules.superperf_metrics import get_all_data, get_descriptive_and_technical, \
                get_financial_ratios, get_fmprep_historical, get_quote_benchmark, \
                get_financial_ratios_benchmark, get_key_metrics_benchmark, get_income_benchmark,\
                get_balancesheet_benchmark, compute_cagr, calculate_piotrosky_score, \
                get_institutional_holders_quote, filter_historical, get_latest_stock_news,\
                get_mm_trend_template, get_fundamental_parameters, get_peter_lynch_ratio


def get_fundamental_data(ticker, key):
    ## we get ROC, divYield and pl ratio
    metrics = get_key_metrics_benchmark(ticker, key)
    ratios = get_financial_ratios(ticker, key)

    ratios.update(metrics)

    peterlynch = get_peter_lynch_ratio(key, 'AAPL', ratios)

    ratios['peterLynchRatio'] = peterlynch
    return ratios






