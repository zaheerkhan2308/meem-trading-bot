# S&P 500 constituents (approximate, ~503 symbols across dual-class shares)
# Update periodically as index composition changes (~20-30 changes per year).

SP500: frozenset[str] = frozenset({
    # Information Technology
    "AAPL", "MSFT", "NVDA", "AVGO", "ORCL", "AMD", "QCOM", "TXN", "AMAT", "LRCX",
    "KLAC", "MU", "ADI", "MCHP", "NOW", "CRM", "ADBE", "INTU", "PANW", "CDNS",
    "SNPS", "FTNT", "ANSS", "AKAM", "CTSH", "IT", "JNPR", "HPE", "HPQ", "CSCO",
    "IBM", "ACN", "ANET", "GLW", "ZBRA", "NTAP", "STX", "VRSN", "CDW", "FFIV",
    "GEN", "TER", "SWKS", "MPWR", "KEYS", "ENPH", "FSLR", "WDC", "EPAM", "LDOS",
    "JKHY", "GDDY", "TRMB", "PAYC", "PTC", "NXPI", "DXC", "ROP",
    # Communication Services
    "GOOGL", "GOOG", "META", "NFLX", "DIS", "CMCSA", "T", "VZ", "TMUS", "CHTR",
    "IPG", "OMC", "LYV", "MTCH", "EA", "TTWO", "PARA", "WBD", "FOX", "FOXA", "NWSA",
    # Consumer Discretionary
    "AMZN", "TSLA", "HD", "LOW", "MCD", "SBUX", "NKE", "TJX", "ROST", "ORLY",
    "AZO", "DLTR", "DG", "BKNG", "MAR", "HLT", "MGM", "WYNN", "LVS", "RCL",
    "CCL", "NCLH", "F", "GM", "APTV", "BWA", "LKQ", "GRMN", "PHM", "DHI",
    "LEN", "NVR", "TOL", "PVH", "RL", "VF", "TPR", "CPRI", "YUM", "DRI", "CMG",
    "EXPE", "ABNB", "EBAY", "ETSY", "BBY", "NWL", "MHK", "POOL", "CZR", "HAS",
    "MAT", "NFLX",
    # Consumer Staples
    "WMT", "COST", "KR", "SYY", "PG", "KO", "PEP", "PM", "MO", "MDLZ", "HSY",
    "K", "GIS", "CPB", "HRL", "MKC", "CAG", "SJM", "KHC", "CL", "EL", "CHD",
    "CLX", "KDP", "MNST", "STZ", "TAP", "ADM", "BG", "INGR", "WBA", "POST",
    # Healthcare
    "UNH", "LLY", "JNJ", "ABBV", "MRK", "TMO", "ABT", "DHR", "BMY", "AMGN",
    "PFE", "CI", "CVS", "HCA", "SYK", "EW", "BDX", "ISRG", "ZBH", "BAX",
    "HOLX", "DXCM", "IDXX", "MTD", "WST", "A", "WAT", "RMD", "IQV", "VRTX",
    "REGN", "GILD", "BIIB", "MRNA", "ILMN", "ALGN", "GEHC", "MCK", "COR",
    "CAH", "MOH", "HUM", "ELV", "CNC", "HSIC", "XRAY", "TECH", "PODD", "INCY",
    "VTRS", "OGN", "SOLV", "DVA",
    # Financials
    "JPM", "V", "MA", "BAC", "WFC", "GS", "MS", "BLK", "SCHW", "AXP",
    "COF", "USB", "PNC", "TFC", "MTB", "RF", "HBAN", "CFG", "FITB", "KEY",
    "ZION", "CMA", "ALLY", "DFS", "SYF", "BK", "STT", "NTRS", "TROW", "IVZ",
    "AMG", "BEN", "VOYA", "AIG", "MET", "PRU", "AFL", "ALL", "HIG", "TRV",
    "CB", "PGR", "AON", "MMC", "WTW", "CINF", "GL", "FNF", "LNC", "EQH",
    "CBOE", "CME", "ICE", "NDAQ", "SPGI", "MCO", "MSCI", "VRSK", "BR", "FDS",
    "HOOD", "LPLA", "RJF", "SF", "SEIC", "UBSI", "WBS", "EWBC",
    # Energy
    "XOM", "CVX", "COP", "EOG", "PSX", "VLO", "MPC", "OXY", "HAL", "SLB",
    "BKR", "DVN", "FANG", "APA", "MRO", "HES", "OKE", "WMB", "KMI", "LNG",
    "CTRA", "EQT", "NOG",
    # Industrials
    "CAT", "DE", "HON", "RTX", "LMT", "BA", "GE", "GD", "NOC", "LHX",
    "TDG", "TXT", "HEI", "CARR", "OTIS", "EMR", "ETN", "PH", "ROK", "AME",
    "XYL", "FTV", "GNRC", "FAST", "GWW", "WSO", "CTAS", "RSG", "WM", "ECL",
    "URI", "AGCO", "PCAR", "CMI", "EXPD", "CHRW", "UPS", "FDX", "CSX", "NSC",
    "UNP", "LUV", "UAL", "DAL", "AAL", "JBHT", "ODFL", "AXON", "GEV", "PWR",
    "EME", "MTZ", "MAS", "SWK", "PNR", "IR", "HII", "L", "FLR", "AECOM",
    "SAIC", "BAH", "LDOS", "DRS", "BR",
    # Materials
    "APD", "PPG", "SHW", "IFF", "DD", "DOW", "RPM", "NUE", "STLD", "ATI",
    "VMC", "MLM", "FCX", "NEM", "BALL", "PKG", "IP", "SEE", "WRK", "CF",
    "MOS", "ALB", "FMC", "CE", "EMN", "HUN", "SON", "CCK", "OLN", "AMCR", "AVY",
    # Real Estate
    "AMT", "PLD", "EQIX", "CCI", "PSA", "WELL", "EQR", "AVB", "O", "SPG",
    "VICI", "VTR", "UDR", "ESS", "MAA", "CPT", "REG", "KIM", "BXP", "ARE",
    "WY", "HST", "SUI", "ELS", "AMH", "INVH", "REXR", "NNN", "CUBE", "EXR",
    "IRM", "SBAC", "DLR", "GLPI",
    # Utilities
    "NEE", "DUK", "SO", "D", "AEP", "EXC", "PCG", "PEG", "WEC", "ES",
    "FE", "PPL", "CMS", "NI", "ETR", "AEE", "LNT", "EVRG", "NRG", "AES",
    "CEG", "AWK", "SRE", "ED", "XEL", "EIX",
})
