# pip install yfinance pandas
import yfinance as yf, pandas as pd

xtb = {  # XTB -> Yahoo
 "4GLD.DE":"4GLD.DE","AAPL.US":"AAPL","ARCC.US":"ARCC","COST.US":"COST","EGAN.US":"EGAN",
 "ENEL.IT":"ENEL.MI","EUNA.DE":"EUNA.DE","IDVY.NL":"IDVY.AS","IS3N.DE":"IS3N.DE","JNJ.US":"JNJ",
 "KO.US":"KO","LDOS.US":"LDOS","LYP6.DE":"LYP6.DE","MAIN.US":"MAIN","MNST.US":"MNST",
 "MSFT.US":"MSFT","NVDA.US":"NVDA","O.US":"O","OXY.US":"OXY","PFE.US":"PFE","PLTR.US":"PLTR",
 "SXRV.DE":"SXRV.DE","TTD.US":"TTD","TTE.FR":"TTE.PA","VHYD.UK":"VHYD.L","VWCE.DE":"VWCE.DE","WDP.BE":"WDP.BR"}

br = ["ABEV3","AURE3","BBAS3","BBSE3","BHIA3","BIDI4","BRFS3","COGN3","CPLE6","CVCB3","ENBR3",
 "EVEN3","FLRY3","ITSA4","PFRM3","SAPR11","SOJA3","SQIA3","STBP3","UNIP6","VALE3","ALUP11",
 "ALZR11","BTLG11","HGLG11","HGRU11","IRIM11","KNRI11","KNUQ11","MCCI11","OUJP11","RBRP11",
 "RECR11","VILG11","VRTA11","XPLG11","XPML11"]

tickers = list(xtb.values()) + [t + ".SA" for t in br]
rows = []
for t in tickers:
    s = yf.Ticker(t).splits
    if s.empty:
        rows.append((t, None, None))
    for d, r in s.items():
        rows.append((t, d.date(), r))
df = pd.DataFrame(rows, columns=["yahoo_ticker", "split_date", "ratio"])
print(df.to_string())
df.to_csv("C:\\Users\\bruno\\Downloads\\yahoo_splits.csv", index=False)