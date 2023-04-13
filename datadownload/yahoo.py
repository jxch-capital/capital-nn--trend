import yfinance as yf
import pandas as pd

excel_path = '../res/raw/'
intervals_d = ['1d', '5d', '1wk', '1mo', '3mo']
intervals_m = ['5m', '15m', '30m', '60m', '90m']

codes = {
    'us-it': 'QQQ,GOOGL,AAPL,MSFT,TSLA,AMZN,AMD,TSM,NVDA,META,INTC,IBM,ORCL,HPQ',
    'us-industry': 'SPY,XLI,XLE,XLY,XLP,XLF,XLV,XLC,XLB,XLRE,XLK,XLU',
    'us-industry-detailed': 'XHB,XRT,XLP,XOP,OIH,TAN,URA,KBE,KIE,IAI,IBB,IHI,IHF,XPH,ITA,IYT,JETS,GDX,XME,LIT,REMX,IYM,'
                            'VNQ,VNQI,REM,VGT,FDN,SOCL,IGV,SOXX',
    'us-market': 'VTI,DIA,OEF,MDY,IWB,IWM,QTEC,IVW,IVE,IWF,IWD',
    'us-theme': 'MOAT,FFTY,IBUY,CIBR,SKYY,IPAY,FINX,XT,ARKK,BOTZ,MOO,ARKG,MJ,ARKW,ARKQ,PBW,BLOK,SNSR',
    'us-blue': 'BRK-B,BA,CHKP,MRK,JPM,MS,T,JNPR,MCD,ORCL,DUK,GE,KO,XOM,AEP,PEP,JNJ,ADBE,WFC,ERIC,C,CPB,BAC,'
               'HPQ,PFE,CSCO,F,XRX,GM,DAL,DIS,AMAT,UNH,WMT,PG,MA,NKE,FDX,MMM',
    'us-futures': 'ES=F,NQ=F,YM=F,RTY=F,GC=F,HG=F,CL=F,ZC=F,ZW=F,KC=F,SB=F,CC=F,BTC=F,LE=F,CT=F,ZS=F,PA=F,SI=F,PL=F',
    'us-china': 'BABA,BIDU,JD,PDD,NTES,WB,BILI,TCOM',
    'china-industry': '000001.SS,000032.SZ,000034.SZ,000035.SZ,000036.SZ,000037.SZ,000038.SZ,000039.SZ,000040.SZ,'
                      '000042.SZ',
    'china-blue': '600519.SS,600600.SS,601318.SS,600887.SS,603288.SS,000538.SZ,000333.SZ,002594.SZ,601607.SS,000002.SZ',
}


def download():
    for key, code_str in codes.items():
        filepath = f'{excel_path}{key}-d.xlsx'
        with pd.ExcelWriter(filepath) as writer:
            for interval in intervals_d:
                df = yf.download(code_str, interval=interval, period='max')
                df.to_excel(excel_writer=writer, sheet_name=f'{interval}')
                print(f'end: {filepath}-{interval}')
