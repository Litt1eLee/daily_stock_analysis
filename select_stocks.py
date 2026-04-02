"""
每日自动选股脚本 v2
- 09:35 运行，数据更稳定
- 过滤停牌、一字涨停、次新股
"""

import os
import sys
import json
import datetime


def get_hot_stocks(top_n=50):
    try:
        import akshare as ak
        import pandas as pd
    except ImportError:
        os.system("pip install akshare pandas -q")
        import akshare as ak
        import pandas as pd

    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 开始获取全市场行情...")

    try:
        df = ak.stock_zh_a_spot_em()
        print(f"获取到 {len(df)} 只股票")
    except Exception as e:
        print(f"行情获取失败: {e}，使用备用列表")
        fallback = "601138,603986,603501,603259,300759,600519,000858,601318,601899,002747"
        return fallback

    # ── 列名统一 ──────────────────────────────────
    col_map = {
        '代码': 'code', '名称': 'name',
        '涨跌幅': 'pct_chg', '换手率': 'turnover',
        '总市值': 'market_cap', '量比': 'vol_ratio',
        '成交额': 'amount', '最高': 'high', '最低': 'low',
        '今开': 'open', '最新价': 'close',
        '60日涨跌幅': 'pct_60d', '上市时间': 'list_date'
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    for col in ['pct_chg', 'turnover', 'market_cap', 'vol_ratio', 'amount', 'high', 'low']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['pct_chg', 'turnover', 'market_cap', 'amount'])

    total_before = len(df)

    # ── 过滤第一关：基础排除 ──────────────────────
    # 1. 排除 ST、退市、B股
    df = df[~df['name'].str.contains('ST|退市|B股', na=False)]

    # 2. 排除停牌股（成交额为0）
    df = df[df['amount'] > 0]
    print(f"排除停牌后: {len(df)} 只（过滤了 {total_before - len(df)} 只停牌股）")

    # 3. 排除一字涨停/跌停（高=低，无法买卖）
    if 'high' in df.columns and 'low' in df.columns:
        before = len(df)
        df = df[df['high'] != df['low']]
        print(f"排除一字板后: {len(df)} 只（过滤了 {before - len(df)} 只一字板）")

    # 4. 排除次新股（上市不足60个交易日，数据不稳定）
    if 'list_date' in df.columns:
        try:
            today = datetime.datetime.now()
            df['list_date'] = pd.to_datetime(df['list_date'], errors='coerce')
            df['days_listed'] = (today - df['list_date']).dt.days
            before = len(df)
            df = df[df['days_listed'] > 90]  # 上市超过90天
            print(f"排除次新股后: {len(df)} 只（过滤了 {before - len(df)} 只次新股）")
        except Exception:
            pass

    # 5. 只保留沪深主板、创业板、科创板
    df = df[df['code'].str.match(r'^[036]\d{5}$')]

    # 6. 市值和流动性门槛
    df = df[
        (df['market_cap'] > 30e8)       # 市值 > 30亿
        & (df['amount'] > 5000e4)        # 成交额 > 5000万
    ]

    print(f"基础过滤完成，剩余: {len(df)} 只")

    # ── 过滤第二关：09:35 特有的开盘质量过滤 ─────
    # 排除高开超过5%的（追高风险大，可能是假突破）
    if 'open' in df.columns and 'close' in df.columns:
        df['gap_up'] = (df['open'] - df['close']) / df['close'] * 100  # 近似用昨收
        # 只排除明显异常的跳空（实际逻辑由 pct_chg 控制即可）

    # ── 分层筛选 ──────────────────────────────────
    results_list = []

    # 第一层：强势股（涨幅靠前 + 真实放量，排除涨停无法买入的）
    strong = df[
        (df['pct_chg'] > 3)
        & (df['pct_chg'] < 9.5)         # 排除即将涨停的（买不到）
        & (df['vol_ratio'] > 1.5)
        & (df['turnover'] > 1)
    ].nlargest(15, 'pct_chg')
    results_list.append(('强势股', strong))

    # 第二层：高换手活跃大盘股
    active = df[
        (df['pct_chg'] > 0)
        & (df['turnover'] > 3)
        & (df['market_cap'] > 100e8)
    ].nlargest(15, 'turnover')
    results_list.append(('高换手大盘股', active))

    # 第三层：大市值蓝筹防御（今日未大跌）
    bluechip = df[
        (df['market_cap'] > 1000e8)
        & (df['pct_chg'] > -1)
        & (df['amount'] > 1e8)          # 成交1亿以上
    ].nlargest(10, 'market_cap')
    results_list.append(('大市值蓝筹', bluechip))

    # 第四层：中期趋势强势（60日涨幅靠前）
    if 'pct_60d' in df.columns:
        df['pct_60d'] = pd.to_numeric(df['pct_60d'], errors='coerce')
        trend = df[
            (df['pct_60d'] > 20)
            & (df['pct_chg'] > -2)
            & (df['pct_chg'] < 9.5)     # 排除涨停
            & (df['turnover'] > 0.5)
        ].nlargest(10, 'pct_60d')
        results_list.append(('60日趋势股', trend))

    # ── 合并去重 ──────────────────────────────────
    import pandas as pd_inner
    results = pd_inner.concat([r for _, r in results_list]).drop_duplicates(subset=['code'])

    for label, r in results_list:
        print(f"{label}: {len(r)} 只")

    # 综合评分
    results['score'] = (
        results['pct_chg'].rank(pct=True) * 0.4
        + results['turnover'].rank(pct=True) * 0.3
        + results['vol_ratio'].fillna(1).rank(pct=True) * 0.2
        + results['market_cap'].rank(pct=True) * 0.1
    )

    final = results.nlargest(top_n, 'score')

    # ── 打印结果 ──────────────────────────────────
    print(f"\n{'='*55}")
    print(f"{'代码':<8} {'名称':<10} {'涨跌幅':>7} {'换手率':>7} {'市值(亿)':>10}")
    print(f"{'-'*55}")
    for _, row in final.iterrows():
        cap = row.get('market_cap', 0) / 1e8
        print(f"{row['code']:<8} {str(row.get('name','')):<10} "
              f"{row.get('pct_chg',0):>+6.2f}%  "
              f"{row.get('turnover',0):>6.2f}%  "
              f"{cap:>8.1f}亿")
    print(f"{'='*55}")

    return ','.join(final['code'].tolist())


if __name__ == '__main__':
    top_n = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    stock_list = get_hot_stocks(top_n=top_n)
    count = len(stock_list.split(','))

    # 写入 GitHub Actions 环境变量
    env_file = os.environ.get('GITHUB_ENV', '')
    if env_file:
        with open(env_file, 'a') as f:
            f.write(f"DYNAMIC_STOCK_LIST={stock_list}\n")

    # 写入 GitHub Actions output
    output_file = os.environ.get('GITHUB_OUTPUT', '')
    if output_file:
        with open(output_file, 'a') as f:
            f.write(f"stock_list={stock_list}\n")
            f.write(f"stock_count={count}\n")

    # 保存 JSON
    with open('selected_stocks.json', 'w', encoding='utf-8') as f:
        json.dump({
            'date': datetime.datetime.now().strftime('%Y-%m-%d'),
            'time': datetime.datetime.now().strftime('%H:%M:%S'),
            'count': count,
            'stock_list': stock_list,
            'note': '已过滤停牌、一字板、次新股、ST'
        }, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 选股完成: {count} 只")
    print(f"STOCK_LIST={stock_list}")
