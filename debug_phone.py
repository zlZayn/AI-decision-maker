"""Debug script to trace phone number float issue - full pipeline"""
import pandas as pd
import re

# 读取数据
dirty = pd.read_csv('data/dirty/user.csv')
phone_col = dirty['PHONE'].copy()
print('=== 原始 Phone 列 ===')
print(f'  dtype: {phone_col.dtype}')
print(f'  values: {phone_col.tolist()[:5]}')

# 全局符号清理
MEANINGLESS_SYMBOLS = re.compile(
    r'[!?！？~～#＊^&*()（）【】〔〕［］｛｝<>"\'`;；=|\\]'
)

def _clean_text(val):
    if pd.isna(val):
        return None
    s = str(val)
    s = MEANINGLESS_SYMBOLS.sub('', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s if s else None

# 全局清理后
for idx in phone_col.index:
    phone_col[idx] = _clean_text(phone_col[idx])

print()
print('=== 全局符号清理后 ===')
print(f'  dtype: {phone_col.dtype}')
print(f'  values: {phone_col.tolist()[:5]}')

# PhoneValidator 处理
from signalchain.operations.phone import PhoneValidator
pv = PhoneValidator()
result = pv.execute(phone_col)

print()
print('=== PhoneValidator 处理后 ===')
print(f'  dtype: {result.dtype}')
print(f'  values: {result.tolist()[:5]}')

# 赋值回 DataFrame
df = dirty.copy()
for idx in df.index:
    df.at[idx, 'PHONE'] = result[idx]

print()
print('=== 赋值回 DataFrame 后 ===')
print(f'  dtype: {df["PHONE"].dtype}')
print(f'  values: {df["PHONE"].tolist()[:5]}')

# 保存并读取
df.to_csv('debug_output.csv', index=False)
df2 = pd.read_csv('debug_output.csv')
print()
print('=== 保存后再读取 ===')
print(f'  dtype: {df2["PHONE"].dtype}')
print(f'  values: {df2["PHONE"].tolist()[:5]}')
