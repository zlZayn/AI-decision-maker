"""操作模块测试"""

import pandas as pd
import pytest

from signalchain.operations.pass_through import PassThrough
from signalchain.operations.gender import GenderNormalizer
from signalchain.operations.age import AgeExtractor
from signalchain.operations.department import DepartmentNormalizer
from signalchain.operations.drug_name import DrugNameNormalizer
from signalchain.operations.icd10 import ICD10Validator
from signalchain.operations.datetime_parser import DateTimeParser
from signalchain.operations.currency import CurrencyNormalizer
from signalchain.operations.email import EmailValidator
from signalchain.operations.phone import PhoneValidator
from signalchain.operations.log_level import LogLevelNormalizer
from signalchain.operations.coordinates import CoordinatesValidator


class TestPassThrough:
    def test_name(self):
        op = PassThrough()
        assert op.name == "pass_through"

    def test_execute(self):
        op = PassThrough()
        s = pd.Series(["a", "b", "c"])
        result = op.execute(s)
        assert list(result) == ["a", "b", "c"]


class TestGenderNormalizer:
    def setup_method(self):
        self.op = GenderNormalizer()

    def test_name(self):
        assert self.op.name == "normalize_gender"

    def test_english(self):
        result = self.op.execute(pd.Series(["M", "F", "Male", "Female"]))
        assert list(result) == ["男", "女", "男", "女"]

    def test_chinese(self):
        result = self.op.execute(pd.Series(["男", "女"]))
        assert list(result) == ["男", "女"]

    def test_slang(self):
        result = self.op.execute(pd.Series(["帅哥"]))
        assert list(result) == ["男"]

    def test_numeric(self):
        result = self.op.execute(pd.Series(["1", "0"]))
        assert list(result) == ["男", "女"]

    def test_unknown(self):
        result = self.op.execute(pd.Series(["??", "未知"]))
        assert list(result) == [None, None]

    def test_nan(self):
        result = self.op.execute(pd.Series([None, "M"]))
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == "男"


class TestAgeExtractor:
    def setup_method(self):
        self.op = AgeExtractor()

    def test_name(self):
        assert self.op.name == "extract_age"

    def test_plain_number(self):
        result = self.op.execute(pd.Series(["30", "25"]))
        assert list(result) == [30, 25]

    def test_with_suffix(self):
        result = self.op.execute(pd.Series(["30岁", "25Y"]))
        assert list(result) == [30, 25]

    def test_approximate(self):
        result = self.op.execute(pd.Series(["约30"]))
        assert list(result) == [30]

    def test_nan(self):
        result = self.op.execute(pd.Series([None]))
        assert result.iloc[0] is None

    def test_out_of_range(self):
        result = self.op.execute(pd.Series(["999"]))
        assert result.iloc[0] is None


class TestDepartmentNormalizer:
    def setup_method(self):
        self.op = DepartmentNormalizer()

    def test_name(self):
        assert self.op.name == "normalize_department"

    def test_abbreviation(self):
        result = self.op.execute(pd.Series(["心内"]))
        assert list(result) == ["心内科"]

    def test_english(self):
        result = self.op.execute(pd.Series(["Cardiology"]))
        assert list(result) == ["心内科"]

    def test_unknown(self):
        result = self.op.execute(pd.Series(["未知科室"]))
        assert list(result) == ["未知科室"]

    def test_icu(self):
        result = self.op.execute(pd.Series(["ICU"]))
        assert list(result) == ["重症医学科"]


class TestDrugNameNormalizer:
    def setup_method(self):
        self.op = DrugNameNormalizer()

    def test_name(self):
        assert self.op.name == "normalize_drug_name"

    def test_with_dosage(self):
        result = self.op.execute(pd.Series(["阿莫西林0.25g"]))
        assert result.iloc[0] == "阿莫西林"

    def test_plain_name(self):
        result = self.op.execute(pd.Series(["甲硝唑"]))
        assert result.iloc[0] == "甲硝唑"

    def test_unknown(self):
        result = self.op.execute(pd.Series(["未知药品"]))
        assert result.iloc[0] == "未知药品"


class TestICD10Validator:
    def setup_method(self):
        self.op = ICD10Validator()

    def test_valid(self):
        result = self.op.execute(pd.Series(["I10", "E11.9"]))
        assert result.iloc[0] == "I10"
        assert result.iloc[1] == "E11.9"

    def test_invalid(self):
        result = self.op.execute(pd.Series(["abc", "123"]))
        assert result.iloc[0] is None
        assert result.iloc[1] is None


class TestDateTimeParser:
    def setup_method(self):
        self.op = DateTimeParser()

    def test_iso_format(self):
        result = self.op.execute(pd.Series(["2024-01-15"]))
        assert "2024-01-15" in str(result.iloc[0])

    def test_chinese_format(self):
        result = self.op.execute(pd.Series(["2024年1月15日"]))
        assert "2024-01-15" in str(result.iloc[0])

    def test_unparseable(self):
        result = self.op.execute(pd.Series(["not a date"]))
        assert result.iloc[0] == "not a date"


class TestCurrencyNormalizer:
    def setup_method(self):
        self.op = CurrencyNormalizer()

    def test_cny(self):
        result = self.op.execute(pd.Series(["¥100.50", "￥200"]))
        assert result.iloc[0] == 100.5
        assert result.iloc[1] == 200.0

    def test_usd(self):
        result = self.op.execute(pd.Series(["$50", "USD 100"]))
        assert result.iloc[0] == 50.0

    def test_plain_number(self):
        result = self.op.execute(pd.Series(["100"]))
        assert result.iloc[0] == 100.0

    def test_with_comma(self):
        result = self.op.execute(pd.Series(["1,000.50"]))
        assert result.iloc[0] == 1000.5


class TestEmailValidator:
    def setup_method(self):
        self.op = EmailValidator()

    def test_valid(self):
        result = self.op.execute(pd.Series(["test@example.com"]))
        assert result.iloc[0] == "test@example.com"

    def test_invalid(self):
        result = self.op.execute(pd.Series(["not-email", "@missing"]))
        assert result.iloc[0] is None
        assert result.iloc[1] is None


class TestPhoneValidator:
    def setup_method(self):
        self.op = PhoneValidator()

    def test_valid_mobile(self):
        result = self.op.execute(pd.Series(["13800138000", "13912345678"]))
        assert result.iloc[0] == "13800138000"
        assert result.iloc[1] == "13912345678"

    def test_invalid(self):
        result = self.op.execute(pd.Series(["12345", "abc"]))
        assert result.iloc[0] is None


class TestLogLevelNormalizer:
    def setup_method(self):
        self.op = LogLevelNormalizer()

    def test_various_levels(self):
        result = self.op.execute(pd.Series(["DEBUG", "info", "WARNING", "error"]))
        assert list(result) == ["DEBUG", "INFO", "WARN", "ERROR"]

    def test_critical(self):
        result = self.op.execute(pd.Series(["CRITICAL"]))
        assert result.iloc[0] == "FATAL"


class TestCoordinatesValidator:
    def setup_method(self):
        self.op = CoordinatesValidator()

    def test_valid_latitude(self):
        result = self.op.execute(pd.Series(["39.9042"]))
        assert result.iloc[0] == 39.9042

    def test_valid_longitude(self):
        result = self.op.execute(pd.Series(["116.4074"]))
        assert result.iloc[0] == 116.4074

    def test_out_of_range(self):
        result = self.op.execute(pd.Series(["200"]))
        assert result.iloc[0] is None

    def test_dms_format(self):
        result = self.op.execute(pd.Series(["39°54'15\""]))
        assert result.iloc[0] is not None
        assert abs(result.iloc[0] - 39.904167) < 0.01
