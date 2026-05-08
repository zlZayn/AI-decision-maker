"""操作注册表 — 操作名 → Operation 实例"""

from __future__ import annotations

from signalchain.operations.base import Operation
from signalchain.operations.pass_through import PassThrough
from signalchain.operations.gender import GenderNormalizer
from signalchain.operations.age import AgeExtractor
from signalchain.operations.department import DepartmentNormalizer
from signalchain.operations.drug_name import DrugNameNormalizer
from signalchain.operations.icd10 import ICD10Validator
from signalchain.operations.datetime_parser import DateTimeParser
from signalchain.operations.currency import CurrencySplitter
from signalchain.operations.email import EmailValidator
from signalchain.operations.phone import PhoneValidator
from signalchain.operations.log_level import LogLevelNormalizer
from signalchain.operations.coordinates import CoordinatesValidator

OPERATION_REGISTRY: dict[str, Operation] = {
    op.name: op
    for op in [
        PassThrough(),
        GenderNormalizer(),
        AgeExtractor(),
        DepartmentNormalizer(),
        DrugNameNormalizer(),
        ICD10Validator(),
        DateTimeParser(),
        CurrencySplitter(),
        EmailValidator(),
        PhoneValidator(),
        LogLevelNormalizer(),
        CoordinatesValidator(),
    ]
}
