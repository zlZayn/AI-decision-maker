"""语义知识库 — 本地预置的"世界知识"

AI 只输出信号码（如 G），操作执行时从知识库获取具体映射规则。
"""

SEMANTIC_KNOWLEDGE: dict[str, dict] = {
    "gender": {
        "male_values": ["男", "M", "Male", "男生", "男士", "1", "帅哥", "male", "m"],
        "female_values": ["女", "F", "Female", "女生", "女士", "0", "female", "f"],
        "unknown_values": ["??", "未知", "不详", "保密", "unknown", "NA", "N/A"],
    },
    "department": {
        "mappings": {
            "心内": "心内科",
            "心内一区": "心内科",
            "心内二区": "心内科",
            "Cardiology": "心内科",
            "外科": "外科",
            "General Surgery": "外科",
            "妇科": "妇科",
            "Gynecology": "妇科",
            "骨科": "骨科",
            "Orthopedics": "骨科",
            "儿科": "儿科",
            "Pediatrics": "儿科",
            "神经内科": "神经内科",
            "Neurology": "神经内科",
            "呼吸内科": "呼吸内科",
            "Pulmonology": "呼吸内科",
            "消化内科": "消化内科",
            "Gastroenterology": "消化内科",
            "肾内科": "肾内科",
            "Nephrology": "肾内科",
            "内分泌科": "内分泌科",
            "Endocrinology": "内分泌科",
            "肿瘤科": "肿瘤科",
            "Oncology": "肿瘤科",
            "急诊科": "急诊科",
            "Emergency": "急诊科",
            "ICU": "重症医学科",
            "重症": "重症医学科",
        }
    },
    "drug_name": {
        "pattern": r"^[一-龥a-zA-Z]+",
        "dose_pattern": r"(\d+\.?\d*)\s*(mg|g|ml|片|粒|支|瓶|袋|IU|U)",
        "common_drugs": {
            "阿莫西林": "阿莫西林",
            "甲硝唑": "甲硝唑",
            "头孢": "头孢类",
            "青霉素": "青霉素",
            "布洛芬": "布洛芬",
            "对乙酰氨基酚": "对乙酰氨基酚",
            "阿司匹林": "阿司匹林",
        },
    },
    "icd10": {
        "pattern": r"^[A-Z]\d{2}(\.\d+)?$",
        "common_codes": {
            "I10": "原发性高血压",
            "E11": "2型糖尿病",
            "J06": "上呼吸道感染",
            "K29": "胃炎",
            "N18": "慢性肾病",
        },
    },
    "email": {
        "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    },
    "phone": {
        # 中国大陆手机号
        "cn_mobile_pattern": r"^1[3-9]\d{9}$",
        # 固定电话（带区号）
        "cn_landline_pattern": r"^0\d{2,3}-?\d{7,8}$",
    },
    "currency": {
        "cny_pattern": r"[¥￥]",
        "usd_pattern": r"[\$USD]",
        "eur_pattern": r"[€EUR]",
        "number_pattern": r"[\d,]+\.?\d*",
    },
    "log_level": {
        "mappings": {
            "DEBUG": "DEBUG",
            "debug": "DEBUG",
            "INFO": "INFO",
            "info": "INFO",
            "WARN": "WARN",
            "WARNING": "WARN",
            "warn": "WARN",
            "warning": "WARN",
            "ERROR": "ERROR",
            "error": "ERROR",
            "ERR": "ERROR",
            "FATAL": "FATAL",
            "fatal": "FATAL",
            "CRITICAL": "FATAL",
            "critical": "FATAL",
            "TRACE": "TRACE",
            "trace": "TRACE",
        }
    },
    "coordinates": {
        "lat_range": (-90.0, 90.0),
        "lon_range": (-180.0, 180.0),
        "dms_pattern": r"(\d+)[°]\s*(\d+)[′']\s*(\d+\.?\d*)[\"″]",
    },
}
