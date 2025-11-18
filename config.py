HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.banki.ru/products/credits/",
    "X-Requested-With": "XMLHttpRequest",
}

# mapping of columns from raw data to real columns in the database
columns_credits = {
    'productId': 'offer_id',
    'productUid': 'offer_uid',
    'productType': 'offer_type',
    # credit or smth
    'productName': 'offer_pledge',
    # equal productName
    'name': 'offer_pledge1',
    # equal meta_detailLink: url_with_details
    'url': 'offer_url',
    'smallImage': 'img_logo_url',
    'updatedAt': 'update_time',
    'partner_id': 'bank_id',
    # bank_id + 1
    'partner_uid': 'bank_uid',
    # bank (can be smth another? #TODO check)
    'partner_type': 'partner_type',
    'partner_name': 'bank_name',
    # same as smallImage, should drop column
    'partner_image': 'img_logo_url1',
    'partner_license': 'idx_license',
    'partner_url': 'bank_url',
    'partner_phone': 'bank_phone',
    'partner_address': 'bank_address',
    'partner_code': 'bank_code',
    # валюта
    'meta_currency': 'currency',
    'meta_detailLink': 'url_with_details',
    'meta_rateMin': 'rateMin',
    'meta_rateMax': 'rateMax',
    # rateMin - rateMax Р, should drop
    'meta_rateRange': 'rateRange',
    # idk what is it
    'meta_fullCreditRateMin': 'fullCreditRateMin_none',
    'meta_fullCreditRateMax': 'fullCreditRateMax_none',
    'meta_amountMin': 'amountMin',
    'meta_amountMax': 'amountMax',
    # rateMin - rateMax Р, should drop
    'meta_amountRange': 'amountRange',
    # Сроки (мин, макс)
    'meta_termMin': 'termMin',
    'meta_termMax': 'termMax',
    # сроки в месяцах/годах
    'meta_periodFromNotation': 'periodFromNotation',
    'meta_periodToNotation': 'periodToNotation',
    # idk, default = 4
    'meta_termUnit': 'termUnit',
    # block of values equal None coherent
    'meta_issuanceCostMin': 'issuanceCostMin',
    'meta_issuanceCostMax': 'issuanceCostMax',
    'meta_annualServiceMin': 'annualServiceMin',
    'meta_annualServiceMax': 'annualServiceMax',
    'meta_cashWithdrawalsAtAtms': 'cashWithdrawalsAtAtms',
    'meta_cashWithdrawalsAtOtherBankAtms': 'cashWithdrawalsAtOtherBankAtms',
    'meta_initialFeeMin': 'initialFeeMin',
    'meta_initialFeeMax': 'initialFeeMax'
}
CREDITS_MAP = {
    'productId': 'offer_id',
    'productUid': 'offer_uid',
    'productType': 'offer_type',
    'productName': 'offer_pledge',
    'name': 'offer_pledge_alt',
    'url': 'offer_url',
    'smallImage': 'img_logo_url',
    'updatedAt': 'update_time',
    'partner_id': 'bank_id',
    'partner_uid': 'bank_uid',
    'partner_type': 'bank_type',
    'partner_name': 'bank_name',
    'partner_image': 'img_logo_url_alt',
    'partner_license': 'idx_license',
    'partner_url': 'bank_url',
    'partner_phone': 'bank_phone',
    'partner_address': 'bank_address',
    'partner_code': 'bank_code',
    'meta_currency': 'currency',
    'meta_detailLink': 'url_with_details',
    'meta_rateMin': 'rateMin',
    'meta_rateMax': 'rateMax',
    'meta_rateRange': 'rateRange',
    'meta_fullCreditRateMin': 'fullCreditRateMin',
    'meta_fullCreditRateMax': 'fullCreditRateMax',
    'meta_amountMin': 'amountMin',
    'meta_amountMax': 'amountMax',
    'meta_amountRange': 'amountRange',
    'meta_termMin': 'termMin',
    'meta_termMax': 'termMax',
    'meta_periodFromNotation': 'periodFromNotation',
    'meta_periodToNotation': 'periodToNotation',
    'meta_termUnit': 'termUnit',
    'meta_issuanceCostMin': 'issuanceCostMin',
    'meta_issuanceCostMax': 'issuanceCostMax',
    'meta_annualServiceMin': 'annualServiceMin',
    'meta_annualServiceMax': 'annualServiceMax',
    'meta_cashWithdrawalsAtAtms': 'cashWithdrawalsAtAtms',
    'meta_cashWithdrawalsAtOtherBankAtms': 'cashWithdrawalsAtOtherBankAtms',
    'meta_initialFeeMin': 'initialFeeMin',
    'meta_initialFeeMax': 'initialFeeMax'
}
columns_deposits = {
    'bank_id': 'bank_id',
    'bank_name': 'bank_name',
    'bank_logo': 'img_logo_url',
    'bank_licence': 'idx_license',
    'product_name': 'offer_pledge',
    'product_url': 'offer_url',
    'rate_min': 'rateMin',
    'rate_max': 'rateMax',
    'amount_from': 'amountMin',
    'amount_to': 'amountMax',
    'period_from': 'termMin',
    'period_to': 'termMax',
    'currency': 'currency',
    'efficient_rate': 'rateEfficient',
    'is_special_offer': 'is_special_offer',
    'is_online_opening_possible': 'is_online_opening_possible',
    # досрочное частичное снятие
    'is_partial_withdrawal_possible': 'is_partial_withdrawal_possible',
    # пополнение депозита
    'is_replenishment_possible': 'is_replenishment_possible',
    # бонусы за вклады через банки ру
    'action_title': 'action_title',
    # (+1.5 %)
    'action_percent': 'action_percent',
    'action_link': 'action_link',
    'capitalization': 'capitalization',
    'early_termination_method': 'early_termination_method',
    # вероятно, это реальная инфа о мин/максах по вкладам (макс +- такой же, 
    # минимум иногда либо от 0,01 (видимо, при неполном сроке) либо от 1/2 от максимального)
    'rates_min': 'ratesMin',
    'rates_max': 'ratesMax'
    }


DEPOSITS_MAP = {
    'bank_id': 'bank_id',
    'bank_name': 'bank_name',
    'bank_logo': 'img_logo_url',
    'bank_licence': 'idx_license',
    'product_name': 'offer_pledge',
    'product_url': 'offer_url',
    'rate_min': 'rateMin',
    'rate_max': 'rateMax',
    'amount_from': 'amountMin',
    'amount_to': 'amountMax',
    'period_from': 'termMin',
    'period_to': 'termMax',
    'currency': 'currency',
    'efficient_rate': 'rateEfficient',
    'is_special_offer': 'is_special_offer',
    'is_online_opening_possible': 'is_online_opening_possible',
    'is_partial_withdrawal_possible': 'is_partial_withdrawal_possible',
    'is_replenishment_possible': 'is_replenishment_possible',
    'action_title': 'action_title',
    'action_percent': 'action_percent',
    'action_link': 'action_link',
    'capitalization': 'capitalization',
    'early_termination_method': 'early_termination_method',
    'rates_min': 'ratesMin',
    'rates_max': 'ratesMax'
}
DROP_ALWAYS = [
    "name", 
    "partner_type",
    "partner_image",
    "meta_fullCreditRateMin",
    "meta_fullCreditRateMax",
    "meta_issuanceCostMin",
    "meta_issuanceCostMax",
    "meta_annualServiceMin",
    "meta_annualServiceMax",
    "meta_cashWithdrawalsAtAtms",
    "meta_cashWithdrawalsAtOtherBankAtms",
    "meta_initialFeeMin",
    "meta_initialFeeMax",
]

REGIONS_MAP = {
    "id": "region_id",
    "name": "region_name",
    "area_name": "area_name",
    "is_city": "is_city",
    "parent_id": "parent_id",
    "kladr_code": "kladr_code",
    "region_url": "region_url",
    "is_regional_center": "is_regional_center",
}
# DB_HOST= "localhost"
# DB_PORT= 5432
# DB_NAME= "finance_db"
# DB_USER= "student"
# DB_PASSWORD= "veryhardpassword"



