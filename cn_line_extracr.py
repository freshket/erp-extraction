from func.utils import *
n = 15
staging = 'staging'
purchase_credit_line_url = "https://api.businesscentral.dynamics.com/v2.0/f5030c5d-b648-4145-80d3-72361127bbb2/Production/ODataV4/Company('Freshket_Production')/nwthptdpurchcreditline"
purchase_credit_line = "purchase_credit_note_line"
purchase_credit_header = "purchase_credit_note_header"

tmp = f'_{n}_days'
purchase_credit_header_tmp = purchase_credit_header + tmp
purchase_credit_line_tmp = purchase_credit_line +tmp

get_api_line_data(purchase_credit_line_url,purchase_credit_header_tmp,purchase_credit_line_tmp,staging)