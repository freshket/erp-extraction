from func.utils import *
n = 15

purchase_credit_header_url = "https://api.businesscentral.dynamics.com/v2.0/f5030c5d-b648-4145-80d3-72361127bbb2/Production/ODataV4/Company('Freshket_Production')/nwthptdpurchcreditheader"
purchase_credit_header = "purchase_credit_note_header"
staging = 'staging'
tmp = f'_{n}_days'
purchase_credit_header_tmp = purchase_credit_header + tmp
get_api_header_data_n_days_ago(purchase_credit_header_url,purchase_credit_header_tmp,'Posting_Date',n,staging,'cn')