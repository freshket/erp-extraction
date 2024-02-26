from func.utils import *
days = [3,7,15,30]

invoice_header_url = "https://api.businesscentral.dynamics.com/v2.0/f5030c5d-b648-4145-80d3-72361127bbb2/Production/ODataV4/Company('Freshket_Production')/nwthptdpurchinvoiceheader"
invoice_header = "purchase_invoice_header"
staging = 'staging'
for n in days:
    tmp = f'_{n}_days'
    invoice_header_tmp = invoice_header + tmp
    get_api_header_data_n_days_ago(invoice_header_url,invoice_header_tmp,'Posting_Date',n,staging,'inv')