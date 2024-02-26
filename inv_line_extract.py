from func.utils import *

days = [3,7,15,30]
invoice_line_url = "https://api.businesscentral.dynamics.com/v2.0/f5030c5d-b648-4145-80d3-72361127bbb2/Production/ODataV4/Company('Freshket_Production')/nwthptdpurchinvoiceline"
invoice_header = "purchase_invoice_header"
invoice_line = "purchase_invoice_line"
staging = 'staging'

for n in days:
    tmp = f'_{n}_days'
    invoice_header_tmp = invoice_header + tmp
    invoice_line_tmp = invoice_line + tmp
    get_api_line_data(invoice_line_url,invoice_header_tmp,invoice_line_tmp,staging)