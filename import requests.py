import requests

url = "https://www9.bcb.gov.br/protegehom/ws/sta/TransferenciaArquivos.asmx?wsdl"

resp = requests.get(url, timeout=15)

print("Status:", resp.status_code)
print(resp.text[:500])