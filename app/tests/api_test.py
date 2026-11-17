import requests
import json

input_json = {
    "requestType" : "voc_suggestion",
    "voc" : {
        "voc" : "금융 외 다른 기능에 대한 홍보 부족 및 미사용으로 인해 검색 기능 사용을 하고 있지 않음.",
        "qusnid" : "QSN000000000003080",
        "qsitmid" : "ITM000000000001336",
        "qusnInvlTagtpUniqID" : "TAR000000011223075"
    },
    "sq" : "059",
}
inputs = {'input_value': json.dumps(input_json)}
result = requests.post("http://localhost:8000/run_agent", json=inputs)
res = result.json()

suggestionReport = json.loads(res['content'])['suggestionReport']

print(suggestionReport)
