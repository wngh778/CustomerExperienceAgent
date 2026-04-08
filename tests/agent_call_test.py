import requests
import json

### agent_type 테스트하고자하는 agent로 꼭 변경! agent폴더 __init__.py 참고
agent_type = "sql"
input_json = {"stream": True, "user": {"name": "3902169", "id":"3902169", "email":"3902169@gmail.com", "x-client-user":"3902169", "role":"user"}, "messages": [{"role":"user","content":"2025년 10월 대기시간 관련 voc 분석해줘"}], "agent_type": agent_type}
inputs = {'input_value': json.dumps(input_json)}
result = requests.post("http://localhost:8001/run_agent", json=inputs)

# sql agent 테스트할때는 non stream response
if agent_type == "sql":
    print(result.json())
# 그 외는 stream response로 와야함
else:
    for line in result.iter_lines(chunk_size=1, decode_unicode=True):
        text = line.decode('utf-8') if isinstance(line, bytes) else line
        if text.startswith("data: "):
            data = text[len("data:"):].strip() 
            if data == "[DONE]": 
                break
            if len(data) != 0:
                obj = json.loads(data)
                content = obj.get("content") 
                references = obj.get("references")
            print(content)