FROM python:3.9-slim

WORKDIR /app

# 1. 패키지 설치에 필요한 시스템 도구 먼저 설치 (거의 안 변함)
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# 2. ★ 핵심: requirements.txt만 먼저 복사!
COPY requirements.txt .

# 3. 패키지 설치 (requirements.txt가 안 변하면 도커는 이 단계를 캐시에서 불러와 1초 만에 끝냅니다)
RUN pip install --no-cache-dir -r requirements.txt

# 4. 마지막에 소스 코드 복사 (코드가 수정되어도 위 1~3번은 다시 실행되지 않음)
COPY . .

CMD ["python", "faker_gen.py"]