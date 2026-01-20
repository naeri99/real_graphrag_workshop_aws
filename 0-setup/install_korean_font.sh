#!/bin/bash

echo "한글 폰트 설치 및 matplotlib 설정을 시작합니다..."

# Noto Sans CJK 폰트 설치
echo "Noto Sans CJK 폰트 설치 중..."
if command -v yum > /dev/null; then
    sudo yum install -y google-noto-sans-cjk-ttc-fonts
elif command -v apt-get > /dev/null; then
    sudo apt-get update
    sudo apt-get install -y fonts-noto-cjk
else
    echo "패키지 관리자를 찾을 수 없습니다."
    exit 1
fi

# 폰트 캐시 갱신
echo "폰트 캐시를 갱신합니다..."
fc-cache -f -v

# 설치된 폰트 확인
echo "설치된 Noto Sans CJK 폰트:"
fc-list | grep -i "noto.*cjk"

# matplotlib 캐시 삭제
echo "matplotlib 폰트 캐시 삭제 중..."
rm -rf ~/.cache/matplotlib

# matplotlib 설정 디렉토리 찾기
echo "matplotlib 설정 파일 생성 중..."
MATPLOTLIB_DIR=$(uv run python -c "import matplotlib; print(matplotlib.get_configdir())" 2>/dev/null || python3 -c "import matplotlib; print(matplotlib.get_configdir())")

# matplotlibrc 파일 생성
mkdir -p "${MATPLOTLIB_DIR}"
cat > "${MATPLOTLIB_DIR}/matplotlibrc" << 'EOF'
font.family: sans-serif
font.sans-serif: Noto Sans CJK KR, Noto Sans CJK JP, DejaVu Sans
axes.unicode_minus: False
EOF

echo "설정 파일이 생성되었습니다: ${MATPLOTLIB_DIR}/matplotlibrc"

# 테스트 스크립트 생성
echo "테스트 스크립트 생성 중..."
cat > test_korean_font.py << 'EOF'
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.font_manager as fm

# Noto Sans CJK 폰트 직접 지정
font_path = '/usr/share/fonts/google-noto-cjk/NotoSansCJK-Regular.ttc'
fm.fontManager.addfont(font_path)
plt.rcParams['font.family'] = fm.FontProperties(fname=font_path).get_name()
plt.rcParams['axes.unicode_minus'] = False

# 데이터 생성
x = np.linspace(0, 10, 100)
y = np.sin(x)

# 그래프 생성
plt.figure(figsize=(10, 6))
plt.plot(x, y, label='사인 함수')
plt.plot(x, -y, label='-사인 함수')
plt.title('한글 테스트: 사인 함수 그래프')
plt.xlabel('x축 라벨')
plt.ylabel('y축 라벨')
plt.legend()
plt.grid(True)
plt.savefig('korean_font_test.png', dpi=150)

print("테스트 완료! korean_font_test.png 파일을 확인하세요.")
EOF

echo "테스트 스크립트가 생성되었습니다: test_korean_font.py"
echo "다음 명령어로 테스트할 수 있습니다: uv run python test_korean_font.py"
echo "설정이 완료되었습니다!"