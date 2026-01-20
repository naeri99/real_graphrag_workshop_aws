#!/bin/bash

# UV 환경 설정 및 Jupyter 커널 등록 스크립트 (pyproject.toml 기반)
# 사용법: ./create_uv_env.sh <환경이름> [python버전]

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 함수: 출력 메시지
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 사용법 출력
usage() {
    echo "사용법: $0 <환경이름> [python버전]"
    echo ""
    echo "예시:"
    echo "  $0 myproject"
    echo "  $0 myproject 3.12"
    echo ""
    echo "옵션:"
    echo "  환경이름     : 생성할 환경의 이름 (필수)"
    echo "  python버전   : 사용할 Python 버전 (선택, 기본값: 3.12)"
    exit 1
}

# 인수 검증
if [ $# -lt 1 ]; then
    print_error "환경 이름이 필요합니다."
    usage
fi

ENV_NAME=$1
PYTHON_VERSION=${2:-3.11}

print_info "환경 설정을 시작합니다..."
print_info "환경 이름: $ENV_NAME"
print_info "Python 버전: $PYTHON_VERSION"

# /home/ec2-user/workshop으로 강제 이동
WORKSHOP_DIR="/home/ec2-user/workshop"
print_info "작업 디렉토리를 $WORKSHOP_DIR로 이동합니다..."
cd "$WORKSHOP_DIR" || {
    print_error "$WORKSHOP_DIR 디렉토리를 찾을 수 없습니다."
    exit 1
}

# 기존 가상환경 정리
if [ -d ".venv" ]; then
    print_warning "기존 가상환경을 제거합니다: .venv"
    rm -rf .venv
    print_success "기존 가상환경이 제거되었습니다."
fi

install_uv() {
    print_info "UV를 설치합니다..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
    
    if [ -f "$HOME/.local/bin/env" ]; then
        source "$HOME/.local/bin/env"
    fi
    
    if command -v uv &> /dev/null; then
        print_success "UV가 성공적으로 설치되었습니다!"
        uv --version
    else
        print_error "UV 설치에 실패했습니다."
        print_info "수동 설치: curl -LsSf https://astral.sh/uv/install.sh | sh"
        return 1 2>/dev/null || exit 1
    fi
}

if ! command -v uv &> /dev/null; then
    print_warning "UV가 설치되어 있지 않습니다."
    read -p "UV를 자동으로 설치하시겠습니까? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        install_uv || exit 1
    else
        print_error "UV가 필요합니다."
        print_info "설치 방법: curl -LsSf https://astral.sh/uv/install.sh | sh"
        return 1 2>/dev/null || exit 1
    fi
fi



# 1. Python 버전 설정
print_info "Python $PYTHON_VERSION 설정 중..."
uv python pin $PYTHON_VERSION
print_success "Python $PYTHON_VERSION이 설정되었습니다."

# 2. 프로젝트 초기화
print_info "프로젝트 초기화 중..."
if [ ! -f "pyproject.toml" ]; then
    uv init --name "$ENV_NAME"
    print_success "프로젝트가 '$ENV_NAME'으로 초기화되었습니다."
else
    print_warning "이미 pyproject.toml이 존재합니다. 기존 프로젝트를 사용합니다."
fi

# 3. 필수 패키지 추가
print_info "필수 패키지 추가 중..."
uv add ipykernel jupyter jupyterlab boto3

# 4. 의존성 동기화
print_info "의존성 동기화 중..."
uv sync
print_success "의존성이 설치되었습니다."

# 5. 한글 폰트 설치 (선택적)
if [ -f "0-setup/install_korean_font.sh" ]; then
    print_info "한글 폰트 설치 중..."
    bash 0-setup/install_korean_font.sh
fi

# 6. 시스템 패키지 설치 (선택적)
print_info "시스템 패키지 설치 중..."
sudo yum update -y
sudo yum install -y wget poppler-utils

# Pandoc 설치
if ! command -v pandoc &> /dev/null; then
    print_info "Pandoc 설치 중..."
    cd /tmp
    wget -q https://github.com/jgm/pandoc/releases/download/3.1.11/pandoc-3.1.11-linux-amd64.tar.gz
    sudo tar xvzf pandoc-3.1.11-linux-amd64.tar.gz --strip-components 1 -C /usr/local/
    cd "$WORKSHOP_DIR"
fi

# 7. Jupyter 커널 등록
print_info "Jupyter 커널 등록 중..."
DISPLAY_NAME="$ENV_NAME (UV)"

if jupyter kernelspec list 2>/dev/null | grep -q "$ENV_NAME"; then
    print_warning "기존 '$ENV_NAME' 커널을 제거합니다..."
    jupyter kernelspec remove -f "$ENV_NAME" 2>/dev/null || true
fi

uv run python -m ipykernel install --user --name "$ENV_NAME" --display-name "$DISPLAY_NAME" || {
    print_error "Jupyter 커널 등록에 실패했습니다."
    exit 1
}
print_success "Jupyter 커널이 '$DISPLAY_NAME'로 등록되었습니다."

# 8. 설치 확인
print_info "설치 확인 중..."
echo ""
echo "=== 설치된 Python 버전 ==="
uv run python --version

echo ""
echo "=== 등록된 Jupyter 커널 ==="
uv run jupyter kernelspec list 2>/dev/null | grep -E "(Available|$ENV_NAME)" || echo "커널 목록을 가져올 수 없습니다."

echo ""
print_success "환경 설정이 완료되었습니다!"
echo ""
echo "=== 사용 방법 ==="
echo "1. 패키지 추가: cd $WORKSHOP_DIR && uv add <패키지명>"
echo "2. 패키지 제거: cd $WORKSHOP_DIR && uv remove <패키지명>"
echo "3. 의존성 동기화: cd $WORKSHOP_DIR && uv sync"
echo "4. 스크립트 실행: cd $WORKSHOP_DIR && uv run python main.py"
echo "5. Jupyter Lab 실행: cd $WORKSHOP_DIR && uv run jupyter lab"
echo "6. 새 노트북 생성 시 '$DISPLAY_NAME' 커널 선택"
echo ""
echo "=== 파일 정보 ==="
echo "- pyproject.toml: 프로젝트 설정 및 의존성 ($WORKSHOP_DIR)"
echo "- uv.lock: 정확한 버전 락 파일 (버전 관리에 포함 권장)"
echo "- .venv/: 가상 환경 디렉토리 (버전 관리에서 제외)"
echo ""
print_info "모든 uv 명령은 $WORKSHOP_DIR 에서 실행하세요!"
print_info "전통적인 방식: cd $WORKSHOP_DIR && source .venv/bin/activate"
print_info "UV 권장 방식: cd $WORKSHOP_DIR && uv run <명령어>"