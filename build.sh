#!/bin/bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

IMAGE_NAME="gdrive-agent"

setup_buildx() {
  echo -e "${YELLOW}Setting up Docker buildx with host network...${NC}"
  if docker buildx inspect gdrive-agent-builder &>/dev/null; then
    docker buildx rm gdrive-agent-builder --force >/dev/null 2>&1
  fi
  docker buildx create --name gdrive-agent-builder --driver docker-container --driver-opt network=host --use
  docker buildx inspect --bootstrap
  echo -e "${GREEN}Buildx setup completed${NC}"
}

cleanup_buildx() {
  echo -e "${YELLOW}Cleaning up Docker buildx...${NC}"
  docker buildx prune -f >/dev/null 2>&1 || true
  if docker buildx inspect gdrive-agent-builder &>/dev/null; then
    docker buildx rm gdrive-agent-builder --force >/dev/null 2>&1 || true
  fi
  echo -e "${GREEN}Buildx cleanup completed${NC}"
}
trap cleanup_buildx EXIT

print_usage() {
  echo -e "${CYAN}Google Drive Agent Build Script${NC}"
  echo
  echo -e "${YELLOW}Usage:${NC} $0 --registry REGISTRY [--multi-arch]"
  echo
  echo -e "${YELLOW}Options:${NC}"
  echo -e "  ${CYAN}--registry REGISTRY${NC}  Docker registry to push to (required)"
  echo -e "  ${CYAN}--multi-arch${NC}         Build for amd64 + arm64"
  echo -e "  ${CYAN}-h, --help${NC}           Show this help"
}

get_native_arch() {
  case $(uname -m) in
    x86_64)  echo "linux/amd64" ;;
    aarch64) echo "linux/arm64" ;;
    *)       echo "linux/amd64" ;;
  esac
}

REGISTRY=""
USE_MULTI_ARCH=false
NATIVE_ARCH=$(get_native_arch)

while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help) print_usage; exit 0 ;;
    --registry) REGISTRY="$2"; shift 2 ;;
    --multi-arch) USE_MULTI_ARCH=true; shift ;;
    *) echo -e "${RED}Unknown option: $1${NC}"; print_usage; exit 1 ;;
  esac
done

if [ -z "$REGISTRY" ]; then
  echo -e "${RED}ERROR: --registry is required${NC}"
  print_usage
  exit 1
fi

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${CYAN}=======================================${NC}"
echo -e "${CYAN}  Google Drive Agent Build                 ${NC}"
echo -e "${CYAN}=======================================${NC}"
echo -e "  Registry:    ${REGISTRY}"
echo -e "  Multi-arch:  ${USE_MULTI_ARCH}"
echo -e "  Timestamp:   ${TIMESTAMP}"
echo

setup_buildx

if [ "$USE_MULTI_ARCH" = true ]; then
  PLATFORMS="linux/amd64,linux/arm64"
else
  PLATFORMS="${NATIVE_ARCH}"
fi

echo -e "${GREEN}===== Building ${IMAGE_NAME} =====${NC}"

if docker buildx build \
  --platform "${PLATFORMS}" \
  -t "${REGISTRY}/${IMAGE_NAME}:latest" \
  -t "${REGISTRY}/${IMAGE_NAME}:${TIMESTAMP}" \
  --cache-from "type=registry,ref=${REGISTRY}/${IMAGE_NAME}:buildcache" \
  --cache-to "type=registry,ref=${REGISTRY}/${IMAGE_NAME}:buildcache,mode=max" \
  --push \
  "${SCRIPT_DIR}"; then
  echo -e "${GREEN}Build completed${NC}" >&2
  echo -e "  ${GREEN}Image:${NC} ${REGISTRY}/${IMAGE_NAME}:latest" >&2
  echo -e "  ${GREEN}Image:${NC} ${REGISTRY}/${IMAGE_NAME}:${TIMESTAMP}" >&2
  echo "{\"success\":true,\"version_tag\":\"${TIMESTAMP}\",\"registry\":\"${REGISTRY}\",\"images\":[{\"name\":\"${IMAGE_NAME}\",\"tag\":\"${TIMESTAMP}\"}]}"
else
  echo -e "${RED}Build failed${NC}" >&2
  echo '{"success":false,"images":[],"failed":["'"${IMAGE_NAME}"'"]}'
  exit 1
fi
