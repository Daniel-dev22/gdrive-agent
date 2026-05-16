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

# setup_ssh_agent ensures SSH_AUTH_SOCK points at an agent with the
# id_rsa_github key loaded, so `docker buildx build --ssh default` can
# fetch the private agent-kit-go module from GitHub. If no agent is
# already forwarded we spin up a transient one for this build and add
# the discovered key. Killed on exit by cleanup_buildx.
setup_ssh_agent() {
  if [ -n "${SSH_AUTH_SOCK:-}" ] && ssh-add -l >/dev/null 2>&1; then
    echo -e "${GREEN}Reusing forwarded SSH agent at ${SSH_AUTH_SOCK}${NC}"
    return
  fi
  local key=""
  for candidate in \
    "${SSH_GITHUB_KEY:-}" \
    "/root/.ssh/id_rsa_github" \
    "/home/user/.ssh/id_rsa_github" \
    "/mnt/storage/ansible/.ssh/id_rsa_github"; do
    if [ -n "${candidate}" ] && [ -r "${candidate}" ]; then
      key="${candidate}"
      break
    fi
  done
  if [ -z "${key}" ]; then
    echo -e "${RED}ERROR: no SSH agent forwarded and no id_rsa_github key found at the usual paths${NC}"
    echo -e "${RED}Cannot fetch the private agent-kit-go module. Either run with a forwarded SSH agent or place id_rsa_github at one of: /root/.ssh, /home/user/.ssh, /mnt/storage/ansible/.ssh${NC}"
    exit 1
  fi
  echo -e "${YELLOW}Starting transient ssh-agent and loading ${key}...${NC}"
  eval "$(ssh-agent -s)" >/dev/null
  ssh-add "${key}" >/dev/null 2>&1
  _STARTED_SSH_AGENT=1
}

cleanup_ssh_agent() {
  if [ "${_STARTED_SSH_AGENT:-0}" = "1" ] && [ -n "${SSH_AGENT_PID:-}" ]; then
    kill "${SSH_AGENT_PID}" >/dev/null 2>&1 || true
  fi
}

cleanup_buildx() {
  echo -e "${YELLOW}Cleaning up Docker buildx...${NC}"
  docker buildx prune -f >/dev/null 2>&1 || true
  if docker buildx inspect gdrive-agent-builder &>/dev/null; then
    docker buildx rm gdrive-agent-builder --force >/dev/null 2>&1 || true
  fi
  cleanup_ssh_agent
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

setup_ssh_agent

echo -e "${GREEN}===== Building ${IMAGE_NAME} =====${NC}"

# --ssh default forwards the host's SSH agent to the build only for
# RUN --mount=type=ssh steps in the Dockerfile. The key is required to
# fetch the private github.com/Daniel-dev22/agent-kit-go module via
# git+SSH (GOPRIVATE in the Dockerfile bypasses the public proxy). The
# key never lands in the image — only mounted during go mod download.
#
# Requires SSH_AUTH_SOCK on the build host, set up via the operator's
# SSH agent before invoking this script (ansible build playbook does so).
if docker buildx build \
  --ssh default \
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
