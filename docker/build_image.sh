#!/bin/bash

GIT_HASH=$(git rev-parse --verify --short=8 HEAD)
REGISTRY_LOCATION="git.elfin.ucla:5050"
CONTAINER_NAME="${REGISTRY_LOCATION}/science-processing/pipeline-refactor/dev-image"

docker build -t "${CONTAINER_NAME}:${GIT_HASH}" .

echo -e "\n\n------------------------------------------"
echo "To push this image to the GitLab registry:"
echo "1. Update your docker daemon, adding \"${REGISTRY_LOCATION}\" as an insecure registry:"
echo "   https://docs.docker.com/registry/insecure/"
echo "2. Run: \$ docker login ${REGISTRY_LOCATION}"
echo "3. Run: \$ docker push ${CONTAINER_NAME}:${GIT_HASH}"
echo "4. Update DEV_IMAGE in .gitlab-ci.yml to use the new image"
echo -e "------------------------------------------\n\n"