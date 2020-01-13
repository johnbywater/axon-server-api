#!/usr/bin/env bash

python -m grpc_tools.protoc -I=src/main/proto --python_out=./axonclient --grpc_python_out=./axonclient src/main/proto/*.proto
