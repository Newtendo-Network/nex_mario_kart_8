# Mario Kart 8 NEX server

# Cloning and building

```shell
git clone --recursive https://.../nex_mario_kart_8.git
```

Install Python3 and these libs:

- NintendoClients
- redis, mongo
- grpc

```shell
python -m grpc_tools.protoc --proto_path=grpc --python_out=. --grpc_python_out=. grpc/amkj_service.proto
```
