# Mario Kart 8 NEX server

# Cloning and building

```shell
git clone --recursive https://www.github.com/EpicUsername12/nex_mario_kart_8.git
```

You need:

- S3 instance
- MongoDB server 6.0+
- Redis server 7.0+

Install Python3 and these libs:

- [NintendoClients](https://github.com/kinnay/NintendoClients)
- ``python -m pip install aioconsole requests pymongo redis grpcio-tools minio``

```shell
python -m grpc_tools.protoc --proto_path=grpc --python_out=. --grpc_python_out=. grpc/amkj_service.proto
```
